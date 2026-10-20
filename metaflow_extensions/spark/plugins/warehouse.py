"""SQL warehouse access: run a statement without a Spark session or a cluster.

Most people who say "I need Spark" need to read some data, and a SQL warehouse answers
that without paying for Spark compute at all. `query()` is a plain function rather than a
decorator: any step can call it, with no compute configuration and no live session.

::

    from metaflow_extensions.spark.plugins.warehouse import query

    @step
    def analyze(self):
        self.daily = query(
            "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
            "WHERE order_date >= :since GROUP BY order_date",
            params={"since": self.since},
            output_format="polars",
        )

`params` are bound as named, typed parameters through the Statement Execution API's
``:name`` markers, never concatenated into the statement text.

The target is an explicit ``warehouse_id=``, else the live session of an enclosing
``@spark`` step (its compute is already running and paid for), else the configured
default warehouse (``warehouse_id`` in the ``databricks`` config section, or
``METAFLOW_DATABRICKS_WAREHOUSE_ID`` / ``DATABRICKS_WAREHOUSE_ID``). The session path
uses the same ``:name`` parameter syntax, so the statement does not change between the
two.

Unlike a pinned ``UnityCatalogTable``, an arbitrary statement is not reproducible: the
tables underneath it can move between runs, and this module does not rewrite user SQL to
imply otherwise. And unlike ``catalog/unity.py``'s credential vending, a warehouse
statement goes through the query engine, so views, row filters, and column masks apply.

No structured task metadata is recorded for a `query()` call: registering it needs the
metadata provider Metaflow only hands to a `StepDecorator`'s lifecycle hooks. The
statement id and warehouse URL are visible in the task's log output.

Cost attribution is via `query_tags` (public preview, needs `databricks-sdk>=0.86`),
which land in `system.query.history`, not `system.billing.usage`.
"""

import functools

from .config import backend_config, require, resolve_config
from .context import TaskContext, log
from .cost import build_tags
from .exceptions import SparkConfigError

QUERY_FORMATS = ("pandas", "arrow", "polars", "none")

WAREHOUSE_HINT = (
    "Set it with query(warehouse_id=...), the 'warehouse_id' key of the 'databricks' "
    "config section, or METAFLOW_DATABRICKS_WAREHOUSE_ID / DATABRICKS_WAREHOUSE_ID."
)


def query(
    statement,
    params=None,
    output_format="pandas",
    warehouse_id=None,
    flow=None,
    config="spark_config",
    catalog=None,
    schema=None,
    row_limit=None,
    byte_limit=None,
    timeout=None,
    tags=None,
    crash_on_failure=True,
):
    """Run one SQL statement on a Databricks SQL warehouse and return its result.

    `flow` is optional and only needed to pick up a flow-level config artifact (by
    default `self.spark_config`); pass `flow=self` from inside a step to get that layer
    of the precedence chain. Without it, the warehouse still resolves from
    `METAFLOW_DATABRICKS_WAREHOUSE_ID` / `DATABRICKS_WAREHOUSE_ID` and explicit
    arguments.

    A failed statement raises `QueryFailed`, unless `crash_on_failure=False`, in which
    case `query()` returns None.
    """
    if output_format not in QUERY_FORMATS:
        raise SparkConfigError(
            "query(output_format=%r) is not supported. Choose one of: %s."
            % (output_format, ", ".join(QUERY_FORMATS))
        )

    overrides = {}
    if timeout is not None:
        overrides["timeout"] = timeout
    if not warehouse_id:
        session = _live_session()
        if session is not None:
            return _query_via_session(
                session,
                statement,
                params,
                output_format,
                crash_on_failure,
                catalog=catalog,
                schema=schema,
                row_limit=row_limit,
                byte_limit=byte_limit,
            )

    resolved = resolve_config(flow, config, overrides)
    section = dict(backend_config(resolved, "databricks"))
    if warehouse_id:
        section["warehouse_id"] = warehouse_id
    require(section, "warehouse_id", "databricks-sql", WAREHOUSE_HINT)

    section["statement"] = statement
    section["parameters"] = params or {}
    for key, value in (
        ("catalog", catalog),
        ("schema", schema),
        ("row_limit", row_limit),
        ("byte_limit", byte_limit),
    ):
        if value is not None:
            section[key] = value

    ctx = _build_context(resolved, tags)

    from .backends.databricks.sql import DatabricksSqlBackend

    backend = DatabricksSqlBackend(section, ctx)
    handle, status = backend.run(ctx, crash_on_failure=crash_on_failure)
    if not status.ok:
        return None
    return backend.read_output(handle, output_format)


def _live_session():
    """The session an enclosing @spark step established, if any."""
    from metaflow import current

    return getattr(current, "spark", None)


def _query_via_session(
    session, statement, params, output_format, crash_on_failure, **options
):
    from .output import from_spark_dataframe

    unsupported = sorted(
        k for k in ("catalog", "schema", "byte_limit") if options.get(k) is not None
    )
    if unsupported:
        raise SparkConfigError(
            "query(%s=...) only applies on a SQL warehouse, and this call runs through "
            "the step's Spark session. Qualify table names in the statement, or pass "
            "warehouse_id= to run on a warehouse." % ", ".join(unsupported)
        )
    try:
        df = session.sql(statement, args=params or None)
        if options.get("row_limit") is not None:
            df = df.limit(options["row_limit"])
        return from_spark_dataframe(df, output_format)
    except Exception:
        if crash_on_failure:
            raise
        return None


def _build_context(resolved, tags):
    from metaflow import current

    ctx = TaskContext(
        step_name=current.step_name,
        pathspec=current.pathspec,
        flow_name=current.flow_name,
        run_id=current.run_id,
        task_id=current.task_id,
        attempt=getattr(current, "retry_count", 0) or 0,
        user=_username(),
        tags={},
        timeout_minutes=resolved.get("timeout"),
        logger=functools.partial(log, prefix="query"),
    )
    ctx.tags = build_tags(ctx, extra=tags)
    return ctx


def _username():
    from metaflow.util import get_username

    try:
        return get_username()
    except Exception:
        return None
