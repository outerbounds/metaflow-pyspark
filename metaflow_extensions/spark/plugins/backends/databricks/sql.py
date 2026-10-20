"""SQL warehouse access: a statement, no cluster, no Spark session.

Most people who say "I need Spark" need to read some data, and a SQL warehouse answers
that without paying for Spark compute at all. It is a different mechanism from `catalog/unity.py`'s credential vending: a
warehouse statement goes through the query engine, so views, row filters, and column
masks apply, where vending bypasses the engine entirely.

Built on the Statement Execution API
(``POST /api/2.0/sql/statements``, ``GET /api/2.0/sql/statements/{id}``,
``POST /api/2.0/sql/statements/{id}/cancel``,
``GET /api/2.0/sql/statements/{id}/result/chunks/{n}``), reached through the typed
``databricks-sdk`` service (``client.sdk.statement_execution``) rather than
``DatabricksClient.api()``, because that surface is well modeled by every supported SDK
version and typed dataclasses catch a wrong field name at call time instead of at a
30x-later runtime AttributeError.

Every statement is submitted with ``disposition=EXTERNAL_LINKS`` and
``format=ARROW_STREAM`` unconditionally, small results included. That keeps a single
code path for reading results, rather than branching between an inline JSON decoder for
small results and a chunked Arrow reader for large ones, and it means a result is always
real Arrow-typed data rather than the string-encoded values ``JSON_ARRAY`` returns.

Cost attribution goes through ``query_tags`` (key/value, public preview, capped at 20),
which land in ``system.query.history`` rather than ``system.billing.usage``, so
attributing warehouse spend means querying query history rather than the billing
tables.
"""

import datetime
import decimal

from ...context import JobHandle, JobState, JobStatus
from ...exceptions import SparkConfigError, SparkException
from .. import StatementBackend
from .client import DatabricksClient

#: Databricks recommends against reusing a caller's own auth headers on external-link
#: downloads: the presigned URL already embeds its own short-lived credential.
EXTERNAL_LINK_TIMEOUT_SECONDS = 60

#: The API accepts at most 20 query tags and silently truncates the rest, so truncate
#: deliberately rather than let the API decide which ones survive.
MAX_QUERY_TAGS = 20

MAX_DECIMAL_PRECISION = 38

#: databricks-sdk's ColumnInfoTypeName values that map onto a plain pyarrow type. DECIMAL
#: is handled separately because it needs precision/scale, and anything not listed here
#: (ARRAY, STRUCT, MAP, INTERVAL, USER_DEFINED_TYPE) falls back to a string column, which
#: only matters for the empty-result placeholder schema below, never for real data:
#: pyarrow.ipc decodes real rows from the Arrow schema embedded in the stream itself.
_ARROW_TYPE_NAMES = (
    "BOOLEAN",
    "BYTE",
    "SHORT",
    "INT",
    "LONG",
    "FLOAT",
    "DOUBLE",
    "DATE",
    "TIMESTAMP",
    "STRING",
    "CHAR",
    "BINARY",
    "NULL",
)


class DatabricksSqlBackend(StatementBackend):
    """Run one SQL statement against a Databricks SQL warehouse.

    Gets the shared wait loop, cancellation, and error classification in
    ``backends/__init__.py`` for free: a statement is submitted asynchronously
    (``wait_timeout="0s"``) and then polled.
    """

    name = "databricks-sql"

    def __init__(self, config, ctx=None):
        super().__init__(config, ctx)
        self.client = DatabricksClient(config)
        # statement_id -> the last StatementResponse seen for it, so a terminal poll()
        # that already carries the manifest and first result chunk is not re-fetched.
        self._responses = {}

    # ------------------------------------------------------------------
    def submit(self, ctx):
        config = self.config
        warehouse_id = config.get("warehouse_id")
        if not warehouse_id:
            raise SparkConfigError(
                "No warehouse_id to run this statement on. This is an internal error: "
                "query() resolves a warehouse before constructing this backend."
            )
        statement = config.get("statement")
        if not statement:
            raise SparkConfigError("No SQL statement was given to execute.")

        service = self._service()
        from databricks.sdk.service.sql import Disposition, Format

        kwargs = {
            "statement": statement,
            "warehouse_id": warehouse_id,
            "disposition": Disposition.EXTERNAL_LINKS,
            "format": Format.ARROW_STREAM,
            "wait_timeout": "0s",
        }
        parameters = _bind_parameters(config.get("parameters"))
        if parameters:
            kwargs["parameters"] = parameters
        query_tags = _query_tags(ctx.tags if ctx else None)
        if query_tags and _supports_query_tags(service):
            kwargs["query_tags"] = query_tags
        for key in ("catalog", "schema", "row_limit", "byte_limit"):
            if config.get(key):
                kwargs[key] = config[key]

        try:
            response = service.execute_statement(**kwargs)
        except Exception as exc:
            raise SparkException(
                "Databricks rejected the statement before it started: %s" % exc
            ) from exc

        self._responses[response.statement_id] = response
        return JobHandle(
            backend=self.name,
            job_id=response.statement_id,
            ui_url=self.client.warehouse_url(warehouse_id),
        )

    def poll(self, handle):
        response = self._service().get_statement(handle.job_id)
        self._responses[handle.job_id] = response
        return self._status(response, handle)

    def cancel(self, handle):
        self._service().cancel_execution(handle.job_id)

    def read_output(self, handle, output_format):
        from ...output import from_arrow_table

        if output_format == "none":
            return None
        response = self._responses.get(handle.job_id)
        if response is None or not _is_succeeded(response):
            response = self._service().get_statement(handle.job_id)
        table = self._collect_arrow(handle.job_id, response)
        return from_arrow_table(table, output_format)

    # ------------------------------------------------------------------
    def _service(self):
        return self.client.sdk.statement_execution

    def _status(self, response, handle):
        from databricks.sdk.service.sql import StatementState

        state_map = {
            StatementState.PENDING: JobState.PENDING,
            StatementState.RUNNING: JobState.RUNNING,
            StatementState.SUCCEEDED: JobState.SUCCESS,
            # A statement's result is no longer fetchable once CLOSED, but that only
            # happens after a successful execution, never in place of one.
            StatementState.CLOSED: JobState.SUCCESS,
            StatementState.FAILED: JobState.FAILED,
            StatementState.CANCELED: JobState.CANCELLED,
        }
        status = response.status
        state = state_map.get(status.state, JobState.PENDING)
        error = status.error
        return JobStatus(
            state=state,
            message=error.message if error else None,
            error_class=(
                error.error_code.value if error and error.error_code else None
            ),
            ui_url=handle.ui_url if handle else None,
        )

    def _collect_arrow(self, statement_id, response):
        import pyarrow as pa

        batches = []
        result = response.result
        manifest = response.manifest
        while result is not None:
            links = result.external_links or []
            for link in links:
                batches.extend(_read_arrow_stream(link))
            # Chunk-continuation info can show up on the response's `result` itself
            # (the INLINE shape) or on the last link in `external_links` (the
            # EXTERNAL_LINKS shape); check both rather than assuming one.
            next_index = result.next_chunk_index
            if next_index is None and links:
                next_index = links[-1].next_chunk_index
            if next_index is None:
                break
            result = self._service().get_statement_result_chunk_n(
                statement_id, next_index
            )
        if not batches:
            return pa.Table.from_batches([], schema=_manifest_schema(manifest))
        return pa.Table.from_batches(batches)


# ----------------------------------------------------------------------
def _is_succeeded(response):
    from databricks.sdk.service.sql import StatementState

    return response.status and response.status.state == StatementState.SUCCEEDED


def _supports_query_tags(service):
    """Whether the installed databricks-sdk's `execute_statement` accepts `query_tags`.

    `query_tags` landed in the SDK after the Statement Execution API's other fields
    (databricks-sdk 0.86), so this is checked rather than assumed: an older, still
    otherwise-compatible SDK should degrade to no cost-attribution tags rather than
    fail the statement outright.
    """
    import inspect

    try:
        return "query_tags" in inspect.signature(service.execute_statement).parameters
    except (TypeError, ValueError):
        return False


def _read_arrow_stream(link):
    import pyarrow as pa
    import requests

    # Presigned/SAS/signed URLs already embed their own short-lived credential.
    # Databricks documents that no Authorization header should be sent here, so this
    # deliberately does not reuse the SDK's authenticated session.
    response = requests.get(
        link.external_link,
        headers=link.http_headers or {},
        timeout=EXTERNAL_LINK_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    with pa.ipc.open_stream(response.content) as reader:
        return list(reader)


def _manifest_schema(manifest):
    """Build a placeholder Arrow schema for a zero-row result.

    A real result's schema comes from the Arrow stream itself; this only matters when
    there were no chunks to read at all.
    """
    import pyarrow as pa

    columns = manifest.schema.columns if manifest and manifest.schema else None
    if not columns:
        return pa.schema([])
    fields = []
    for column in columns:
        type_name = column.type_name.value if column.type_name else "STRING"
        if type_name == "DECIMAL":
            arrow_type = pa.decimal128(
                column.type_precision or 38, column.type_scale or 0
            )
        elif type_name in _ARROW_TYPE_NAMES:
            arrow_type = _ARROW_TYPE(type_name)
        else:
            arrow_type = pa.string()
        fields.append(pa.field(column.name, arrow_type))
    return pa.schema(fields)


def _ARROW_TYPE(type_name):
    import pyarrow as pa

    return {
        "BOOLEAN": pa.bool_(),
        "BYTE": pa.int8(),
        "SHORT": pa.int16(),
        "INT": pa.int32(),
        "LONG": pa.int64(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
        "STRING": pa.string(),
        "CHAR": pa.string(),
        "BINARY": pa.binary(),
        "NULL": pa.null(),
    }[type_name]


def _bind_parameters(params):
    """Translate named parameters into the API's typed parameter list.

    This is the mechanism that replaces string-formatted SQL: the statement text keeps
    ``:name`` markers, and values cross as typed, escaped parameters rather than being
    concatenated into the query.
    """
    if not params:
        return None
    from databricks.sdk.service.sql import StatementParameterListItem

    items = []
    for name, value in params.items():
        sql_type, sql_value = _sql_type_and_value(value)
        items.append(
            StatementParameterListItem(name=name, value=sql_value, type=sql_type)
        )
    return items


def _sql_type_and_value(value):
    if value is None:
        return "STRING", None
    if isinstance(value, bool):
        # bool is an int subclass, so this has to be checked first.
        return "BOOLEAN", ("true" if value else "false")
    if isinstance(value, int):
        return ("INT" if -(2**31) <= value < 2**31 else "LONG"), str(value)
    if isinstance(value, float):
        return "DOUBLE", repr(value)
    if isinstance(value, decimal.Decimal):
        return _decimal_type_and_value(value)
    if isinstance(value, datetime.datetime):
        # datetime is a date subclass, so this has to be checked before DATE. An
        # aware value keeps its offset so the warehouse reads the right instant; a
        # naive one is read in the session time zone.
        return "TIMESTAMP", value.isoformat(sep=" ", timespec="microseconds")
    if isinstance(value, datetime.date):
        return "DATE", value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        raise SparkConfigError(
            "Binary query parameters are not supported by the Statement Execution API. "
            "Pass value.hex() and use unhex(:name) in the statement."
        )
    return "STRING", str(value)


def _decimal_type_and_value(value):
    if not value.is_finite():
        raise SparkConfigError(
            "Cannot bind %s as a DECIMAL parameter; SQL decimals are finite." % value
        )
    _, digits, exponent = value.as_tuple()
    scale = max(-exponent, 0)
    precision = max(len(digits) + max(exponent, 0), scale + 1)
    if precision > MAX_DECIMAL_PRECISION:
        raise SparkConfigError(
            "Cannot bind %s as a DECIMAL parameter: it needs precision %d, and "
            "Databricks allows at most %d." % (value, precision, MAX_DECIMAL_PRECISION)
        )
    return "DECIMAL(%d,%d)" % (precision, scale), format(value, "f")


def _query_tags(tags):
    if not tags:
        return None
    from databricks.sdk.service.sql import QueryTag

    # Sorted for determinism (dict ordering is call-dependent, and this caps rather
    # than silently letting the API decide which 20 of more survive).
    items = [QueryTag(key=k, value=v) for k, v in sorted(tags.items())]
    return items[:MAX_QUERY_TAGS] or None
