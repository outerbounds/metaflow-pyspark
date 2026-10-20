"""The @spark step decorator."""

import functools

from metaflow.decorators import StepDecorator

from .backends import canonical_backend, get_backend_class
from .config import backend_config, resolve_config
from .context import TaskContext, log
from .exceptions import SparkConfigError
from .output import from_spark_dataframe, strip_spark_attrs, validate_format

#: Attributes that describe the compute or the workspace rather than the step, and so
#: belong in the resolved backend's own config section.
_BACKEND_ATTRS = (
    "serverless",
    "cluster_id",
    "host",
    "token",
    "profile",
    "master",
)

#: Materializing more than this into a step's memory is usually a mistake at Spark
#: scale, so say something rather than let the task get OOM-killed later.
MATERIALIZE_WARN_BYTES = 1024**3


class SparkDecorator(StepDecorator):
    """Give a step a live Spark session.

    The step body uses Spark directly, and the session is gone before Metaflow persists
    the step's artifacts::

        @spark(backend="databricks")
        @step
        def features(self):
            df = self.spark.read.table("main.retail.orders")
            self.count = df.count()

    Alternatively, `job=` names a function taking `(spark, **job_parameters)` whose
    returned DataFrame becomes the `output_artifact`, which keeps Spark code in its own
    importable, testable module::

        @spark(backend="databricks", job=etl.summarize, job_parameters=["cutoff"])
        @step
        def crunch(self):
            print(self.spark_df.head())
    """

    name = "spark"
    defaults = {
        # what to run
        "job": None,
        "job_parameters": None,
        "session_attr": "spark",
        # where to run it
        "backend": None,
        "serverless": None,
        "cluster_id": None,
        "master": None,
        # workspace connection
        "host": None,
        "token": None,
        "profile": None,
        # output of a job function
        "output_artifact": "spark_df",
        "output_format": None,
        # configuration
        "config": "spark_config",
        "spark_parameters": None,
    }

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        output_format = self.attributes.get("output_format")
        if output_format is not None:
            validate_format(output_format)
        if self.attributes.get("job") is not None and not callable(
            self.attributes["job"]
        ):
            raise SparkConfigError(
                "@spark(job=...) must be a callable taking (spark, **job_parameters), "
                "got %r." % (self.attributes["job"],)
            )
        if self.attributes.get("backend") is not None:
            canonical_backend(self.attributes["backend"])

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return _spark_wrapper(self.attributes, step_func, flow, retry_count)


# ----------------------------------------------------------------------
def _spark_wrapper(attrs, step_func, flow, retry_count):
    @functools.wraps(step_func)
    def spark_step():
        from metaflow import current

        config = _resolve(flow, attrs)
        backend_name = config["backend"]
        ctx = TaskContext(
            step_name=current.step_name,
            pathspec=current.pathspec,
            flow_name=current.flow_name,
            run_id=current.run_id,
            task_id=current.task_id,
            attempt=retry_count,
            user=_username(),
            tags={},
            logger=log,
        )
        section = backend_config(config, backend_name)
        backend = get_backend_class(backend_name)(section, ctx)
        _run_session(backend, ctx, step_func, flow, attrs)

    return spark_step


def _resolve(flow, attrs):
    """Build the effective config, then fold backend-specific attributes into it."""
    overrides = {}
    if attrs.get("backend") is not None:
        overrides["backend"] = attrs["backend"]
    if attrs.get("spark_parameters"):
        overrides["spark-parameters"] = attrs["spark_parameters"]

    config = resolve_config(flow, attrs.get("config"), overrides)
    backend_name = canonical_backend(config["backend"])
    config["backend"] = backend_name
    section = dict(config.get(backend_name) or {})
    for attr in _BACKEND_ATTRS:
        if attrs.get(attr) is not None:
            section[attr] = attrs[attr]
    config[backend_name] = section
    return config


def _collect_inputs(flow, job_parameters):
    inputs = {}
    for name in job_parameters or []:
        if not hasattr(flow, name):
            raise SparkConfigError(
                "@spark(job_parameters=[...]) refers to '%s', which is not set on the "
                "flow at this point. Set self.%s in an earlier step." % (name, name)
            )
        inputs[name] = getattr(flow, name)
    return inputs


def _username():
    from metaflow.util import get_username

    try:
        return get_username()
    except Exception:
        return None


def _run_session(backend, ctx, step_func, flow, attrs):
    from metaflow import current

    session_attr = attrs.get("session_attr") or "spark"
    job_func = attrs.get("job")

    with backend.session(ctx) as session:
        # Exposed on `current` as well as on the flow, because `current.spark` is the
        # idiom people expect from other Metaflow decorators, and it is how query()
        # finds the session.
        current._update_env({"spark": session})
        try:
            if job_func is not None:
                inputs = _collect_inputs(flow, attrs.get("job_parameters"))
                output_format = attrs.get("output_format") or "pandas"
                df = job_func(session, **inputs)
                _set_output(flow, attrs, from_spark_dataframe(df, output_format))
                step_func()
            else:
                # A SparkSession is not picklable, so it must be gone before Metaflow
                # persists the task's artifacts. This finally block runs before
                # task_post_step, which runs before persistence.
                setattr(flow, session_attr, session)
                try:
                    step_func()
                finally:
                    if getattr(flow, session_attr, None) is session:
                        delattr(flow, session_attr)
        finally:
            current._update_env({"spark": None})

    # Covers the step body's own toPandas() calls, which never pass through
    # from_spark_dataframe.
    for value in vars(flow).values():
        strip_spark_attrs(value)


def _set_output(flow, attrs, value):
    artifact = attrs.get("output_artifact")
    if not artifact:
        return
    _warn_if_large(value, artifact)
    setattr(flow, artifact, value)


def _warn_if_large(value, artifact):
    """Say something when a job function's result is big enough to hurt."""
    size = None
    try:
        if hasattr(value, "nbytes"):
            size = value.nbytes
        elif hasattr(value, "memory_usage"):
            size = int(value.memory_usage(deep=True).sum())
        elif hasattr(value, "estimated_size"):
            size = value.estimated_size()
    except Exception:
        return
    if size and size > MATERIALIZE_WARN_BYTES:
        # Not warnings.warn(): Metaflow's CLI filters warnings out globally.
        log(
            "materialized %.1fGB into 'self.%s'. Consider writing the result to a "
            "Unity Catalog table and passing a UnityCatalogTable reference to the next "
            "step instead of copying the data." % (size / 1024**3, artifact),
            stream="stderr",
        )
