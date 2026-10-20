"""The @spark step decorator."""

import functools
import json

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

from .backends import (
    SessionBackend,
    canonical_backend,
    config_section,
    get_backend_class,
)
from .config import backend_config, resolve_config
from .context import TaskContext, log
from .cost import build_tags
from .exceptions import SparkConfigError
from .output import from_spark_dataframe, strip_spark_attrs, validate_format

#: Attributes that describe the compute or the workspace rather than the step, and so
#: belong in the resolved backend's own config section.
_BACKEND_ATTRS = (
    "compute",
    "serverless",
    "cluster_id",
    "instance_pool_id",
    "num_workers",
    "node_type_id",
    "runtime_version",
    "photon",
    "dependencies",
    "libraries",
    "volume",
    "output_table",
    "output_path",
    "write_mode",
    "partition_by",
    "include",
    "packages",
    "host",
    "token",
    "profile",
    "master",
    "application_id",
    "execution_role",
    "s3_prefix",
    "cloud",
)

#: Renames between the decorator's Python-friendly names and the config keys the
#: backends use.
_ATTR_TO_CONFIG_KEY = {
    "application_id": "application-id",
    "execution_role": "execution-role",
    "s3_prefix": "s3-prefix",
}

#: Materializing more than this into a step's memory is usually a mistake at Spark
#: scale, so say something rather than let the task get OOM-killed later.
MATERIALIZE_WARN_BYTES = 1024**3


class SparkDecorator(StepDecorator):
    """Run Spark work as part of a step.

    Session style, where the step body uses Spark directly and the session is gone
    before Metaflow persists the step's artifacts::

        @spark(backend="databricks")
        @step
        def features(self):
            df = self.spark.read.table("main.retail.orders")
            self.count = df.count()

    Job style, where `job=` names a function taking `(spark, **job_parameters)` whose
    returned DataFrame becomes the `output_artifact`. On a session backend the function
    runs in the task; on a submit backend (`mode="job"` on Databricks, or
    `backend="emr-serverless"`) it is packaged and runs on the cluster::

        @spark(backend="databricks", mode="job", job=etl.run, volume="/Volumes/...")
        @step
        def features(self):
            print(len(self.spark_df))
    """

    name = "spark"
    defaults = {
        # what to run
        "job": None,
        "job_parameters": None,
        "session_attr": "spark",
        # where to run it
        "backend": None,
        "mode": None,
        "compute": None,
        "serverless": None,
        "cluster_id": None,
        "instance_pool_id": None,
        "num_workers": None,
        "node_type_id": None,
        "runtime_version": None,
        "photon": None,
        "dependencies": None,
        "libraries": None,
        "master": None,
        "cloud": None,
        # EMR Serverless
        "application_id": None,
        "execution_role": None,
        "s3_prefix": None,
        # workspace connection
        "host": None,
        "token": None,
        "profile": None,
        "volume": None,
        # code packaging
        "include": None,
        "packages": None,
        # output of a job function
        "output_artifact": "spark_df",
        "output_format": None,
        "output_table": None,
        "output_path": None,
        "write_mode": None,
        "partition_by": None,
        "save_output": True,
        # lifecycle of a submitted job
        "timeout": None,
        "crash_on_failure": True,
        "show_stdout": True,
        "show_stderr": False,
        # cost attribution of a submitted job
        "tags": None,
        # configuration
        "config": "spark_config",
        "spark_parameters": None,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata = None
        self._run_id = None
        self._task_id = None
        self._retry_count = 0

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
        if output_format == "table" and not self.attributes.get("output_table"):
            raise SparkConfigError(
                "@spark(output_format='table') needs output_table='catalog.schema.table' "
                "for the job to write to."
            )
        if self.attributes.get("backend") is not None:
            canonical_backend(self.attributes["backend"])

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        # Captured here because the wrapper installed by task_decorate has no other way
        # to reach the metadata provider.
        self._metadata = metadata
        self._run_id = run_id
        self._task_id = task_id
        self._retry_count = retry_count
        self._step_name = step_name

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return _spark_wrapper(self, step_func, flow, retry_count)

    def record_metadata(self, entries):
        """Record job details as task metadata so a run stays inspectable later."""
        if self._metadata is None:
            return
        try:
            self._metadata.register_metadata(
                self._run_id,
                self._step_name,
                self._task_id,
                [
                    MetaDatum(
                        field=field,
                        value=value,
                        type=field,
                        tags=["attempt_id:%s" % self._retry_count],
                    )
                    for field, value in entries.items()
                    if value is not None
                ],
            )
        except Exception:
            # Metadata is a convenience. Never fail a working Spark job over it.
            pass


class PySparkDecorator(SparkDecorator):
    """Backwards-compatible alias for the original @pyspark decorator.

    Existing flows keep running unchanged: the original defaults (`pyspark_df`,
    pandas output) are kept, the old boolean output attributes are translated to
    `output_format` with the original precedence (pandas, then arrow, else the output
    URL), and the default backend stays EMR Serverless.
    """

    name = "pyspark"
    defaults = dict(
        SparkDecorator.defaults,
        output_artifact="pyspark_df",
        output_pandas=True,
        output_pyarrow=False,
        user_timeout=None,
        spark_config=None,
    )

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        attrs = self.attributes
        if attrs.get("output_format") is None:
            if attrs.get("output_pandas"):
                attrs["output_format"] = "pandas"
            elif attrs.get("output_pyarrow"):
                attrs["output_format"] = "arrow"
            else:
                attrs["output_format"] = "url"
        if attrs.get("user_timeout") is not None and attrs.get("timeout") is None:
            attrs["timeout"] = attrs["user_timeout"]
        if attrs.get("spark_config") is not None:
            attrs["config"] = attrs["spark_config"]
        if attrs.get("backend") is None:
            attrs["backend"] = "emr-serverless"
        super().step_init(
            flow, graph, step_name, decorators, environment, flow_datastore, logger
        )


# ----------------------------------------------------------------------
def _spark_wrapper(deco, step_func, flow, retry_count):
    attrs = deco.attributes

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
            flow=flow,
            job_func=attrs.get("job"),
            timeout_minutes=config.get("timeout"),
            logger=log,
        )
        ctx.tags = build_tags(ctx, extra=attrs.get("tags"))
        section = backend_config(config, config_section(backend_name))
        backend = get_backend_class(backend_name)(section, ctx)
        if isinstance(backend, SessionBackend):
            _run_session(backend, ctx, step_func, flow, attrs)
        else:
            ctx.inputs = _collect_inputs(flow, attrs.get("job_parameters"))
            _run_submit(deco, backend, ctx, step_func, flow, attrs)

    return spark_step


def _resolve(flow, attrs):
    """Build the effective config, then fold backend-specific attributes into it."""
    overrides = {
        key: attrs[key]
        for key in ("backend", "mode", "timeout")
        if attrs.get(key) is not None
    }
    if attrs.get("spark_parameters"):
        overrides["spark-parameters"] = attrs["spark_parameters"]

    config = resolve_config(flow, attrs.get("config"), overrides)
    backend_name = canonical_backend(config["backend"])
    config["backend"] = backend_name
    section_key = config_section(backend_name)
    section = dict(config.get(section_key) or {})
    for attr in _BACKEND_ATTRS:
        if attrs.get(attr) is not None:
            section[_ATTR_TO_CONFIG_KEY.get(attr, attr)] = attrs[attr]
    if attrs.get("mode") is not None:
        section["mode"] = attrs["mode"]
    config[section_key] = section
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
    _cleanup_output(flow, attrs)


def _run_submit(deco, backend, ctx, step_func, flow, attrs):
    output_format = attrs.get("output_format") or "pandas"
    handle, status = backend.run(
        ctx,
        crash_on_failure=attrs.get("crash_on_failure", True),
        show_stdout=attrs.get("show_stdout", True),
        show_stderr=attrs.get("show_stderr", False),
    )

    deco.record_metadata(
        {
            "spark-backend": backend.name,
            "spark-job-id": handle.job_id,
            "spark-job-url": handle.ui_url,
            "spark-ui-url": handle.spark_ui_url,
            "spark-job-handle": json.dumps(handle.to_dict()),
        }
    )
    if handle.ui_url:
        ctx.log("job details: %s" % handle.ui_url)
    if handle.spark_ui_url:
        ctx.log("Spark UI: %s" % handle.spark_ui_url)

    if attrs.get("output_artifact"):
        result = backend.read_output(handle, output_format) if status.ok else None
        _set_output(flow, attrs, result)

    try:
        step_func()
    finally:
        try:
            backend.cleanup(handle, ctx)
        except Exception:
            pass
    _cleanup_output(flow, attrs)


def _set_output(flow, attrs, value):
    artifact = attrs.get("output_artifact")
    if not artifact:
        return
    _warn_if_large(value, artifact)
    setattr(flow, artifact, value)


def _cleanup_output(flow, attrs):
    artifact = attrs.get("output_artifact")
    if artifact and not attrs.get("save_output", True) and hasattr(flow, artifact):
        delattr(flow, artifact)


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
            "materialized %.1fGB into 'self.%s'. Consider output_format='table' or "
            "'url' to pass the next step a reference instead of copying the data."
            % (size / 1024**3, artifact),
            stream="stderr",
        )
