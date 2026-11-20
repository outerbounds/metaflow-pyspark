"""The @databricks_job step decorator: trigger an existing Databricks job and wait."""

import functools
import json

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

from .config import backend_config, resolve_config
from .context import TaskContext, log
from .exceptions import SparkConfigError

#: Attributes that go into the `databricks` config section the backend reads.
_BACKEND_ATTRS = (
    "job_id",
    "job_name",
    "idempotency_token",
    "queue",
    "host",
    "token",
    "profile",
)


class DatabricksJobDecorator(StepDecorator):
    """Trigger a job that already exists in the Databricks workspace and wait for it.

    The job keeps its own tasks, compute, libraries, and permissions. Before the step
    body runs, `self.databricks_job_result` holds each task's state and, for notebook
    tasks, what it passed to `dbutils.notebook.exit()`::

        @databricks_job(job_name="nightly-enrichment", parameters_from=["run_date"])
        @step
        def enrich(self):
            print(self.databricks_job_result)
    """

    name = "databricks_job"
    defaults = {
        # which job
        "job_id": None,
        "job_name": None,
        # job-level parameters: literal values, and names of flow attributes
        "parameters": None,
        "parameters_from": None,
        # run-now options
        "idempotency_token": None,
        "queue": True,
        # lifecycle
        "timeout": None,
        "crash_on_failure": True,
        "show_output": True,
        # result
        "output_artifact": "databricks_job_result",
        # workspace connection
        "host": None,
        "token": None,
        "profile": None,
        "config": "spark_config",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata = None

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        attrs = self.attributes
        if not (attrs.get("job_id") or attrs.get("job_name")):
            raise SparkConfigError(
                "@databricks_job needs job_id=... or job_name=... naming the workspace "
                "job to trigger."
            )
        if attrs.get("job_id") and attrs.get("job_name"):
            raise SparkConfigError(
                "@databricks_job takes job_id=... or job_name=..., not both."
            )

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
        # The wrapper installed by task_decorate has no other way to reach these.
        self._metadata = metadata
        self._run_id = run_id
        self._task_id = task_id
        self._retry_count = retry_count
        self._step_name = step_name

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        return _job_wrapper(self, step_func, flow, retry_count)

    def record_metadata(self, handle):
        """Record the run as task metadata, so it can be found from the client API."""
        if self._metadata is None:
            return
        entries = {
            "databricks-job-id": handle.extra.get("job_id"),
            "databricks-run-id": handle.job_id,
            "databricks-run-url": handle.ui_url,
            "spark-job-handle": json.dumps(handle.to_dict()),
        }
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
            # Metadata is a convenience. Never fail a finished job run over it.
            pass


def _job_wrapper(deco, step_func, flow, retry_count):
    attrs = deco.attributes

    @functools.wraps(step_func)
    def job_step():
        from metaflow import current

        from .backends.databricks.runs import DatabricksExistingJobBackend

        config = resolve_config(flow, attrs.get("config"), {})
        section = backend_config(config, "databricks")
        for key in _BACKEND_ATTRS:
            if attrs.get(key) is not None:
                section[key] = attrs[key]

        ctx = TaskContext(
            step_name=current.step_name,
            pathspec=current.pathspec,
            flow_name=current.flow_name,
            run_id=current.run_id,
            task_id=current.task_id,
            attempt=retry_count,
            user=None,
            tags={},
            inputs=_parameters(flow, attrs),
            # The job's own timeout applies unless the step sets a shorter one; the
            # 60-minute @spark default would cancel long-running pipelines.
            timeout_minutes=attrs.get("timeout"),
            logger=lambda msg, job_id=None, stream="stdout": log(
                msg, job_id=job_id, stream=stream, prefix="@databricks_job"
            ),
        )
        backend = DatabricksExistingJobBackend(section, ctx)
        handle, _ = backend.run(
            ctx,
            crash_on_failure=attrs.get("crash_on_failure", True),
            show_stdout=attrs.get("show_output", True),
        )
        deco.record_metadata(handle)

        artifact = attrs.get("output_artifact")
        if artifact:
            setattr(flow, artifact, backend.task_results(handle))
        step_func()

    return job_step


def _parameters(flow, attrs):
    """Literal `parameters`, plus `parameters_from` values read off the flow."""
    params = dict(attrs.get("parameters") or {})
    for name in attrs.get("parameters_from") or []:
        if not hasattr(flow, name):
            raise SparkConfigError(
                "@databricks_job(parameters_from=[...]) refers to '%s', which is not set "
                "on the flow at this point. Set self.%s in an earlier step."
                % (name, name)
            )
        params[name] = getattr(flow, name)
    return params
