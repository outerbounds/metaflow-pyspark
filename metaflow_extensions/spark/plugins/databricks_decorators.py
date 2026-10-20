"""Decorators for work that already exists in a Databricks workspace.

``@databricks_job`` triggers a job the customer already has and waits for it.
``@databricks_notebook`` runs an existing notebook as a step.

Neither requires migrating anything, which is the point. A team can put Metaflow in
front of pipelines they already run and keep every Databricks Asset Bundle, schedule,
and permission exactly where it is.
"""

import functools
import json

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum

from .config import backend_config, resolve_config
from .context import TaskContext, log
from .cost import build_tags
from .exceptions import SparkConfigError


class _DatabricksRunDecorator(StepDecorator):
    """Shared plumbing for decorators that wait on a Databricks run."""

    #: Attributes forwarded into the backend's config section.
    backend_attrs = ()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._metadata = None
        self._run_id = None
        self._task_id = None
        self._retry_count = 0

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
        self._metadata = metadata
        self._run_id = run_id
        self._task_id = task_id
        self._retry_count = retry_count
        self._step_name = step_name

    def task_decorate(
        self, step_func, flow, graph, retry_count, max_user_code_retries, ubf_context
    ):
        deco = self
        attrs = self.attributes

        @functools.wraps(step_func)
        def wrapper():
            from metaflow import current

            config = resolve_config(
                flow,
                attrs.get("config"),
                {
                    "backend": "databricks",
                    "timeout": attrs.get("timeout"),
                },
            )
            section = dict(backend_config(config, "databricks"))
            for key in self.backend_attrs:
                value = attrs.get(key)
                if value is not None:
                    section[key] = value

            ctx = TaskContext(
                flow=flow,
                step_name=current.step_name,
                pathspec=current.pathspec,
                flow_name=current.flow_name,
                run_id=current.run_id,
                task_id=current.task_id,
                attempt=retry_count,
                user=_username(),
                tags={},
                inputs=_resolve_parameters(flow, attrs),
                timeout_minutes=config.get("timeout"),
                logger=log,
            )
            ctx.tags = build_tags(ctx, extra=attrs.get("tags"))

            backend = self.backend_class()(section, ctx)
            handle, status = backend.run(
                ctx,
                crash_on_failure=attrs.get("crash_on_failure", True),
                show_stdout=attrs.get("show_stdout", True),
                show_stderr=attrs.get("show_stderr", False),
            )
            deco._record(handle, backend)

            result = self.collect_result(backend, handle, status, ctx)
            artifact = attrs.get("output_artifact")
            if artifact:
                setattr(flow, artifact, result)

            step_func()

            if artifact and not attrs.get("save_output", True):
                if hasattr(flow, artifact):
                    delattr(flow, artifact)

        return wrapper

    def backend_class(self):
        raise NotImplementedError

    def collect_result(self, backend, handle, status, ctx):
        return None

    def _record(self, handle, backend):
        if self._metadata is None:
            return
        entries = {
            "databricks-run-id": handle.job_id,
            "databricks-run-url": handle.ui_url,
            "databricks-job-id": handle.extra.get("job_id"),
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
            pass


class DatabricksJobDecorator(_DatabricksRunDecorator):
    """Trigger an existing Databricks Job and wait for it.

    ::

        @databricks_job(job_name="nightly-features", parameters={"date": "2026-08-24"})
        @step
        def features(self):
            print(self.databricks_result)
    """

    name = "databricks_job"
    defaults = {
        "job_id": None,
        "job_name": None,
        "parameters": None,
        "parameters_from": None,
        "python_params": None,
        "idempotency_token": None,
        "queue": True,
        "timeout": None,
        "output_artifact": "databricks_result",
        "save_output": True,
        "crash_on_failure": True,
        "show_stdout": True,
        "show_stderr": False,
        "tags": None,
        "config": "spark_config",
        "host": None,
        "token": None,
        "profile": None,
    }

    backend_attrs = (
        "job_id",
        "job_name",
        "python_params",
        "idempotency_token",
        "queue",
        "host",
        "token",
        "profile",
    )

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if not (self.attributes.get("job_id") or self.attributes.get("job_name")):
            raise SparkConfigError(
                "@databricks_job needs job_id=... or job_name=... so it knows which "
                "workspace job to trigger."
            )

    def backend_class(self):
        from .backends.databricks.jobs import DatabricksExistingJobBackend

        return DatabricksExistingJobBackend

    def collect_result(self, backend, handle, status, ctx):
        """Return each task's exit value and state, keyed by task_key."""
        try:
            return backend.task_outputs(handle)
        except Exception as exc:
            ctx.log("could not read task outputs: %s" % exc, stream="stderr")
            return None


class DatabricksNotebookDecorator(_DatabricksRunDecorator):
    """Run an existing Databricks notebook as a step.

    ::

        @databricks_notebook(notebook_path="/Repos/team/etl/clean", cluster_id="...")
        @step
        def clean(self):
            print(self.notebook_result)
    """

    name = "databricks_notebook"
    defaults = {
        "notebook_path": None,
        "parameters": None,
        "parameters_from": None,
        "compute": None,
        "serverless": None,
        "cluster_id": None,
        "instance_pool_id": None,
        "num_workers": None,
        "node_type_id": None,
        "runtime_version": None,
        "photon": None,
        "dependencies": None,
        "queue": True,
        "timeout": None,
        "output_artifact": "notebook_result",
        "save_output": True,
        "crash_on_failure": True,
        "show_stdout": True,
        "show_stderr": False,
        "tags": None,
        "config": "spark_config",
        "host": None,
        "token": None,
        "profile": None,
        "cloud": None,
    }

    backend_attrs = (
        "notebook_path",
        "compute",
        "serverless",
        "cluster_id",
        "instance_pool_id",
        "num_workers",
        "node_type_id",
        "runtime_version",
        "photon",
        "dependencies",
        "queue",
        "host",
        "token",
        "profile",
        "cloud",
    )

    def step_init(
        self, flow, graph, step_name, decorators, environment, flow_datastore, logger
    ):
        if not self.attributes.get("notebook_path"):
            raise SparkConfigError(
                "@databricks_notebook needs notebook_path=... pointing at a notebook in "
                "the workspace, for example '/Repos/team/etl/clean'."
            )

    def backend_class(self):
        from .backends.databricks.jobs import DatabricksNotebookBackend

        return DatabricksNotebookBackend

    def collect_result(self, backend, handle, status, ctx):
        try:
            return backend.notebook_result(handle)
        except Exception as exc:
            ctx.log("could not read the notebook result: %s" % exc, stream="stderr")
            return None


def _resolve_parameters(flow, attrs):
    """Merge literal `parameters` with values pulled off the flow."""
    params = dict(attrs.get("parameters") or {})
    for name in attrs.get("parameters_from") or []:
        if not hasattr(flow, name):
            raise SparkConfigError(
                "parameters_from refers to '%s', which is not set on the flow at this "
                "point." % name
            )
        params[name] = getattr(flow, name)
    return params


def _username():
    from metaflow.util import get_username

    try:
        return get_username()
    except Exception:
        return None
