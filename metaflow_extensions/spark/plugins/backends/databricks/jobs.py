"""Submitting packaged Spark code to the Databricks Jobs API as a one-off run.

This is what you need when Connect is not enough: a pinned Databricks Runtime, Photon,
cluster-scoped libraries, JVM UDFs, or a driver that has to live on the cluster.
"""

import json
import posixpath
import tempfile

from ... import remote_driver
from ...config import require
from ...context import JobHandle, JobState, JobStatus, StageProgress
from ...exceptions import SparkConfigError, SparkException
from ...packaging import build_package, pickle_inputs, python_version, read_source
from .. import JobBackend
from .client import DatabricksClient
from .compute import resolve_compute
from .storage import DatabricksStorage, is_cloud_uri, is_volume_path, normalize_volume

DEFAULT_VOLUME_HINT = (
    "Set it with @spark(volume='/Volumes/<catalog>/<schema>/<volume>'), the 'volume' key "
    "of the databricks config section, or METAFLOW_DATABRICKS_VOLUME."
)

#: Jobs API terminal result states that are not successes, mapped to something the user
#: can act on rather than a bare enum name.
_FAILURE_EXPLANATIONS = {
    "TIMEDOUT": "the run exceeded its timeout",
    "CANCELED": "the run was cancelled",
    "MAXIMUM_CONCURRENT_RUNS_REACHED": (
        "the job's max_concurrent_runs limit was reached"
    ),
    "UPSTREAM_FAILED": "an upstream task failed",
    "UPSTREAM_CANCELED": "an upstream task was cancelled",
    "EXCLUDED": "the run was excluded",
    "SUCCESS_WITH_FAILURES": "some tasks failed",
}

_RUNNING_STATES = {
    "PENDING",
    "QUEUED",
    "RUNNING",
    "TERMINATING",
    "BLOCKED",
    "WAITING_FOR_RETRY",
}


class DatabricksJobsBackend(JobBackend):
    name = "databricks-jobs"

    def __init__(self, config, ctx=None):
        super().__init__(config, ctx)
        self.client = DatabricksClient(config)
        self.storage = DatabricksStorage(self.client)

    # ------------------------------------------------------------------
    def poll(self, handle):
        run = self.client.api(
            "GET", "/api/2.2/jobs/runs/get", query={"run_id": handle.job_id}
        )
        return self._status_from_run(run, handle)

    def _status_from_run(self, run, handle=None):
        # API 2.2 reports `status`; 2.1 and earlier report `state`. Support both so the
        # backend keeps working against whichever the workspace serves.
        status_block = run.get("status") or {}
        state_block = run.get("state") or {}

        raw_state = status_block.get("state") or state_block.get("life_cycle_state")
        result_state = state_block.get("result_state")
        termination = status_block.get("termination_details") or {}

        message = (
            termination.get("message")
            or state_block.get("state_message")
            or status_block.get("queue_details", {}).get("code")
        )
        error_class = termination.get("code") or result_state

        if raw_state in _RUNNING_STATES:
            state = JobState.RUNNING if raw_state == "RUNNING" else JobState.PENDING
        elif raw_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            state = self._terminal_state(raw_state, result_state, termination)
        else:
            state = JobState.PENDING

        if state == JobState.FAILED and result_state in _FAILURE_EXPLANATIONS:
            explanation = _FAILURE_EXPLANATIONS[result_state]
            message = "%s (%s)" % (explanation, message) if message else explanation

        cluster_id = _cluster_id(run)
        ui_url = run.get("run_page_url") or (handle.ui_url if handle else None)
        return JobStatus(
            state=state,
            message=message,
            error_class=error_class,
            ui_url=ui_url,
            spark_ui_url=self.client.spark_ui_url(cluster_id),
            stages=_stage_progress(run),
        )

    @staticmethod
    def _terminal_state(raw_state, result_state, termination):
        if raw_state in ("SKIPPED", "INTERNAL_ERROR"):
            return JobState.FAILED

        # API 2.1 and earlier: result_state is authoritative when present.
        if result_state is not None:
            if result_state == "SUCCESS":
                return JobState.SUCCESS
            if result_state == "CANCELED":
                return JobState.CANCELLED
            return JobState.FAILED

        # API 2.2: termination_details carries the outcome.
        code = termination.get("code")
        if code in ("CANCELED", "USER_CANCELED"):
            return JobState.CANCELLED
        termination_type = termination.get("type")
        if termination_type == "SUCCESS" or code == "SUCCESS":
            return JobState.SUCCESS
        if termination_type in ("INTERNAL_ERROR", "CLIENT_ERROR"):
            return JobState.FAILED

        # TERMINATED with no outcome reported at all. Treat as success rather than
        # inventing a failure the user would then have to disprove.
        return JobState.SUCCESS

    def cancel(self, handle):
        self.client.api(
            "POST", "/api/2.2/jobs/runs/cancel", body={"run_id": int(handle.job_id)}
        )

    # ------------------------------------------------------------------
    def fetch_logs(self, handle, stream="stdout"):
        # On failure the driver leaves the Python traceback in a result marker, which
        # says more than everything else here put together.
        if stream == "stderr":
            error = self._marker_error(handle)
            if error:
                return error

        parts = []
        run, output = self._run_output(handle)
        for key in ("error_trace", "error", "logs"):
            value = output.get(key)
            if value and value not in parts:
                parts.append(value)

        log_dir = handle.log_url
        if log_dir and stream in ("stdout", "stderr"):
            # A new cluster usually has no id yet when the run is submitted.
            cluster_id = handle.extra.get("cluster_id") or _cluster_id(run)
            if cluster_id:
                path = posixpath.join(log_dir, cluster_id, "driver", stream)
                text = self.storage.read_text(path)
                if text:
                    parts.append(text)

        return "\n".join(p for p in parts if p) or None

    def _marker_error(self, handle):
        marker = handle.extra.get("result_path")
        text = self.storage.read_text(marker) if marker else None
        if not text:
            return None
        try:
            return json.loads(text).get("error")
        except ValueError:
            return None

    def _run_output(self, handle):
        """The run and its task's output, each empty if unavailable."""
        try:
            run = self.client.api(
                "GET", "/api/2.2/jobs/runs/get", query={"run_id": handle.job_id}
            )
        except Exception:
            return {}, {}
        tasks = run.get("tasks") or []
        task_run_id = tasks[0].get("run_id") if tasks else handle.job_id
        try:
            output = self.client.api(
                "GET", "/api/2.2/jobs/runs/get-output", query={"run_id": task_run_id}
            )
        except Exception:
            output = {}
        return run, output

    # ------------------------------------------------------------------
    def _submit_run(self, payload):
        response = self.client.api("POST", "/api/2.2/jobs/runs/submit", body=payload)
        run_id = response.get("run_id")
        if run_id is None:
            raise SparkException(
                "Databricks did not return a run_id for the submitted job: %s"
                % response
            )
        return str(run_id)

    def submit(self, ctx):
        config = self.config
        if ctx.job_func is None:
            raise SparkConfigError(
                "mode='job' needs a job function: @spark(backend='databricks', "
                "mode='job', job=mymodule.run).\n"
                "Alternatively leave mode unset, which runs the step body itself "
                "against remote Spark through Connect and needs no packaging."
            )

        volume = normalize_volume(
            require(config, "volume", "databricks", DEFAULT_VOLUME_HINT),
            "staging volume",
        )
        staging = posixpath.join(volume, "metaflow", ctx.pathspec)

        package = build_package(
            ctx.job_func,
            include=config.get("include"),
            extra_modules=config.get("packages"),
        )
        for warning in package.warnings:
            ctx.log(warning, stream="stderr")

        # Serverless compute cannot run a python_file from a Volume, though it can read
        # the rest of the staged files there, so the driver goes in the workspace.
        driver_dir = posixpath.join(
            config.get("workspace_dir")
            or posixpath.join(self.storage.home(), "metaflow"),
            ctx.pathspec,
        )
        driver_path = self.storage.upload_workspace_file(
            posixpath.join(driver_dir, "remote_driver.py"),
            read_source(remote_driver.__file__),
        )
        package_path = self.storage.upload(
            posixpath.join(staging, "code.tar.gz"), package.blob
        )
        inputs_path = self.storage.upload(
            posixpath.join(staging, "inputs.pickle"), pickle_inputs(ctx.inputs)
        )
        ctx.log("staged %.1fKB code package at %s" % (package.size / 1024.0, staging))

        output_table = config.get("output_table")
        output_path = config.get("output_path") or posixpath.join(staging, "out")
        result_path = posixpath.join(staging, "result.json")

        compute = resolve_compute(
            config,
            tags=ctx.tags,
            cloud=config.get("cloud") or cloud_from_host(self.client.host),
        )
        ctx.log("target: %s" % compute.describe())

        driver_conf = {
            "package_path": package_path,
            "inputs_path": inputs_path,
            "module_name": package.module_name,
            "func_name": ctx.job_func.__name__,
            "output_path": output_path,
            "output_table": output_table,
            "write_mode": config.get("write_mode", "overwrite"),
            "partition_by": config.get("partition_by"),
            "result_path": result_path,
            "expected_python": python_version(),
            "spark_parameters": config.get("spark-parameters") or {},
            "app_name": ctx.job_name,
        }

        task = {
            "task_key": "main",
            "spark_python_task": {
                "python_file": driver_path,
                "parameters": [json.dumps(driver_conf)],
            },
        }
        task.update(compute.task_fields())

        log_dir = None
        if not compute.is_serverless and compute.new_cluster is not None:
            # Give the cluster somewhere we can read its driver logs from afterwards.
            log_dir = posixpath.join(staging, "cluster-logs")
            self.storage.makedirs(log_dir)
            compute.new_cluster.setdefault(
                "cluster_log_conf", {"volumes": {"destination": log_dir}}
            )
            task["new_cluster"] = compute.new_cluster

        libraries = config.get("libraries")
        if libraries:
            if compute.is_serverless:
                raise SparkConfigError(
                    "Serverless compute does not take cluster libraries. Use "
                    "@spark(dependencies=['pkg==1.0']) instead."
                )
            task["libraries"] = libraries

        payload = {
            "run_name": ctx.job_name,
            "tasks": [task],
            "queue": {"enabled": config.get("queue", True)},
        }
        payload.update(compute.submit_fields(config.get("dependencies")))
        if ctx.timeout_minutes:
            # Let Databricks own the timeout so the run terminates itself with a
            # TIMEDOUT result. Our own deadline is only a backstop.
            payload["timeout_seconds"] = int(ctx.timeout_minutes * 60)
        if config.get("health"):
            payload["health"] = config["health"]

        run_id = self._submit_run(payload)
        run = self.client.api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        return JobHandle(
            backend=self.name,
            job_id=run_id,
            ui_url=run.get("run_page_url") or self.client.run_url(run_id),
            spark_ui_url=self.client.spark_ui_url(_cluster_id(run)),
            output_url=output_table or output_path,
            log_url=log_dir,
            extra={
                "result_path": result_path,
                "staging": staging,
                "driver_dir": driver_dir,
                "cluster_id": _cluster_id(run),
                "output_table": output_table,
                "output_path": output_path,
                "serverless": compute.is_serverless,
            },
        )

    def read_output(self, handle, output_format):
        """Materialize the job's output for the step."""
        from ...output import from_arrow_table, from_storage, read_parquet

        table = handle.extra.get("output_table")
        if table:
            if output_format == "url":
                return table
            from ...catalog.unity import UnityCatalogTable

            ref = UnityCatalogTable(table, client=self.client, config=self.config)
            if output_format == "table":
                return ref
            if output_format == "none":
                return None
            return from_arrow_table(ref.to_arrow(), output_format)

        path = handle.extra.get("output_path")
        if output_format in ("url", "table"):
            return path
        if output_format == "none" or path is None:
            return None

        if is_cloud_uri(path):
            return from_storage(path, output_format)
        if is_volume_path(path):
            with tempfile.TemporaryDirectory(prefix="metaflow-spark-out-") as tmp:
                files = self.storage.download_dir(path, tmp, suffix=".parquet")
                if not files:
                    raise SparkException(
                        "The Spark job wrote no Parquet files to %s. If the job returns "
                        "no DataFrame, set output_artifact=None." % path
                    )
                table = read_parquet(tmp)
            return from_arrow_table(table, output_format)
        raise SparkConfigError("Do not know how to read the output at %r." % path)

    def cleanup(self, handle, ctx):
        driver_dir = handle.extra.get("driver_dir")
        if driver_dir:
            self.storage.delete_workspace_path(driver_dir)
        staging = handle.extra.get("staging")
        if staging:
            for name in ("code.tar.gz", "inputs.pickle"):
                self.storage.delete(posixpath.join(staging, name))


# ----------------------------------------------------------------------
def cloud_from_host(host):
    """The cloud a workspace runs on, which decides the default node type."""
    host = (host or "").lower()
    if ".azuredatabricks.net" in host:
        return "azure"
    if ".gcp.databricks.com" in host:
        return "gcp"
    return "aws"


def _cluster_id(run):
    instance = run.get("cluster_instance") or {}
    if instance.get("cluster_id"):
        return instance["cluster_id"]
    for task in run.get("tasks") or []:
        instance = task.get("cluster_instance") or {}
        if instance.get("cluster_id"):
            return instance["cluster_id"]
    return None


def _stage_progress(run):
    """Coarse per-task progress, which is what the Jobs API actually exposes."""
    stages = []
    for task in run.get("tasks") or []:
        state = task.get("status") or task.get("state") or {}
        stages.append(
            StageProgress(
                stage_id=task.get("task_key"),
                name=task.get("description") or task.get("task_key"),
                status=state.get("state") or state.get("life_cycle_state"),
            )
        )
    return stages
