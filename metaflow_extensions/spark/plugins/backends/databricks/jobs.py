"""Submitting work to the Databricks Jobs API.

Two backends share the polling and cancellation logic here:

``DatabricksJobsBackend``
    Packages the flow's own Spark code and submits it as a one-off run. This is what you
    need when Connect is not enough: a pinned Databricks Runtime, Photon, cluster-scoped
    libraries, JVM UDFs, or a driver that has to live on the cluster.

``DatabricksExistingJobBackend``
    Triggers a job that already exists in the workspace and waits for it. Requires no
    migration at all, which makes it the cheapest way for a team to put Metaflow in
    front of pipelines they already run.
"""

import json
import posixpath
import tempfile

from .. import JobBackend
from ...config import require
from ...context import JobHandle, JobState, JobStatus, StageProgress
from ...exceptions import SparkConfigError, SparkException
from ...packaging import build_package, pickle_inputs, python_version, read_source
from ... import remote_driver
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


class _DatabricksRunBackend(JobBackend):
    """Shared Jobs API run handling."""

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
        parts = []

        # The driver writes a result marker containing the Python traceback, which is
        # far more useful than the Spark log noise around it.
        marker = handle.extra.get("result_path")
        if marker:
            text = self.storage.read_text(marker)
            if text:
                try:
                    result = json.loads(text)
                except ValueError:
                    result = {}
                if result.get("error"):
                    parts.append(result["error"])

        output = self._run_output(handle)
        for key in ("error_trace", "error", "logs"):
            value = output.get(key)
            if value and value not in parts:
                parts.append(value)

        log_dir = handle.log_url
        if log_dir and stream in ("stdout", "stderr"):
            cluster_id = handle.extra.get("cluster_id")
            if cluster_id:
                path = posixpath.join(log_dir, cluster_id, "driver", stream)
                text = self.storage.read_text(path)
                if text:
                    parts.append(text)

        return "\n".join(p for p in parts if p) or None

    def _run_output(self, handle):
        try:
            run = self.client.api(
                "GET", "/api/2.2/jobs/runs/get", query={"run_id": handle.job_id}
            )
            tasks = run.get("tasks") or []
            task_run_id = tasks[0].get("run_id") if tasks else handle.job_id
            return self.client.api(
                "GET",
                "/api/2.2/jobs/runs/get-output",
                query={"run_id": task_run_id},
            )
        except Exception:
            return {}

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


class DatabricksJobsBackend(_DatabricksRunBackend):
    name = "databricks-jobs"

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
        staging = posixpath.join(volume, "metaflow", ctx.pathspec.replace("/", "/"))

        package = build_package(
            ctx.job_func,
            include=config.get("include"),
            extra_modules=config.get("packages"),
        )
        for warning in package.warnings:
            ctx.log(warning, stream="stderr")

        driver_path = self.storage.upload(
            posixpath.join(staging, "remote_driver.py"),
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
            config, tags=ctx.tags, cloud=config.get("cloud", "aws")
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
        if ctx.tags:
            payload["custom_tags"] = dict(ctx.tags)
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
        staging = handle.extra.get("staging")
        if staging:
            for name in ("remote_driver.py", "code.tar.gz", "inputs.pickle"):
                self.storage.delete(posixpath.join(staging, name))


class DatabricksExistingJobBackend(_DatabricksRunBackend):
    """Trigger a job that already exists in the workspace."""

    name = "databricks-existing-job"

    def submit(self, ctx):
        config = self.config
        job_id = config.get("job_id")
        job_name = config.get("job_name")
        if not job_id and not job_name:
            raise SparkConfigError(
                "@databricks_job needs either job_id=... or job_name=..."
            )
        if not job_id:
            job_id = self._lookup_job_id(job_name)

        body: dict = {"job_id": int(job_id)}
        params = ctx.inputs or {}
        if params:
            # job_parameters is the modern form and works for every task type; the older
            # notebook_params/python_params only apply to specific ones.
            body["job_parameters"] = {k: str(v) for k, v in params.items()}
        if config.get("python_params"):
            body["python_params"] = [str(p) for p in config["python_params"]]
        if config.get("idempotency_token"):
            body["idempotency_token"] = config["idempotency_token"]
        if config.get("queue", True):
            body["queue"] = {"enabled": True}

        response = self.client.api("POST", "/api/2.2/jobs/run-now", body=body)
        run_id = response.get("run_id")
        if run_id is None:
            raise SparkException(
                "Databricks did not return a run_id for job %s: %s" % (job_id, response)
            )
        run_id = str(run_id)
        run = self.client.api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        return JobHandle(
            backend=self.name,
            job_id=run_id,
            ui_url=run.get("run_page_url") or self.client.run_url(run_id),
            spark_ui_url=self.client.spark_ui_url(_cluster_id(run)),
            extra={"job_id": str(job_id), "cluster_id": _cluster_id(run)},
        )

    def _lookup_job_id(self, job_name):
        response = self.client.api(
            "GET", "/api/2.2/jobs/list", query={"name": job_name, "limit": 25}
        )
        jobs = response.get("jobs") or []
        exact = [j for j in jobs if (j.get("settings") or {}).get("name") == job_name]
        if not exact:
            available = ", ".join(
                (j.get("settings") or {}).get("name", "?") for j in jobs[:10]
            )
            raise SparkConfigError(
                "No Databricks job named %r was found.%s"
                % (job_name, (" Similar names: %s" % available) if available else "")
            )
        if len(exact) > 1:
            raise SparkConfigError(
                "%d Databricks jobs are named %r. Use job_id=... to disambiguate."
                % (len(exact), job_name)
            )
        return exact[0]["job_id"]

    def task_outputs(self, handle):
        """Collect each task's notebook exit value and metadata."""
        run = self.client.api(
            "GET", "/api/2.2/jobs/runs/get", query={"run_id": handle.job_id}
        )
        outputs = {}
        for task in run.get("tasks") or []:
            key = task.get("task_key")
            try:
                output = self.client.api(
                    "GET",
                    "/api/2.2/jobs/runs/get-output",
                    query={"run_id": task.get("run_id")},
                )
            except Exception:
                continue
            entry = {}
            notebook_output = output.get("notebook_output") or {}
            if "result" in notebook_output:
                entry["result"] = notebook_output["result"]
                entry["truncated"] = notebook_output.get("truncated", False)
            if output.get("error"):
                entry["error"] = output["error"]
            state = task.get("status") or task.get("state") or {}
            entry["state"] = state.get("state") or state.get("result_state")
            outputs[key] = entry
        return outputs


class DatabricksNotebookBackend(_DatabricksRunBackend):
    """Run an existing Databricks notebook as a step.

    The point is not to encourage notebooks in production, it is that teams already have
    them and rewriting is not a precondition for orchestrating them. Whatever the
    notebook passes to ``dbutils.notebook.exit()`` comes back as an artifact.
    """

    name = "databricks-notebook"

    def submit(self, ctx):
        config = self.config
        notebook_path = require(
            config,
            "notebook_path",
            "databricks-notebook",
            "For example: @databricks_notebook(notebook_path='/Users/me/etl')",
        )

        task = {
            "task_key": "main",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    k: _stringify(v) for k, v in (ctx.inputs or {}).items()
                },
            },
        }
        compute = resolve_compute(
            config, tags=ctx.tags, cloud=config.get("cloud", "aws")
        )
        task.update(compute.task_fields())
        ctx.log("target: %s" % compute.describe())

        payload = {
            "run_name": ctx.job_name,
            "tasks": [task],
            "queue": {"enabled": config.get("queue", True)},
        }
        payload.update(compute.submit_fields(config.get("dependencies")))
        if ctx.timeout_minutes:
            payload["timeout_seconds"] = int(ctx.timeout_minutes * 60)
        if ctx.tags:
            payload["custom_tags"] = dict(ctx.tags)

        run_id = self._submit_run(payload)
        run = self.client.api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        return JobHandle(
            backend=self.name,
            job_id=run_id,
            ui_url=run.get("run_page_url") or self.client.run_url(run_id),
            spark_ui_url=self.client.spark_ui_url(_cluster_id(run)),
            extra={"cluster_id": _cluster_id(run), "notebook_path": notebook_path},
        )

    def notebook_result(self, handle):
        """Return whatever the notebook passed to dbutils.notebook.exit()."""
        output = self._run_output(handle)
        notebook_output = output.get("notebook_output") or {}
        result = notebook_output.get("result")
        if result is None:
            return None
        if notebook_output.get("truncated") and self.ctx:
            # Databricks caps the exit value. Silence here would look like data loss.
            self.ctx.log(
                "the notebook's exit value was truncated by Databricks; write large "
                "results to a table or a Volume instead",
                stream="stderr",
            )
        try:
            return json.loads(result)
        except (TypeError, ValueError):
            return result


# ----------------------------------------------------------------------
def _stringify(value):
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)) or value is None:
        return json.dumps(value)
    try:
        return json.dumps(value)
    except TypeError:
        return str(value)


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
