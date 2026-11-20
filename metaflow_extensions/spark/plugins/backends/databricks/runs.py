"""Triggering a Databricks job that already exists in the workspace.

The job definition owns everything about how it runs: tasks, compute, libraries,
permissions, and its own timeout. This backend only starts a run with `run-now`, waits
for it, cancels it if the step is interrupted, and reads back what each task returned.
"""

import json

from ...context import JobHandle, JobState, JobStatus, StageProgress
from ...exceptions import SparkConfigError, SparkException
from .. import JobBackend
from .client import DatabricksClient

#: Jobs API terminal result states that are not successes, mapped to something the user
#: can act on rather than a bare enum name.
_FAILURE_EXPLANATIONS = {
    "TIMEDOUT": "the run exceeded its timeout",
    "CANCELED": "the run was cancelled",
    "MAXIMUM_CONCURRENT_RUNS_REACHED": "the job's max_concurrent_runs limit was reached",
    "UPSTREAM_FAILED": "an upstream task failed",
    "UPSTREAM_CANCELED": "an upstream task was cancelled",
    "EXCLUDED": "the run was excluded",
    "SUCCESS_WITH_FAILURES": "some tasks failed",
}

#: Task state for a task that never ran, because an upstream task failed or it was
#: excluded by a condition.
SKIPPED = "SKIPPED"

_RUNNING_STATES = {
    "PENDING",
    "QUEUED",
    "RUNNING",
    "TERMINATING",
    "BLOCKED",
    "WAITING_FOR_RETRY",
}


class DatabricksExistingJobBackend(JobBackend):
    name = "databricks-job"

    def __init__(self, config, ctx=None):
        super().__init__(config, ctx)
        self.client = DatabricksClient(config)

    # ------------------------------------------------------------------
    def submit(self, ctx):
        config = self.config
        job_id = config.get("job_id") or self._lookup_job_id(config.get("job_name"))

        body: dict = {"job_id": int(job_id)}
        if ctx.inputs:
            field = self._parameter_field(job_id, ctx.inputs)
            body[field] = {k: _stringify(v) for k, v in ctx.inputs.items()}
        if config.get("idempotency_token"):
            body["idempotency_token"] = config["idempotency_token"]
        # Sent either way, since leaving it out falls back to the job's own setting.
        body["queue"] = {"enabled": bool(config.get("queue", True))}

        response = self.client.api("POST", "/api/2.2/jobs/run-now", body=body)
        run_id = response.get("run_id")
        if run_id is None:
            raise SparkException(
                "Databricks did not return a run_id for job %s: %s" % (job_id, response)
            )
        run_id = str(run_id)
        try:
            ui_url = self._get_run_by_id(run_id).get("run_page_url")
        except Exception:
            ui_url = None
        return JobHandle(
            backend=self.name,
            job_id=run_id,
            ui_url=ui_url or self.client.run_url(run_id),
            extra={"job_id": str(job_id)},
        )

    def _lookup_job_id(self, job_name):
        if not job_name:
            raise SparkConfigError("@databricks_job needs job_id=... or job_name=...")
        response = self.client.api(
            "GET", "/api/2.2/jobs/list", query={"name": job_name, "limit": 25}
        )
        matches = [
            j
            for j in response.get("jobs") or []
            if (j.get("settings") or {}).get("name") == job_name
        ]
        if not matches:
            raise SparkConfigError(
                "No Databricks job named %r was found, or you lack CAN VIEW on it."
                % job_name
            )
        if len(matches) > 1:
            raise SparkConfigError(
                "%d Databricks jobs are named %r (job ids %s). Pass job_id=... instead."
                % (
                    len(matches),
                    job_name,
                    ", ".join(str(j["job_id"]) for j in matches),
                )
            )
        return matches[0]["job_id"]

    def _parameter_field(self, job_id, params):
        """The run-now field the parameters go in, after checking the job takes them."""
        job = self.client.api("GET", "/api/2.2/jobs/get", query={"job_id": job_id})
        settings = job.get("settings") or {}
        declared = [p["name"] for p in settings.get("parameters") or []]
        if not declared:
            # Jobs without job-level parameters only take the legacy per-task-type
            # fields, and of those only notebook widgets are keyed by name.
            tasks = settings.get("tasks") or []
            if tasks and all("notebook_task" in t for t in tasks):
                return "notebook_params"
            raise SparkConfigError(
                "Databricks job %s declares no job parameters, so it can't be passed "
                "%s. Add them under the job's Parameters." % (job_id, ", ".join(params))
            )
        # Databricks silently drops job_parameters the job doesn't declare, so a typo
        # would run the job with its default instead.
        unknown = sorted(set(params) - set(declared))
        if unknown:
            raise SparkConfigError(
                "Databricks job %s does not declare the parameter(s) %s, so they would "
                "be ignored. It declares: %s."
                % (job_id, ", ".join(unknown), ", ".join(declared))
            )
        return "job_parameters"

    # ------------------------------------------------------------------
    def poll(self, handle):
        run = self._get_run(handle)
        status = self._status_from_run(run)
        status.ui_url = run.get("run_page_url") or handle.ui_url
        status.stages = [
            StageProgress(
                stage_id=task.get("task_key"),
                name=task.get("task_key"),
                num_tasks=1,
                num_completed=int(self._status_from_run(task).terminal),
            )
            for task in _latest_attempts(run)
        ]
        return status

    def _get_run(self, handle):
        return self._get_run_by_id(handle.job_id)

    def _get_run_by_id(self, run_id):
        return self.client.api(
            "GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id}
        )

    @classmethod
    def _status_from_run(cls, run):
        """Classify a run, or one task of a run, which has the same state blocks."""
        # API 2.2 reports `status`; 2.1 and earlier report `state`. Support both so the
        # backend keeps working against whichever the workspace serves.
        status_block = run.get("status") or {}
        state_block = run.get("state") or {}

        raw_state = status_block.get("state") or state_block.get("life_cycle_state")
        result_state = state_block.get("result_state")
        termination = status_block.get("termination_details") or {}

        message = termination.get("message") or state_block.get("state_message")
        error_class = termination.get("code") or result_state

        if raw_state in _RUNNING_STATES:
            state = JobState.RUNNING if raw_state == "RUNNING" else JobState.PENDING
        elif raw_state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            state = cls._terminal_state(raw_state, result_state, termination)
        else:
            state = JobState.PENDING

        if state == JobState.FAILED and result_state in _FAILURE_EXPLANATIONS:
            explanation = _FAILURE_EXPLANATIONS[result_state]
            message = "%s (%s)" % (explanation, message) if message else explanation

        return JobStatus(state=state, message=message or None, error_class=error_class)

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
        """The output of each task: errors on stderr, printed logs on stdout."""
        keys = ("error", "error_trace") if stream == "stderr" else ("logs",)
        parts = []
        for task, output in self._task_outputs(handle):
            if stream == "stderr" and _task_state(task) != JobState.FAILED:
                continue
            text = "\n".join(output[k] for k in keys if output.get(k))
            if text:
                parts.append("[%s]\n%s" % (task.get("task_key"), text.rstrip()))
        return "\n".join(parts) or None

    def task_results(self, handle):
        """Each task's state, error, and notebook exit value, keyed by task_key."""
        results = {}
        for task, output in self._task_outputs(handle):
            state = _task_state(task)
            entry = {"state": state, "run_id": str(task.get("run_id"))}
            if state == JobState.FAILED:
                entry["error"] = (
                    output.get("error") or self._status_from_run(task).message
                )
            notebook_output = output.get("notebook_output") or {}
            if "result" in notebook_output:
                entry["result"] = notebook_output["result"]
                if notebook_output.get("truncated") and self.ctx is not None:
                    # Databricks caps exit values; silence would look like data loss.
                    self.ctx.log(
                        "task %s's exit value was truncated by Databricks; write "
                        "large results to a table instead" % task.get("task_key"),
                        stream="stderr",
                    )
            results[task.get("task_key")] = entry
        return results

    def _task_outputs(self, handle):
        """(task, output) for every task of the run; output is empty if unavailable."""
        for task in _latest_attempts(self._get_run(handle)):
            try:
                output = self.client.api(
                    "GET",
                    "/api/2.2/jobs/runs/get-output",
                    query={"run_id": task.get("run_id")},
                )
            except Exception:
                output = {}
            yield task, output


def _latest_attempts(run):
    """One entry per task: a retried task is listed once for every attempt."""
    latest = {}
    for task in run.get("tasks") or []:
        key = task.get("task_key")
        if key not in latest or task.get("attempt_number", 0) >= latest[key].get(
            "attempt_number", 0
        ):
            latest[key] = task
    return list(latest.values())


def _task_state(task):
    """A task's JobState, or SKIPPED for a task that never ran."""
    state_block = task.get("state") or {}
    termination = (task.get("status") or {}).get("termination_details") or {}
    if state_block.get("life_cycle_state") == "SKIPPED" or termination.get("code") in (
        "SKIPPED",
        "EXCLUDED",
    ):
        return SKIPPED
    return DatabricksExistingJobBackend._status_from_run(task).state


def _stringify(value):
    """Job parameters are strings; JSON-encode anything that isn't one already."""
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value)
    except TypeError:
        return str(value)
