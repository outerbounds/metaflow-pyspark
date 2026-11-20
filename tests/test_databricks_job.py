import pytest

from metaflow_extensions.spark.plugins.backends.databricks import runs
from metaflow_extensions.spark.plugins.context import JobHandle, JobState, TaskContext
from metaflow_extensions.spark.plugins.databricks_job import (
    DatabricksJobDecorator,
    _parameters,
)
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError

Backend = runs.DatabricksExistingJobBackend


def task(key, run_id, result_state, attempt=0, life_cycle="TERMINATED", code=None):
    return {
        "task_key": key,
        "run_id": run_id,
        "attempt_number": attempt,
        "state": {"life_cycle_state": life_cycle, "result_state": result_state},
        "status": {
            "state": "TERMINATED",
            "termination_details": {"code": code or result_state},
        },
    }


class FakeJobsAPI:
    """Answers the Jobs API calls the backend makes, and records them."""

    host = "https://adb-1.azuredatabricks.net"

    def __init__(self, jobs=(), declared=(), tasks=(), outputs=None, job_tasks=None):
        self.calls = []
        self.jobs = list(jobs)
        self.declared = list(declared)
        self.job_tasks = job_tasks or [{"task_key": "t", "spark_python_task": {}}]
        self.tasks = list(tasks)
        self.outputs = outputs or {}

    def api(self, method, path, body=None, query=None):
        self.calls.append((method, path, body, query))
        if path.endswith("/jobs/list"):
            return {"jobs": self.jobs}
        if path.endswith("/jobs/get"):
            return {
                "settings": {
                    "parameters": [{"name": n} for n in self.declared],
                    "tasks": self.job_tasks,
                }
            }
        if path.endswith("/run-now"):
            return {"run_id": 77}
        if path.endswith("/runs/get-output"):
            return self.outputs.get(query["run_id"], {})
        if path.endswith("/runs/get"):
            return {"run_page_url": "https://adb-1/#job/5/run/77", "tasks": self.tasks}
        return {}

    def run_url(self, run_id):
        return "%s/jobs/runs/%s" % (self.host, run_id)

    def body(self, suffix):
        return next(b for _, path, b, _ in self.calls if path.endswith(suffix))


def make_backend(api, **config):
    backend = Backend.__new__(Backend)
    backend.config = config
    backend.ctx = None
    backend.client = api
    return backend


def ctx(**inputs):
    return TaskContext(
        step_name="enrich",
        pathspec="DailyFlow/42/enrich/7",
        flow_name="DailyFlow",
        run_id="42",
        task_id="7",
        attempt=0,
        user=None,
        tags={},
        inputs=inputs,
        logger=lambda *a, **k: None,
    )


HANDLE = JobHandle(backend="databricks-job", job_id="77")


# ----------------------------------------------------------------------
# finding the job and starting it
# ----------------------------------------------------------------------
def named(job_id, name):
    return {"job_id": job_id, "settings": {"name": name}}


def test_a_job_is_found_by_exact_name_and_run_with_its_parameters():
    api = FakeJobsAPI(
        jobs=[named(5, "nightly"), named(6, "nightly-backfill")], declared=["run_date"]
    )
    handle = make_backend(api, job_name="nightly").submit(ctx(run_date="2024-05-17"))
    body = api.body("/run-now")
    assert body["job_id"] == 5
    assert body["job_parameters"] == {"run_date": "2024-05-17"}
    assert body["queue"] == {"enabled": True}
    assert handle.job_id == "77"
    assert handle.ui_url == "https://adb-1/#job/5/run/77"
    assert handle.extra["job_id"] == "5"


def test_a_missing_job_name_says_so():
    with pytest.raises(SparkConfigError, match="No Databricks job named 'nightly'"):
        make_backend(FakeJobsAPI(), job_name="nightly").submit(ctx())


def test_an_ambiguous_job_name_lists_the_ids():
    api = FakeJobsAPI(jobs=[named(5, "nightly"), named(9, "nightly")])
    with pytest.raises(SparkConfigError, match="5, 9"):
        make_backend(api, job_name="nightly").submit(ctx())


def test_a_job_id_skips_the_lookup():
    api = FakeJobsAPI()
    make_backend(api, job_id="5").submit(ctx())
    assert not any(path.endswith("/jobs/list") for _, path, _, _ in api.calls)
    assert "job_parameters" not in api.body("/run-now")


def test_parameters_the_job_does_not_declare_are_rejected():
    # Databricks would drop them and run the job with its defaults.
    api = FakeJobsAPI(declared=["run_date"])
    with pytest.raises(SparkConfigError, match="rundate.*It declares: run_date"):
        make_backend(api, job_id="5").submit(ctx(rundate="2024-05-17"))
    assert not any(path.endswith("/run-now") for _, path, _, _ in api.calls)


def test_a_notebook_job_without_job_parameters_gets_notebook_params():
    # Databricks rejects notebook_params on jobs that declare job parameters, and
    # job_parameters are dropped on jobs that don't.
    api = FakeJobsAPI(job_tasks=[{"task_key": "a", "notebook_task": {}}])
    make_backend(api, job_id="5").submit(ctx(region="apac"))
    body = api.body("/run-now")
    assert body["notebook_params"] == {"region": "apac"}
    assert "job_parameters" not in body


def test_other_jobs_without_job_parameters_cannot_take_parameters():
    api = FakeJobsAPI()
    with pytest.raises(SparkConfigError, match="declares no job parameters"):
        make_backend(api, job_id="5").submit(ctx(region="apac"))


def test_non_string_parameters_are_json_encoded():
    api = FakeJobsAPI(declared=["limit", "regions", "dry_run"])
    make_backend(api, job_id="5").submit(
        ctx(limit=10, regions=["eu", "us"], dry_run=False)
    )
    assert api.body("/run-now")["job_parameters"] == {
        "limit": "10",
        "regions": '["eu", "us"]',
        "dry_run": "false",
    }


def test_idempotency_token_and_queue_are_passed_through():
    api = FakeJobsAPI()
    make_backend(api, job_id="5", idempotency_token="tok", queue=False).submit(ctx())
    body = api.body("/run-now")
    assert body["idempotency_token"] == "tok"
    assert body["queue"] == {"enabled": False}


# ----------------------------------------------------------------------
# run and task states
# ----------------------------------------------------------------------
def test_api_22_cancellation_reports_cancelled_even_with_a_success_code():
    # What a cancelled run actually returns: termination code SUCCESS, result CANCELED.
    run = {
        "status": {"state": "TERMINATED", "termination_details": {"code": "SUCCESS"}},
        "state": {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"},
    }
    assert Backend._status_from_run(run).state == JobState.CANCELLED


def test_a_failed_run_explains_upstream_failures():
    run = {
        "state": {"life_cycle_state": "TERMINATED", "result_state": "UPSTREAM_FAILED"}
    }
    status = Backend._status_from_run(run)
    assert status.state == JobState.FAILED
    assert "upstream task failed" in status.message


def test_poll_counts_finished_tasks_once_per_task():
    api = FakeJobsAPI(
        tasks=[
            task("check", 1, "FAILED", attempt=0),
            task("check", 2, "SUCCESS", attempt=1),
            dict(
                task("summarize", 3, None),
                state={"life_cycle_state": "RUNNING"},
                status={"state": "RUNNING"},
            ),
        ]
    )
    status = make_backend(api).poll(HANDLE)
    assert status.ui_url == "https://adb-1/#job/5/run/77"
    assert [(s.stage_id, s.num_completed) for s in status.stages] == [
        ("check", 1),
        ("summarize", 0),
    ]


# ----------------------------------------------------------------------
# what comes back
# ----------------------------------------------------------------------
FAILED_RUN = [
    task("summarize", 3, "UPSTREAM_FAILED", life_cycle="SKIPPED", code="SKIPPED"),
    task("check", 1, "FAILED", attempt=0),
    task("check", 2, "FAILED", attempt=1),
]
FAILED_OUTPUTS = {
    1: {"error": "ValueError: first attempt"},
    2: {"error": "ValueError: bad run_date", "error_trace": "Traceback ..."},
    3: {"error": "Run result unavailable: the task was skipped"},
}


def test_results_hold_each_tasks_state_error_and_exit_value():
    api = FakeJobsAPI(
        tasks=[task("check", 1, "SUCCESS"), task("summarize", 2, "SUCCESS")],
        outputs={
            1: {"notebook_output": {"result": "ok"}},
            2: {"notebook_output": {"result": '{"orders": 3333}'}},
        },
    )
    assert make_backend(api).task_results(HANDLE) == {
        "check": {"state": "SUCCESS", "run_id": "1", "result": "ok"},
        "summarize": {"state": "SUCCESS", "run_id": "2", "result": '{"orders": 3333}'},
    }


def test_a_retried_task_reports_its_last_attempt_and_skipped_tasks_are_skipped():
    api = FakeJobsAPI(tasks=FAILED_RUN, outputs=FAILED_OUTPUTS)
    results = make_backend(api).task_results(HANDLE)
    assert results["check"] == {
        "state": "FAILED",
        "run_id": "2",
        "error": "ValueError: bad run_date",
    }
    assert results["summarize"] == {"state": "SKIPPED", "run_id": "3"}


def test_failure_output_comes_from_the_failed_tasks_last_attempt_only():
    api = FakeJobsAPI(tasks=FAILED_RUN, outputs=FAILED_OUTPUTS)
    logs = make_backend(api).fetch_logs(HANDLE, "stderr")
    assert logs == "[check]\nValueError: bad run_date\nTraceback ..."


def test_stdout_is_the_printed_logs_of_every_task():
    api = FakeJobsAPI(
        tasks=[task("a", 1, "SUCCESS"), task("b", 2, "SUCCESS")],
        outputs={1: {"logs": "hello"}, 2: {}},
    )
    assert make_backend(api).fetch_logs(HANDLE, "stdout") == "[a]\nhello"


def test_a_truncated_exit_value_is_reported():
    logged = []
    api = FakeJobsAPI(
        tasks=[task("a", 1, "SUCCESS")],
        outputs={1: {"notebook_output": {"result": "x" * 10, "truncated": True}}},
    )
    backend = make_backend(api)
    backend.ctx = ctx()
    backend.ctx.logger = lambda msg, job_id=None, stream="stdout": logged.append(msg)
    backend.task_results(HANDLE)
    assert any("truncated" in msg for msg in logged)


# ----------------------------------------------------------------------
# the decorator
# ----------------------------------------------------------------------
def make_decorator(**attributes):
    deco = DatabricksJobDecorator(attributes=attributes)
    deco.step_init(None, None, "enrich", [], None, None, None)
    return deco


def test_the_decorator_needs_a_job():
    with pytest.raises(SparkConfigError, match="job_id=... or job_name="):
        make_decorator()


def test_the_decorator_takes_one_way_of_naming_the_job():
    with pytest.raises(SparkConfigError, match="not both"):
        make_decorator(job_id=5, job_name="nightly")


def test_there_is_no_default_timeout():
    # The job's own timeout applies; a 60-minute default would cancel long pipelines.
    assert make_decorator(job_id=5).attributes["timeout"] is None


class Flow:
    run_date = "2024-05-17"


def test_parameters_from_reads_the_flow_and_overrides_literals():
    params = _parameters(
        Flow(),
        {
            "parameters": {"run_date": "x", "env": "prod"},
            "parameters_from": ["run_date"],
        },
    )
    assert params == {"run_date": "2024-05-17", "env": "prod"}


def test_parameters_from_names_a_missing_attribute():
    with pytest.raises(SparkConfigError, match="'region'"):
        _parameters(Flow(), {"parameters_from": ["region"]})
