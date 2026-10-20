"""Tests for the shared polling driver.

The failures this guards against: an interrupt leaking a running statement, and a
control-plane outage being reported as a failed one.
"""

import pytest

from metaflow_extensions.spark.plugins import backends
from metaflow_extensions.spark.plugins.backends import StatementBackend
from metaflow_extensions.spark.plugins.context import (
    JobHandle,
    JobState,
    JobStatus,
    TaskContext,
)
from metaflow_extensions.spark.plugins.exceptions import (
    ControlPlaneError,
    QueryCancelled,
    QueryFailed,
    QueryTimeout,
    SparkJobCancelled,
    SparkJobFailed,
)


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    monkeypatch.setattr(backends.time, "sleep", lambda seconds: None)


def make_ctx(timeout_minutes=None):
    return TaskContext(
        step_name="start",
        pathspec="Flow/1/start/2",
        flow_name="Flow",
        run_id="1",
        task_id="2",
        attempt=0,
        user="tester",
        tags={},
        timeout_minutes=timeout_minutes,
        logger=lambda msg, job_id=None, stream="stdout": None,
    )


class FakeBackend(StatementBackend):
    name = "fake"

    def __init__(self, states=(), poll_errors=(), message=None):
        super().__init__({}, None)
        self.states = list(states)
        self.poll_errors = list(poll_errors)
        self.cancelled = 0
        self.message = message
        self.polls = 0

    def submit(self, ctx):
        return JobHandle(backend=self.name, job_id="job-1", ui_url="https://ui/job-1")

    def poll(self, handle):
        self.polls += 1
        if self.poll_errors:
            raise self.poll_errors.pop(0)
        state = self.states.pop(0) if self.states else JobState.SUCCESS
        return JobStatus(state=state, message=self.message)

    def cancel(self, handle):
        self.cancelled += 1


def test_wait_returns_the_terminal_status():
    backend = FakeBackend([JobState.PENDING, JobState.RUNNING, JobState.SUCCESS])
    status = backend.wait(make_ctx(), backend.submit(make_ctx()))
    assert status.state == JobState.SUCCESS
    assert backend.cancelled == 0


def test_timeout_cancels_the_remote_job():
    backend = FakeBackend([JobState.RUNNING] * 50)
    ctx = make_ctx(timeout_minutes=1)
    handle = backend.submit(ctx)
    # A monotonic clock that jumps past the deadline on the second reading.
    times = iter([0.0, 0.0, 0.0, 10_000.0] + [10_000.0] * 50)
    import metaflow_extensions.spark.plugins.backends as mod

    original = mod.time.monotonic
    mod.time.monotonic = lambda: next(times)
    try:
        with pytest.raises(QueryTimeout):
            backend.wait(ctx, handle, timeout_minutes=1)
    finally:
        mod.time.monotonic = original
    assert backend.cancelled == 1


def test_interrupt_cancels_the_remote_job():
    backend = FakeBackend(poll_errors=[KeyboardInterrupt()])
    ctx = make_ctx()
    handle = backend.submit(ctx)
    with pytest.raises(KeyboardInterrupt):
        backend.wait(ctx, handle)
    assert backend.cancelled == 1


def test_a_control_plane_fault_does_not_cancel():
    backend = FakeBackend(poll_errors=[RuntimeError("boom")] * 20)
    ctx = make_ctx()
    handle = backend.submit(ctx)
    with pytest.raises(ControlPlaneError):
        backend.wait(ctx, handle)
    # The statement itself may be perfectly healthy.
    assert backend.cancelled == 0


def test_transient_poll_failures_are_retried():
    backend = FakeBackend(
        states=[JobState.SUCCESS], poll_errors=[ConnectionError("flaky")]
    )
    status = backend.wait(make_ctx(), backend.submit(make_ctx()))
    assert status.state == JobState.SUCCESS


def test_control_plane_outage_is_not_reported_as_a_failed_statement():
    backend = FakeBackend(poll_errors=[ConnectionError("down")] * 20)
    ctx = make_ctx()
    with pytest.raises(ControlPlaneError) as exc:
        backend.wait(ctx, backend.submit(ctx))
    assert "may still be running" in str(exc.value)
    assert not isinstance(exc.value, QueryFailed)


def test_run_raises_query_failed_with_the_error_message():
    backend = FakeBackend([JobState.FAILED], message="[PARSE_SYNTAX_ERROR] near FROM")
    with pytest.raises(QueryFailed) as exc:
        backend.run(make_ctx())
    assert "PARSE_SYNTAX_ERROR" in str(exc.value)


def test_run_distinguishes_cancellation_from_failure():
    backend = FakeBackend([JobState.CANCELLED])
    with pytest.raises(QueryCancelled):
        backend.run(make_ctx())


def test_crash_on_failure_false_returns_the_status():
    backend = FakeBackend([JobState.FAILED])
    handle, status = backend.run(make_ctx(), crash_on_failure=False)
    assert status.state == JobState.FAILED
    assert handle.job_id == "job-1"


# ----------------------------------------------------------------------
# submitted jobs share the loop but report as Spark jobs, with the driver's output
# ----------------------------------------------------------------------
class FakeJobBackend(backends.JobBackend):
    name = "fake-job"

    def __init__(self, states):
        super().__init__({}, None)
        self.states = list(states)

    def submit(self, ctx):
        return JobHandle(backend=self.name, job_id="run-1", ui_url="https://ui/run-1")

    def poll(self, handle):
        return JobStatus(state=self.states.pop(0), message="task failed")

    def cancel(self, handle):
        pass

    def fetch_logs(self, handle, stream="stdout"):
        return "Traceback: AnalysisException" if stream == "stderr" else "driver out"


def test_a_failed_job_raises_spark_job_failed_with_the_driver_output():
    with pytest.raises(SparkJobFailed) as exc:
        FakeJobBackend([JobState.FAILED]).run(make_ctx())
    assert "AnalysisException" in str(exc.value)
    assert "https://ui/run-1" in str(exc.value)


def test_a_cancelled_job_raises_spark_job_cancelled():
    with pytest.raises(SparkJobCancelled):
        FakeJobBackend([JobState.CANCELLED]).run(make_ctx())


def test_job_logs_are_shown_when_asked_for():
    shown = []
    ctx = make_ctx()
    ctx.logger = lambda msg, job_id=None, stream="stdout": shown.append(msg)
    FakeJobBackend([JobState.SUCCESS]).run(ctx, show_stdout=True)
    assert any("job stdout" in msg and "driver out" in msg for msg in shown)
