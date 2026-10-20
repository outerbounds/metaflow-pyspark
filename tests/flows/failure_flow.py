"""How a failed remote job surfaces in a flow.

A Spark failure has to arrive as a Metaflow step failure carrying the remote error, not
as a mystery exit code.
"""

from metaflow import FlowSpec, spark, step

from metaflow_extensions.spark.plugins.backends import JobBackend, register_backend
from metaflow_extensions.spark.plugins.context import JobHandle, JobState, JobStatus


class FailingBackend(JobBackend):
    name = "failing"

    def submit(self, ctx):
        return JobHandle(
            backend=self.name,
            job_id="failing-run-1",
            ui_url="https://example.invalid/runs/failing-run-1",
        )

    def poll(self, handle):
        return JobStatus(
            state=JobState.FAILED,
            message="the run failed",
            error_class="DRIVER_ERROR",
        )

    def cancel(self, handle):
        pass

    def fetch_logs(self, handle, stream="stdout"):
        return "AnalysisException: Table or view not found: main.retail.nope"


register_backend("failing", FailingBackend)


class FailureFlow(FlowSpec):
    @spark(backend="failing", job=lambda s: None, crash_on_failure=True)
    @step
    def start(self):
        print("the step body must not run after a failed job")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    FailureFlow()
