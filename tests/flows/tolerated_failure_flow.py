"""crash_on_failure=False lets a step handle a failed Spark job itself."""

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


class ToleratedFailureFlow(FlowSpec):
    @spark(
        backend="failing",
        job=lambda s: None,
        crash_on_failure=False,
        output_artifact="result",
    )
    @step
    def start(self):
        # The step runs, and no output was produced for it to read.
        assert self.result is None
        print("tolerated failure ok")
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    ToleratedFailureFlow()
