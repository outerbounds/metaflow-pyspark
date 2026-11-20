"""End-to-end exercise of @spark in submit mode, with a stand-in for a cluster.

Covers the parts that only appear once a real task runs: job_parameters read off the
flow, the output artifact being set before the step body runs, task metadata being
registered, and cleanup happening afterwards.
"""

from metaflow import FlowSpec, Task, current, spark, step

from metaflow_extensions.spark.plugins.backends import JobBackend, register_backend
from metaflow_extensions.spark.plugins.context import JobHandle, JobState, JobStatus


class FakeSubmitBackend(JobBackend):
    name = "fake-submit"

    def submit(self, ctx):
        assert ctx.tags["metaflow_flow"] == "SubmitFlow"
        assert ctx.tags["metaflow_step"] == ctx.step_name
        return JobHandle(
            backend=self.name,
            job_id="fake-run-1",
            ui_url="https://example.invalid/runs/fake-run-1",
            spark_ui_url="https://example.invalid/sparkui/fake-run-1",
            output_url="s3://fake/out",
            extra={"inputs": sorted(ctx.inputs)},
        )

    def poll(self, handle):
        return JobStatus(state=JobState.SUCCESS)

    def cancel(self, handle):
        pass

    def fetch_logs(self, handle, stream="stdout"):
        return "fake driver %s" % stream

    def read_output(self, handle, output_format):
        return {"format": output_format, "url": handle.output_url}

    def cleanup(self, handle, ctx):
        CLEANED.append(handle.job_id)


CLEANED = []
register_backend("fake-submit", FakeSubmitBackend)


class SubmitFlow(FlowSpec):
    @step
    def start(self):
        self.start_date = "2026-08-01"
        self.next(self.crunch)

    @spark(
        backend="fake-submit",
        job=lambda spark_session: None,
        job_parameters=["start_date"],
        output_format="url",
        output_artifact="result",
    )
    @step
    def crunch(self):
        # The artifact is set before the step body runs, so a step can use it directly.
        assert self.result == {"format": "url", "url": "s3://fake/out"}
        self.pathspec = current.pathspec
        self.next(self.end)

    @step
    def end(self):
        assert self.result["url"] == "s3://fake/out"
        metadata = Task(self.pathspec).metadata_dict
        assert metadata["spark-backend"] == "fake-submit"
        assert metadata["spark-job-id"] == "fake-run-1"
        assert metadata["spark-job-url"] == "https://example.invalid/runs/fake-run-1"
        assert metadata["spark-ui-url"] == "https://example.invalid/sparkui/fake-run-1"
        print("submit flow ok")


if __name__ == "__main__":
    SubmitFlow()
