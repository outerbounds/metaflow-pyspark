"""End-to-end exercise of @databricks_job, with a stand-in for the Jobs API.

Covers what only appears once a real task runs: parameters read off the flow, the result
artifact being set before the step body runs, task metadata being registered, and a
failed run under crash_on_failure=False.
"""

from metaflow import FlowSpec, Task, current, databricks_job, step

from metaflow_extensions.spark.plugins.backends.databricks import runs


class FakeClient:
    host = "https://example.invalid"
    run_now = []

    def __init__(self, config):
        self.config = config

    def api(self, method, path, body=None, query=None):
        if path.endswith("/jobs/get"):
            return {"settings": {"parameters": [{"name": "run_date"}]}}
        if path.endswith("/run-now"):
            FakeClient.run_now.append(body)
            return {"run_id": body["job_id"] * 10}
        failing = str(query["run_id"]).startswith("60")
        if path.endswith("/runs/get-output"):
            if failing:
                return {"error": "ValueError: bad run_date"}
            return {"notebook_output": {"result": "ok"}}
        state = {"life_cycle_state": "TERMINATED"}
        state["result_state"] = "FAILED" if failing else "SUCCESS"
        return {
            "run_page_url": "https://example.invalid/run/%s" % query["run_id"],
            "state": state,
            "tasks": [{"task_key": "main", "run_id": query["run_id"], "state": state}],
        }

    def run_url(self, run_id):
        return None


runs.DatabricksClient = FakeClient


class DatabricksJobFlow(FlowSpec):
    @step
    def start(self):
        self.run_date = "2024-05-17"
        self.next(self.enrich)

    @databricks_job(job_id=5, parameters_from=["run_date"])
    @step
    def enrich(self):
        assert self.databricks_job_result == {
            "main": {"state": "SUCCESS", "run_id": "50", "result": "ok"}
        }
        assert FakeClient.run_now[-1]["job_parameters"] == {"run_date": "2024-05-17"}
        self.next(self.tolerate)

    @databricks_job(job_id=6, crash_on_failure=False, output_artifact="outcome")
    @step
    def tolerate(self):
        assert self.outcome["main"]["state"] == "FAILED"
        assert self.outcome["main"]["error"] == "ValueError: bad run_date"
        self.next(self.end)

    @step
    def end(self):
        task = Task("%s/%s/enrich/2" % (current.flow_name, current.run_id))
        assert task.metadata_dict["databricks-run-id"] == "50"
        assert task.metadata_dict["databricks-job-id"] == "5"
        assert task.metadata_dict["databricks-run-url"].endswith("/run/50")
        print("databricks job flow ok")


if __name__ == "__main__":
    DatabricksJobFlow()
