"""Chapter 6: put Metaflow in front of a job that already exists in Databricks.

    python create_job.py                       # once, to stand in for an existing job
    python trigger.py run
    python trigger.py run --run-date 2023-01-01  # the check task fails

`@databricks_job` starts the workspace job with `run_date` as a job parameter, waits
for it, and puts each task's state and exit value in `self.databricks_job_result`. The
job writes its summary to a table, and `end` reads it back through a pinned
`UnityCatalogTable`, the same way the other chapters pass tables between steps.
"""

import json

from _env import step_env
from metaflow import FlowSpec, Parameter, UnityCatalogTable, databricks_job, step

JOB_NAME = "metaflow-pyspark-demo-daily-orders"


class TriggerJobFlow(FlowSpec):
    run_date = Parameter("run-date", default="2024-05-17")

    @step_env()
    @step
    def start(self):
        self.next(self.daily)

    @step_env()
    @databricks_job(job_name=JOB_NAME, parameters_from=["run_date"])
    @step
    def daily(self):
        for task, outcome in self.databricks_job_result.items():
            print("%-10s %s" % (task, outcome["state"]))
        self.written = json.loads(self.databricks_job_result["summarize"]["result"])
        self.next(self.end)

    @step_env("vending")
    @step
    def end(self):
        daily = UnityCatalogTable(self.written["table"])
        print("%r holds %d days" % (daily, self.written["days"]))
        print(
            daily.to_pandas().sort_values("order_date").tail(5).to_string(index=False)
        )


if __name__ == "__main__":
    TriggerJobFlow()
