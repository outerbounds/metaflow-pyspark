"""Chapter 6: the same summary, submitted as a Databricks job.

    export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
    python daily_job.py run

`summarize` runs no Spark in the task. `@spark(mode="job")` packages `jobs/` and
`_orders.py`, stages them on the Volume, and submits `jobs.daily.summarize` as a one-off
run through the Jobs API. The driver on Databricks writes the returned DataFrame to
`output_table`, and the step gets back a `UnityCatalogTable` pinned to that write.

Compute is serverless unless the decorator or the environment says otherwise. A job
cluster on a pinned runtime, for example:

    METAFLOW_DATABRICKS_RUNTIME_VERSION=15.4.x-scala2.12 python daily_job.py run
"""

from _env import step_env
from _orders import ORDERS, ORDERS_DAILY
from jobs import daily
from metaflow import FlowSpec, Parameter, UnityCatalogTable, spark, step

TARGET = ORDERS_DAILY + "_job"


class DailyJobFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)

    @step_env("vending")
    @step
    def start(self):
        orders = UnityCatalogTable(self.table, pin="required")
        self.orders = orders
        # Plain values, because job_parameters are pickled for the cluster.
        self.table_name = orders.full_name
        self.version = orders.version
        print("summarizing %r" % orders)
        self.next(self.summarize)

    @step_env("vending")
    @spark(
        backend="databricks",
        mode="job",
        job=daily.summarize,
        job_parameters=["table_name", "version"],
        packages=["_orders"],
        output_table=TARGET,
        output_format="table",
    )
    @step
    def summarize(self):
        self.daily = self.spark_df
        print("wrote %r" % self.daily)
        self.next(self.end)

    @step_env("vending")
    @step
    def end(self):
        print(self.daily.to_pandas().tail(5).to_string(index=False))


if __name__ == "__main__":
    DailyJobFlow()
