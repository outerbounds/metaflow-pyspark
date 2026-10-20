"""Demo 1: the same flow, three places to run it.

    # local Spark, no account, no credentials
    python hello_spark.py run

    # Databricks serverless via Spark Connect
    METAFLOW_SPARK_BACKEND=databricks python hello_spark.py run

    # the same, on an existing cluster
    METAFLOW_SPARK_BACKEND=databricks DATABRICKS_CLUSTER_ID=<id> python hello_spark.py run

The step body does not change between them, so "is this real Spark or a lookalike" has an
answer you can run rather than argue about. Each environment variable can instead be a
decorator attribute or a key in the flow's `spark_config` artifact.
"""

from _env import spark_backend_kind, step_env

from metaflow import FlowSpec, Parameter, spark, step


class HelloSparkFlow(FlowSpec):
    rows = Parameter("rows", default=1_000_000)

    @step_env()
    @step
    def start(self):
        self.next(self.crunch)

    @step_env(spark_backend_kind())
    @spark
    @step
    def crunch(self):
        """A step that uses Spark directly. No packaging, no serialization boundary."""
        session = self.spark
        print("Spark %s" % session.version)

        df = session.range(self.rows).selectExpr(
            "id", "id % 7 AS bucket", "rand(42) AS value"
        )
        summary = (
            df.groupBy("bucket").agg({"value": "avg", "id": "count"}).orderBy("bucket")
        )
        # Aggregated results are small, so collecting them into the task is the right
        # call here. For anything large, write a table and pass a UnityCatalogTable.
        self.summary = summary.toPandas()
        self.spark_version = session.version
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(self.summary.to_string(index=False))
        print("\nran on Spark %s" % self.spark_version)


if __name__ == "__main__":
    HelloSparkFlow()
