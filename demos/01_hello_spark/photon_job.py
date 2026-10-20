"""Demo 1b: when Connect is not enough.

Spark Connect gives you a session against Databricks Spark, but the driver runs in the
Metaflow task. If you need a pinned Databricks Runtime, Photon, cluster-scoped libraries,
or JVM UDFs, the work has to run on the cluster. That is `mode="job"`: the Spark code is
packaged, staged on a Unity Catalog Volume, and submitted through the Jobs API.

    export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
    python photon_job.py run

The job function receives a session and returns a DataFrame. `jobs/etl.py` imports from
`jobs/helpers.py`, which is the multi-file case the packaging layer exists for: the whole
`jobs/` package ships, not just the one file naming the entry point.
"""

from _env import step_env
from jobs import etl

from metaflow import FlowSpec, Parameter, spark, step


class PhotonJobFlow(FlowSpec):
    cutoff = Parameter("cutoff", default=100.0)

    @step_env("pyspark")
    @step
    def start(self):
        self.cutoff_value = self.cutoff
        self.next(self.crunch)

    @step_env("pyspark")
    @spark(
        backend="databricks",
        mode="job",
        job=etl.summarize,
        job_parameters=["cutoff_value"],
        runtime_version="15.4.x-scala2.12",
        photon=True,
        num_workers=4,
        output_format="pandas",
        tags={"cost_center": "ml-platform"},
    )
    @step
    def crunch(self):
        print(self.spark_df.to_string(index=False))
        self.next(self.end)

    @step_env("pyspark")
    @step
    def end(self):
        pass


if __name__ == "__main__":
    PhotonJobFlow()
