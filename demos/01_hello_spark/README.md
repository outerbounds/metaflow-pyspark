# Demo 1: the same flow, three places to run it

The opening question on most calls is some version of "is this real Spark, or a lookalike". The answer is a flow whose step body does not change between a laptop, Databricks serverless, and an existing Databricks cluster.

## Local, no account

```bash
pip install -e '../..[local]'      # needs a JVM
python hello_spark.py run
```

`self.spark` is a local `SparkSession`. Useful for CI and for iterating on transformation logic without paying for anything.

## Databricks serverless, through Spark Connect

```bash
pip install -e '../..[connect]'
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com DATABRICKS_TOKEN=dapi...
METAFLOW_SPARK_BACKEND=databricks python hello_spark.py run
```

Same file, same body. `self.spark` is now a session against Databricks compute: the step's Python runs wherever Metaflow put it, and only the query plan crosses the wire. `session.version` prints the Databricks Runtime's Spark version.

`databricks-connect` bundles its own `pyspark` and the two conflict, so use a separate environment from the local backend. The extension detects that case and says so rather than letting the import error point nowhere.

## An existing cluster

```bash
METAFLOW_SPARK_BACKEND=databricks DATABRICKS_CLUSTER_ID=<cluster-id> python hello_spark.py run
```

The cluster's runtime Python must match the task's minor version for Python UDFs; this demo uses none.
