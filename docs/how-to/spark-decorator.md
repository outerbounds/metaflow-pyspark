# Use @spark

`@spark` gives a step a `SparkSession` for the duration of its body. The session is available as `self.spark` and as `current.spark`, and the extension removes it from the flow before Metaflow persists the step's artifacts, because a session cannot be pickled.

```python
from metaflow import FlowSpec, spark, step


class HelloSpark(FlowSpec):

    @spark
    @step
    def start(self):
        df = self.spark.range(1_000_000).selectExpr("id % 7 AS bucket")
        self.counts = df.groupBy("bucket").count().toPandas()
        self.next(self.end)

    @step
    def end(self):
        print(self.counts)


if __name__ == "__main__":
    HelloSpark()
```

Only the step's own results become artifacts. Collect what the next step needs (with `toPandas()`, or by writing a table as described in [Write data](write-data.md)) before the step ends.

## Choose a backend

=== "Local Spark"

    ```python
    @spark(backend="local")   # the default
    ```

    Starts Spark inside the task process. No account or credentials are needed, which makes it the backend for CI and for iterating on transformation logic. It needs `pyspark` and a JDK; when `JAVA_HOME` is not set, a JDK installed into the Python environment (such as conda's `openjdk`) is found automatically. `master=` defaults to `local[*]`. The session is stopped when the step ends.

=== "Databricks serverless"

    ```python
    @spark(backend="databricks")
    ```

    Connects to Databricks serverless compute through Databricks Connect. The step's Python keeps running wherever Metaflow put it, and only the query plan and results cross the wire. Unity Catalog grants apply to every read as they would in a notebook.

=== "Databricks cluster"

    ```python
    @spark(backend="databricks", cluster_id="0123-456789-abcdefgh")
    ```

    Connects to an existing all-purpose cluster instead. `serverless=True` and `cluster_id` are mutually exclusive.

The `databricks-connect` version has to match the compute it connects to. The demos pin `databricks-connect` 17.3 on Python 3.12 for serverless.

On Databricks, every query in the session is tagged with the step's pathspec and labelled with it in the query history. Interrupting the step interrupts its in-flight queries rather than leaving them running. A Connect session cannot carry billing tags, which belong to the cluster, so its spend is not attributable per step.

## Configure without editing the flow

Every setting can come from the environment or from a flow-level `spark_config` artifact instead of the decorator, which lets one flow file run locally in CI and on Databricks in production. Lowest to highest precedence:

1. The Metaflow config and the environment: `METAFLOW_SPARK_BACKEND`, `DATABRICKS_CLUSTER_ID` (or `METAFLOW_DATABRICKS_CLUSTER_ID`), and the standard `DATABRICKS_*` authentication variables.
2. The `spark_config` artifact: a dict, a JSON string, or an `IncludeFile`.
3. Arguments to `@spark`.

```python
from metaflow import FlowSpec, IncludeFile, spark, step


class ConfiguredFlow(FlowSpec):
    spark_config = IncludeFile("spark_config", default="spark_config.json")

    @spark
    @step
    def start(self):
        ...
```

```json
{
  "backend": "databricks",
  "spark-parameters": {"spark.sql.shuffle.partitions": "64"},
  "databricks": {"cluster_id": "0123-456789-abcdefgh"},
  "local": {"master": "local[4]"}
}
```

`spark-parameters` at the top level apply to every backend, and a backend section can override individual keys. Pass `config="other_artifact"` to read a different artifact, or a dict to configure the step directly.

## Credentials

The Databricks backend authenticates through `databricks-sdk`, so the task needs the same credentials the SDK would: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, a profile in `~/.databrickscfg` selected with `DATABRICKS_CONFIG_PROFILE`, or OAuth via `DATABRICKS_CLIENT_ID` and `DATABRICKS_CLIENT_SECRET`. `host=`, `token=`, and `profile=` on the decorator override them.

On Outerbounds, store the host and token in an integration and attach it to the step with `@secrets`, which exports its keys as environment variables when the task starts:

```python
@secrets(sources=["outerbounds.databricks"])
@spark(backend="databricks")
@step
def features(self):
    ...
```

## Keep Spark code in its own module

Instead of using the session in the step body, `job=` names a function that takes the session and returns a DataFrame. The extension calls it with the session and any `job_parameters` read off the flow, and stores the result in `self.spark_df` before the step body runs:

```python
# etl.py
def summarize(spark, cutoff):
    return spark.read.table("main.retail.orders").where(f"amount > {cutoff}")
```

```python
import etl

@spark(
    backend="databricks",
    job=etl.summarize,
    job_parameters=["cutoff"],
    output_format="arrow",
)
@step
def crunch(self):
    print(self.spark_df.num_rows)
```

`output_format` is `pandas` (the default), `arrow`, `polars`, or `none`, and `output_artifact=` renames `spark_df`. The function is ordinary importable code, which makes it straightforward to unit-test against a local session.

## Artifacts from Spark Connect

`toPandas()` on a Databricks Connect DataFrame attaches pyspark objects to the result's `DataFrame.attrs`. The extension strips them from every artifact a `@spark` step produces, so steps that read the artifact do not need pyspark installed.
