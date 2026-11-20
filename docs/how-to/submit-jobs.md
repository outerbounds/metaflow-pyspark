# Submit Databricks jobs

A Databricks Connect session keeps the Spark driver in the Metaflow task and only gets what the compute it connects to offers. A pinned Databricks Runtime, Photon, cluster-scoped libraries, and JVM UDFs all need the driver on the cluster, which is what `mode="job"` is for. The extension packages a `job=` function, submits it as a one-off run through the Jobs API, and waits for it.

```python
# jobs/daily.py
def summarize(spark, table_name):
    return spark.sql(
        f"SELECT order_date, COUNT(*) AS orders FROM {table_name} GROUP BY order_date"
    )
```

```python
from jobs import daily

from metaflow import FlowSpec, spark, step


class DailyJob(FlowSpec):

    @step
    def start(self):
        self.table_name = "main.retail.orders"
        self.next(self.summarize)

    @spark(
        backend="databricks",
        mode="job",
        job=daily.summarize,
        job_parameters=["table_name"],
        volume="/Volumes/main/metaflow/staging",
    )
    @step
    def summarize(self):
        print(self.spark_df.tail())
        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    DailyJob()
```

The step body runs after the job has finished, with its result in `self.spark_df`. The task needs the `[databricks]` extra, plus `[catalog]` when the result is a table. It needs neither `pyspark` nor `databricks-connect`, unless the job module imports `pyspark` at the top: the flow file imports that module in every step. The job module itself runs on the cluster and must not import `metaflow`.

`mode` and the Volume can also come from the environment, `METAFLOW_SPARK_MODE=job` and `METAFLOW_DATABRICKS_VOLUME`, or from the `databricks` section of a `spark_config` artifact, as described in [Use @spark](spark-decorator.md#configure-without-editing-the-flow).

## What gets shipped

The extension ships the whole top-level package that defines the job function, all of `jobs/` in the example, and the job can import anything inside it. Other code has to be named:

- `include=["lookups.json", "../shared"]` adds files or directories, each at its basename. An included directory is importable as a package.
- `packages=["_orders"]` adds an importable module or package by name.

`job_parameters` names flow attributes to pass to the function as keyword arguments. They are pickled and have to unpickle on the cluster, which is safest with plain built-in types. When the cluster's Python minor version differs from the task's and unpickling fails, the error says so.

The code package and parameters are staged under `<volume>/metaflow/<pathspec>/`, which needs `READ VOLUME` and `WRITE VOLUME` on the Volume. Serverless compute can't run a Python file from a Volume, so the driver script goes in the workspace under `/Users/<you>/metaflow/<pathspec>/`. Set `workspace_dir=` to use another folder, such as one under `/Shared` for a service principal. Both are deleted once the run finishes, except the result marker the driver writes next to the code.

## Choose compute

=== "Serverless"

    ```python
    @spark(backend="databricks", mode="job", job=..., volume=...)
    ```

    The default when no compute is configured. Add PyPI packages with `dependencies=["scikit-learn==1.6.1"]`; serverless takes no cluster libraries.

=== "New job cluster"

    ```python
    @spark(
        backend="databricks", mode="job", job=..., volume=...,
        runtime_version="15.4.x-scala2.12",
        photon=True,
        num_workers=4,
        node_type_id="Standard_D8ds_v5",
    )
    ```

    Creates a cluster for the run and terminates it afterwards. Any of `runtime_version`, `num_workers`, or `node_type_id` selects this; the defaults are the 15.4 LTS runtime, two workers, and a general-purpose node type for the workspace's cloud. `libraries=[{"pypi": {"package": "..."}}]` installs cluster libraries, and `compute={...}` passes any other `new_cluster` field through. The cluster runs in single-user access mode so it can use Unity Catalog, and writes its driver logs to the staging directory.

=== "Instance pool"

    ```python
    @spark(backend="databricks", mode="job", job=..., volume=..., instance_pool_id="0101-pool")
    ```

    A new job cluster drawn from a warm pool, which avoids most of the start-up time. The pool fixes the node type.

=== "Existing cluster"

    ```python
    @spark(backend="databricks", mode="job", job=..., volume=..., cluster_id="0123-456789-abcdefgh")
    ```

    Runs on an all-purpose cluster that is already up. Its runtime decides the Python version.

## Get the result back

The driver writes the DataFrame the function returns to Parquet under the staging directory, or to a Unity Catalog table with `output_table=`. `write_mode=` (default `"overwrite"`) and `partition_by=[...]` control the write. The step then receives it according to `output_format`:

| `output_format` | `self.spark_df` |
| --- | --- |
| `pandas` (default), `arrow`, `polars` | the result, read into the task |
| `table` | a `UnityCatalogTable` for `output_table`, pinned to the version the job wrote |
| `url` | `output_table`, or the Parquet path |
| `none` | `None`; the job's own writes are the result |

A pinned table reference is usually the better choice for anything large, since the next step can read it through Spark, credential vending, or a warehouse, as [Read data](read-data.md) describes, and the datastore never holds a copy. `output_artifact=` renames `spark_df`, and `save_output=False` drops it before the step's artifacts are saved.

## Failures, timeouts, and cancellation

A failed run raises `SparkJobFailed` in the step, before the step body runs. The message has Databricks' termination reason, a link to the run, and the Python traceback from the driver, which shows the line in your job function and Spark's error message:

```
Spark job failed with state 'FAILED'.
Error class: RUN_EXECUTION_ERROR
Run details: https://adb-....azuredatabricks.net/?o=...#job/.../run/...

Last 30 lines of output:
Traceback (most recent call last):
  ...
  File "/tmp/metaflow-spark-.../code/jobs/daily.py", line 2, in summarize
    return spark.sql("SELECT * FROM main.retail.does_not_exist")
  ...
pyspark.errors.exceptions.connect.AnalysisException: [TABLE_OR_VIEW_NOT_FOUND] ...
```

With `crash_on_failure=False` the step body runs anyway and `self.spark_df` is `None`.

`timeout=` (minutes, default 60) is passed to Databricks as the run's timeout. Interrupting the step, or Metaflow killing the task, cancels the run. If the Jobs API stops answering while the run is in flight, the step raises `ControlPlaneError` and leaves the run alone, since it may still be healthy.

The driver's output is printed in the task log after a successful run; `show_stdout=False` turns that off. The run id, run link, and Spark UI link are recorded as task metadata and stay available from the Metaflow client.

## Attribute cost

A new job cluster, from an instance pool or not, carries `custom_tags` naming the flow, run, step, task, attempt, and user, plus anything passed in `tags={...}`. They land in `system.billing.usage`, where DBUs can be grouped per step:

```sql
SELECT custom_tags['metaflow_flow'] AS flow,
       custom_tags['metaflow_step'] AS step,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE custom_tags['metaflow_run_id'] = :run_id
GROUP BY ALL
```

Serverless job runs and existing clusters don't accept per-run tags, and their usage can't be attributed this way.
