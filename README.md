
# EXPERIMENTAL Spark and Databricks decorators for Metaflow

## Spark sessions, submitted jobs, and governed Databricks data

Documentation: https://outerbounds.github.io/metaflow-pyspark/

```bash
pip install -e '.[local]'              # @spark with a local Spark session, needs a JVM
pip install -e '.[connect]'            # @spark on Databricks through Spark Connect
pip install -e '.[databricks]'         # Databricks jobs and SQL warehouse statements
pip install -e '.[emr]'                # @spark / @pyspark on EMR Serverless
pip install -e '.[catalog]' duckdb     # Unity Catalog tables via credential vending
```

`databricks-connect` bundles its own `pyspark`, so install `[local]` and `[connect]` into separate environments. The extension detects the clash and names it in the error.

Authentication is delegated to `databricks-sdk`, so PATs, CLI profiles, OAuth service principals, and Azure MSI work as they already do (`DATABRICKS_HOST` and `DATABRICKS_TOKEN`, or `DATABRICKS_CONFIG_PROFILE`).

### `@spark`: a live session in the step

```python
from metaflow import FlowSpec, spark, step

class MyFlow(FlowSpec):

    @spark(backend="databricks")
    @step
    def features(self):
        df = self.spark.read.table("main.retail.orders")
        self.daily = df.groupBy("order_date").count().toPandas()
        self.next(self.end)
```

`self.spark` (also `current.spark`) is a `SparkSession` for the duration of the step body, and is detached before Metaflow persists artifacts because a session is not picklable. `backend="local"`, the default, starts Spark in the task process. `backend="databricks"` connects through Spark Connect to serverless compute, or to `cluster_id=...`; the driver logic stays in the task and only the query plan crosses the wire. Interrupting the step interrupts its in-flight queries.

The backend can also come from `METAFLOW_SPARK_BACKEND`, `DATABRICKS_CLUSTER_ID`, or the flow's `spark_config` artifact, so the same flow file runs locally and on Databricks without edits. Precedence, lowest to highest: the Metaflow config and environment, the `spark_config` artifact (a dict, JSON string, or `IncludeFile` with a top-level `backend`, backend-neutral `spark-parameters`, and `databricks`/`local` sections), then decorator attributes.

`job=` runs a function taking `(spark, **job_parameters)` and stores the DataFrame it returns in `self.spark_df` as `pandas`, `arrow`, `polars`, or `none` (`output_format=`). Spark code can then live in its own testable module:

```python
@spark(backend="databricks", job=etl.summarize, job_parameters=["cutoff"])
@step
def crunch(self):
    print(self.spark_df.head())
```

Connect sessions are labelled with the step's pathspec in the Databricks query history, but cannot carry billing tags, which belong to the cluster rather than the session.

### Submitted jobs

When the work has to run on the cluster (a pinned Databricks Runtime, Photon, cluster-scoped libraries, JVM UDFs), `job=` names a function taking `(spark, **job_parameters)` that is packaged and submitted instead:

```python
@spark(
    backend="databricks",
    mode="job",
    job=etl.summarize,
    job_parameters=["cutoff"],
    volume="/Volumes/main/metaflow/staging",
    runtime_version="15.4.x-scala2.12",
    photon=True,
    output_format="pandas",
)
@step
def crunch(self):
    print(self.spark_df.head())
```

The function's whole top-level package ships, plus anything named in `include=`, so a job can span several modules; `job_parameters` are pickled across, and the job must not import metaflow. On Databricks the package is staged on a Unity Catalog Volume and run through the Jobs API on serverless compute, an existing cluster (`cluster_id=`), an instance pool, or a new cluster. `backend="emr-serverless"` stages on S3 and runs on EMR Serverless (`application_id=`, `execution_role=`).

The shared wait loop cancels the remote run on interrupt, `SIGTERM`, or timeout, retries control-plane outages without reporting them as job failures, and raises `SparkJobFailed` with the driver's traceback. Run and Spark UI links are logged and recorded as task metadata. `output_format` can also be `table` (with `output_table=`, returning a `UnityCatalogTable`) or `url`, to pass the next step a reference instead of the data. Submitted jobs carry the pathspec as `custom_tags`, which land in `system.billing.usage`; `DATABRICKS_USAGE_QUERY` in `plugins/cost.py` attributes spend per flow, run, and step.

`@pyspark`, the original EMR Serverless decorator, is now an alias of `@spark` with its original defaults (`pyspark_df`, pandas output, `output_pandas`/`output_pyarrow`, `user_timeout`, and a flat `spark_config` with `application-id` and `execution-role`). See `example/sparkflow.py`.

### Existing Databricks jobs and notebooks

```python
@databricks_job(job_name="nightly-features", parameters={"date": "2026-08-24"})
@step
def features(self):
    print(self.databricks_result)          # each task's exit value and state

@databricks_notebook(notebook_path="/Repos/team/etl/clean", parameters_from=["date"])
@step
def clean(self):
    print(self.notebook_result)            # whatever the notebook passed to dbutils.notebook.exit()
```

Both trigger work that already exists in the workspace and wait for it, with the same cancellation, error reporting, tags, and metadata as `@spark` jobs.

### Unity Catalog tables as artifacts

```python
from metaflow import UnityCatalogTable

self.orders = UnityCatalogTable("main.retail.orders")   # pins the current Delta version
```

The artifact is a reference, not a copy. Assignment records the table's current Delta version, and every read through the reference uses that version, so a re-run reads the same bytes even after the table has moved on. `at_version()`, `at_timestamp()`, and `latest()` return re-pinned references, and `history()` lists the versions.

Reading the current version goes through credential vending. The step that creates the reference therefore needs `deltalake` and the `EXTERNAL USE SCHEMA` grant. Without them the reference is left unpinned with a warning on stderr. Pass `pin="required"` to raise instead, or `version=N` to pin explicitly.

`to_arrow()`, `to_pandas()`, `to_polars()`, and `to_duckdb()` ask Unity Catalog to vend temporary, table-scoped credentials and read the Delta files directly with `deltalake`. UC checks the grants and issues the credentials. Vending needs `EXTERNAL USE SCHEMA` in addition to `SELECT`, and the error says so when it is missing, pointing at `query()` as the alternative. Tables with deletion vectors, which `deltalake` cannot read, fall back to DuckDB's Delta reader on AWS and Azure. `to_spark(self.spark)` reads the pinned version through Spark in an `@spark` step.

Connection settings resolve the same way as for `query()` below, with `config={...}` as the explicit layer and `flow=self` adding the flow's `spark_config`. Neither vended credentials nor a `token` or `client_secret` from `config` is pickled into the artifact; a step that reads the reference authenticates from its own environment.

### SQL warehouse statements

```python
from metaflow_extensions.spark.plugins.warehouse import query

self.daily = query(
    "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
    "WHERE order_date >= :since GROUP BY order_date",
    params={"since": self.since},
    output_format="polars",
)
```

`query()` submits one statement to a Databricks SQL warehouse and returns the result as `pandas`, `arrow`, `polars`, or `none`. `params` bind as named, typed parameters through `:name` markers and are never formatted into the SQL text. Unlike credential vending, a statement goes through the query engine, so views, row filters, and column masks apply. An arbitrary statement is not pinned the way a `UnityCatalogTable` is; add `VERSION AS OF` yourself when that matters.

Inside an `@spark` step without an explicit `warehouse_id=`, `query()` runs the statement through the step's session, whose compute is already running. Otherwise the warehouse is `warehouse_id=`, else `warehouse_id` in the `databricks` section of a flow-level config artifact (`query(..., flow=self)` reads `self.spark_config`), else `METAFLOW_DATABRICKS_WAREHOUSE_ID` or `DATABRICKS_WAREHOUSE_ID`. Polling shares one wait loop that cancels the statement on interrupt or timeout and reports a control-plane outage as `ControlPlaneError` rather than as a failed statement (`QueryFailed`). Each statement carries the flow, run, step, task, and user as `query_tags`, which land in `system.query.history` (needs `databricks-sdk>=0.86`; older SDKs run without the tags).

### Demos

[`demos/01_hello_spark`](demos/01_hello_spark) runs one `@spark` step locally, on serverless, and on a cluster, switched by environment variable, plus a Photon job on a pinned runtime. [`demos/02_unity_catalog`](demos/02_unity_catalog) pins a table, mutates it, and replays the original version. [`demos/03_no_cluster`](demos/03_no_cluster) reads the same table through Spark, vending, and a warehouse, side by side. [`demos/04_existing_jobs`](demos/04_existing_jobs) orchestrates existing jobs and a notebook, and ports one to a step. [`demos/06_cost_attribution`](demos/06_cost_attribution) tags jobs and attributes their spend. See [`demos/README.md`](demos/README.md) for setup.

### Tests

```bash
pip install -e '.[databricks,catalog]' duckdb polars pytest
python -m pytest tests -q
```

No cloud account needed. The real local Spark test runs when `pyspark` and a JDK are installed, and is skipped otherwise.
