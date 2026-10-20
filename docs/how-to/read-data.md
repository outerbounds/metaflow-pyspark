# Read Data

There are three ways to read a Unity Catalog table into a step. All three are governed by Unity Catalog; they differ in what executes the read and what it costs.

| | Spark | Credential vending | SQL warehouse |
| --- | --- | --- | --- |
| Call | `ref.to_spark(self.spark)` in an `@spark` step | `ref.to_arrow()`, `to_pandas()`, `to_polars()`, `to_duckdb()` | `query("SELECT ...")` |
| Where the read runs | Databricks compute | the Metaflow task, reading Delta files directly | a SQL warehouse |
| DBUs | yes | none | yes (SQL DBUs) |
| Cold start | seconds to minutes | none | none to tens of seconds (serverless) |
| Joins across large tables | yes | not usefully | yes |
| Predicate and column pushdown | yes | yes, via Delta and Arrow | yes |
| Views, row filters, column masks | yes | no, they need the query engine | yes |
| Grants | `SELECT` | `SELECT` and `EXTERNAL USE SCHEMA` | `SELECT` |
| Honours a pinned version | yes | yes | only with `VERSION AS OF` in the SQL |
| Install | `[connect]` | `[catalog]` and `duckdb` | `[databricks]` |

As a rule of thumb, use credential vending when the table fits comfortably in the task's memory and no view or row filter is in the way, which covers most feature assembly and evaluation reads. Use a warehouse when a view, a row filter, or a column mask is involved, or when the query needs joins the task cannot do efficiently. Use Spark when the input is large enough that even a warehouse struggles, or the work is distributed beyond one aggregation.

## Pin a table as an artifact

A `UnityCatalogTable` is a reference, not a copy. Assigning one records the table's current Delta version, and every read through it uses that version:

```python
from metaflow import UnityCatalogTable

@step
def start(self):
    self.orders = UnityCatalogTable("main.retail.orders")
    print(self.orders)          # UnityCatalogTable(main.retail.orders @v12)
```

A later step, a resumed run, or a notebook that loads the artifact reads version 12 even after the table has moved on. `at_version(n)`, `at_timestamp(ts)`, and `latest()` return re-pinned references, and `history()` lists the table's versions.

Reading the current version goes through credential vending, so the step that creates the reference needs `deltalake` and the `EXTERNAL USE SCHEMA` grant. Without them the reference is left unpinned and a warning is printed to stderr. Choose the behaviour explicitly when it matters:

```python
UnityCatalogTable("main.retail.orders", pin="required")   # raise instead of warning
UnityCatalogTable("main.retail.orders", version=12)       # pin a known version
UnityCatalogTable("main.retail.orders", pin=False)        # always read the latest
```

Connection settings resolve like everything else in the extension: `DATABRICKS_*` environment variables, then the `databricks` section of `self.spark_config` when you pass `flow=self`, then `config={...}`. Neither vended credentials nor a `token` or `client_secret` from `config` is pickled into the artifact, so a step that reads the reference authenticates from its own environment.

## Through Spark

In an `@spark` step, `to_spark()` reads the pinned version through the session:

```python
@spark(backend="databricks")
@step
def features(self):
    df = self.orders.to_spark(self.spark)
    self.daily = df.groupBy("order_date").sum("amount").toPandas()
```

This is the only path that runs arbitrary DataFrame code on the data where it lives, and the natural one for large tables or join-heavy work. It costs compute for the step's duration.

## Through credential vending

`to_arrow()` asks Unity Catalog to vend a short-lived credential scoped to the table's storage location, and the task reads the Delta files itself with `deltalake`. No compute starts:

```python
@step
def evaluate(self):
    recent = self.orders.to_pandas(columns=["order_date", "amount"])
    con = self.orders.to_duckdb()
    top = con.execute("SELECT order_date, SUM(amount) FROM orders GROUP BY 1").df()
```

`to_arrow()`, `to_pandas()`, and `to_polars()` accept `columns=` and a pyarrow `filters=` expression, which are pushed down to the Parquet scan. `to_duckdb()` registers the table as a DuckDB view named after the table, or `view_name=`.

Vending bypasses the query engine, so it cannot read views or foreign tables, and cannot apply row filters or column masks. When Unity Catalog refuses, the error passes on its reason, names the `EXTERNAL USE SCHEMA` grant, and suggests the equivalent `query()` call. Tables with deletion vectors, which recent Databricks runtimes enable by default for many write patterns, cannot be read by `deltalake`; for those the read falls back to DuckDB's own Delta reader at the pinned version, on AWS and on Azure SAS credentials. Install `duckdb` to get the fallback.

## Through a SQL warehouse

`query()` runs one statement and returns the result:

```python
from metaflow_extensions.spark.plugins.warehouse import query

@step
def daily(self):
    self.revenue = query(
        "SELECT order_date, SUM(amount) AS revenue FROM main.retail.orders "
        "WHERE order_date >= :since GROUP BY order_date",
        params={"since": self.since},
        output_format="polars",
    )
```

`params` bind as named, typed `:name` parameters and are never formatted into the SQL text, which also means they bind values rather than table names. Timezone-aware datetimes keep their offset; decimals keep their precision.

The warehouse is `warehouse_id=`, else `warehouse_id` in the `databricks` section of the flow's `spark_config` (pass `flow=self`), else `METAFLOW_DATABRICKS_WAREHOUSE_ID` or `DATABRICKS_WAREHOUSE_ID`. `output_format` is `pandas` (the default), `arrow`, `polars`, or `none`. Results of any size come back through the same Arrow path. Each statement is tagged with the flow, run, step, task, and user as `query_tags`, which appear in `system.query.history` with `databricks-sdk` 0.86 or newer.

A failed statement raises `QueryFailed` with the warehouse's error class and message, or returns `None` with `crash_on_failure=False`. Interrupting the step, or exceeding `timeout=` minutes, cancels the statement. A statement is not pinned the way a `UnityCatalogTable` is; to reproduce a read, put the version in the SQL:

```python
query(f"SELECT * FROM {self.orders.full_name} VERSION AS OF {self.orders.version}")
```

Inside an `@spark` step, `query()` without `warehouse_id=` runs through the step's session instead, whose compute is already running. The statement and its parameters do not change.
