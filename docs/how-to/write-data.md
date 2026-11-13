# Write Data

Most writes go through Spark or a SQL warehouse, so Databricks commits them and Unity Catalog records their lineage. External tables can also be written from the task through vended credentials. In an `@spark` step on Databricks, `self.spark` can turn data held by the task into a DataFrame and save it as a Unity Catalog table, under the step's own grants.

## Publish a DataFrame from a step

```python
from metaflow import FlowSpec, UnityCatalogTable, spark, step


class PublishFlow(FlowSpec):

    @step
    def start(self):
        import pandas as pd

        self.features_df = pd.DataFrame(
            {"customer_id": [1, 2, 3], "score": [0.2, 0.9, 0.4]}
        )
        self.next(self.publish)

    @spark(backend="databricks")
    @step
    def publish(self):
        (
            self.spark.createDataFrame(self.features_df)
            .write.mode("overwrite")
            .saveAsTable("main.ml.customer_scores")
        )
        self.scores = UnityCatalogTable("main.ml.customer_scores", pin="required")
        self.next(self.end)

    @step
    def end(self):
        print(self.scores)      # UnityCatalogTable(main.ml.customer_scores @v3)


if __name__ == "__main__":
    PublishFlow()
```

`createDataFrame` accepts a pandas DataFrame, and Databricks Connect sends it to Databricks, where the write runs. The step needs `CREATE TABLE` on the schema for a new table, or `MODIFY` on an existing one.

The last line of `publish` is what makes the write reproducible. Pinning right after the write records the version this run produced. Downstream steps, re-runs, and anyone inspecting the run later read exactly that snapshot, even after the next run overwrites the table. `pin="required"` fails the step if the version cannot be read rather than silently storing an unpinned reference. Pinning goes through credential vending, and the step also needs `deltalake` installed and the `EXTERNAL USE SCHEMA` grant.

Any `DataFrameWriter` option works as usual, for example `mode("append")`, `partitionBy("region")`, or `option("overwriteSchema", "true")`.

## Transform where the data lives

When the input is already in Databricks, read it, transform it, and write it without collecting anything into the task:

```python
@spark(backend="databricks")
@step
def aggregate(self):
    from pyspark.sql import functions as F

    daily = (
        self.orders.to_spark(self.spark)
        .groupBy("order_date")
        .agg(F.sum("amount").alias("revenue"))
    )
    daily.write.mode("overwrite").saveAsTable("main.retail.orders_daily")
    self.daily = UnityCatalogTable("main.retail.orders_daily", pin="required")
```

Reading through `self.orders` rather than the table name computes the aggregate from the version the run pinned, which makes the output table reproducible from the run too.

The same kind of write can also be a single statement on a SQL warehouse, with no Spark session:

```python
from metaflow_extensions.spark.plugins.warehouse import query

query(
    f"CREATE OR REPLACE TABLE main.retail.orders_daily AS "
    f"SELECT order_date, SUM(amount) AS revenue "
    f"FROM {self.orders.full_name} VERSION AS OF {self.orders.version} "
    f"GROUP BY order_date",
    output_format="none",
)
```

Statement parameters bind single values, and a warehouse cannot receive data held by the task in bulk. This only suits data that is already in Databricks.

## Write an external table without Databricks compute

`write()` commits Arrow or pandas data to the table's Delta files with `deltalake`, using `READ_WRITE` credentials vended by Unity Catalog, and returns a reference pinned to the new version:

```python
self.daily = UnityCatalogTable("main.ext.orders_daily").write(arrow_table, mode="append")
```

This needs `MODIFY` and `EXTERNAL USE SCHEMA`, and only works for external tables: Databricks has to commit writes to a managed table, so `write()` refuses one before touching any files. The engine never sees the write, so Unity Catalog records no lineage for it and does not evaluate constraints or generated columns.

## Label what a run wrote

Unity Catalog records table lineage for engine writes on its own. Tagging the table with the pathspec connects it back to the exact Metaflow task, which Unity Catalog cannot infer:

```python
from metaflow import current

self.daily.stamp_lineage(current)   # metaflow_pathspec, metaflow_flow, metaflow_run_id
```

`stamp_lineage` runs `ALTER TABLE ... SET TAGS` through `query()`, so inside an `@spark` step it reuses the session and elsewhere it needs a warehouse.

## Things that do not write to Databricks

With `backend="local"`, `saveAsTable` writes to a temporary warehouse directory inside the task, which is removed when the step ends unless `spark.sql.warehouse.dir` is set in `spark_parameters`. Use local Spark to test transformation logic, and a Databricks backend to publish.

`UnityCatalogTable.write()` does not write managed tables, since writing their Delta files directly would bypass Unity Catalog's commit coordination.
