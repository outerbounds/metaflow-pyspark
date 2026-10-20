# metaflow-pyspark

A Metaflow extension for working with Spark and governed Databricks data from ordinary flow steps.

- `@spark` gives a step a live `SparkSession`, either local Spark in the task process or Databricks compute through Databricks Connect. The same step body runs on both.
- `UnityCatalogTable` is an artifact that references a Unity Catalog table at a pinned Delta version. A re-run reads the same snapshot without copying the data.
- `query()` runs one SQL statement on a Databricks SQL warehouse, or on the step's Spark session when there is one, and returns pandas, Arrow, or Polars.

```python
from metaflow import FlowSpec, UnityCatalogTable, spark, step


class OrdersFlow(FlowSpec):

    @step
    def start(self):
        # Pins the table's current Delta version.
        self.orders = UnityCatalogTable("main.retail.orders")
        self.next(self.daily)

    @spark(backend="databricks")
    @step
    def daily(self):
        df = self.orders.to_spark(self.spark)
        self.revenue = df.groupBy("order_date").sum("amount").toPandas()
        self.next(self.end)

    @step
    def end(self):
        print(self.revenue.head())


if __name__ == "__main__":
    OrdersFlow()
```

## Install

Install from a checkout, choosing the extras for the pieces you use:

| Command | For |
| --- | --- |
| `pip install -e '.[local]'` | `@spark` with local Spark (needs a JDK) |
| `pip install -e '.[connect]'` | `@spark` on Databricks through Databricks Connect |
| `pip install -e '.[catalog]' duckdb` | `UnityCatalogTable` reads through credential vending |
| `pip install -e '.[databricks]'` | `query()` on a SQL warehouse |

`databricks-connect` bundles its own copy of `pyspark`, and the two conflict. Install `[local]` and `[connect]` into separate environments; the extension recognizes the clash and names it in the error.

Databricks authentication is delegated to `databricks-sdk`, so anything it understands works: `DATABRICKS_HOST` and `DATABRICKS_TOKEN`, a CLI profile via `DATABRICKS_CONFIG_PROFILE`, OAuth service principals, and Azure managed identities.

## Where to go next

- [Use @spark](how-to/spark-decorator.md): pick a backend, configure it, and give a step a session.
- [Read data](how-to/read-data.md): Spark, credential vending, or a SQL warehouse, and when to use each.
- [Write data](how-to/write-data.md): publish a DataFrame to Unity Catalog from a step and pin what you wrote.
