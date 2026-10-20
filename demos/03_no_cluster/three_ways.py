"""Demo 3: the same governed table, read three ways.

    python three_ways.py run --table main.retail.orders --warehouse-id <id>

Reads `main.retail.orders`, the table demo 2 creates and mutates. If it does not exist
yet, seed it first: see "Seed data" in `../02_unity_catalog/README.md`.

All three branches read the same Unity Catalog table, and all are governed by UC. The
difference is what runs the read:

*Through Spark* (`through_spark`): the query executes on Databricks compute through
Spark Connect, at the pinned version. The right answer for large tables and joins.

*Through credential vending* (`through_vending`): UC hands out a short-lived,
table-scoped cloud credential and the Metaflow task reads the Delta files itself, at the
version pinned in `start`, then aggregates in DuckDB. No compute starts, so there are no
DBUs and no cold start. The right answer for mid-size reads with no engine-enforced
constructs in the way.

*Through a SQL warehouse* (`through_warehouse`): the same aggregate, submitted as a
statement to a warehouse via `query()`. Like vending, no Spark session is involved; unlike vending, the statement goes through the query engine, so views, row
filters, and column masks are respected. Needs `--warehouse-id`, or
`DATABRICKS_WAREHOUSE_ID` in the environment; the branch is skipped with an explanation
if neither is set, the same way the vending branch is skipped on a refused grant.
"""

import time

from _env import step_env

from metaflow import (
    FlowSpec,
    Parameter,
    UnityCatalogError,
    UnityCatalogTable,
    spark,
    step,
)
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError
from metaflow_extensions.spark.plugins.warehouse import query


class ThreeWaysFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        self.orders = UnityCatalogTable(self.table)
        print("reading %r" % self.orders)
        self.next(self.through_vending, self.through_spark, self.through_warehouse)

    @step_env("vending")
    @step
    def through_vending(self):
        """UC vends credentials, delta-rs reads, DuckDB aggregates."""
        started = time.monotonic()
        try:
            connection = self.orders.to_duckdb(view_name="orders")
        except UnityCatalogError as exc:
            # EXTERNAL USE SCHEMA missing, a view/row filter/column mask, or deletion
            # vectors on GCP. The message names which; the warehouse branch still runs.
            print("credential vending refused:\n%s" % exc)
            self.vended = None
            self.next(self.compare)
            return

        self.vended = connection.execute(
            """
            SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
            FROM orders GROUP BY order_date ORDER BY order_date
            """
        ).df()
        self.vended_seconds = time.monotonic() - started
        print(
            "vended read in %.1fs, %d groups" % (self.vended_seconds, len(self.vended))
        )
        self.next(self.compare)

    @step_env("connect")
    @spark(backend="databricks")
    @step
    def through_spark(self):
        """The same aggregate, executed on Databricks compute."""
        from pyspark.sql import functions as F

        started = time.monotonic()
        self.spark_result = (
            self.orders.to_spark(self.spark)
            .groupBy("order_date")
            .agg(F.count("*").alias("orders"), F.sum("amount").alias("revenue"))
            .orderBy("order_date")
            .toPandas()
        )
        self.spark_seconds = time.monotonic() - started
        print("spark read in %.1fs" % self.spark_seconds)
        self.next(self.compare)

    @step_env()
    @step
    def through_warehouse(self):
        """The same aggregate, submitted as a statement to a SQL warehouse.

        The table name is part of the statement text rather than a bound parameter,
        because parameter markers bind values, not identifiers. `self.table` is an
        operator-supplied flow Parameter, the same trust boundary `UnityCatalogTable`
        relies on for the same string.
        """
        started = time.monotonic()
        try:
            self.warehouse_result = query(
                """
                SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
                FROM {table}
                GROUP BY order_date ORDER BY order_date
                """.format(table=self.table),
                warehouse_id=self.warehouse_id,
                output_format="pandas",
            )
        except SparkConfigError as exc:
            print("SQL warehouse branch skipped:\n%s" % exc)
            self.warehouse_result = None
            self.next(self.compare)
            return
        self.warehouse_seconds = time.monotonic() - started
        print(
            "warehouse read in %.1fs, %d groups"
            % (self.warehouse_seconds, len(self.warehouse_result))
        )
        self.next(self.compare)

    @step_env()
    @step
    def compare(self, inputs):
        self.merge_artifacts(inputs, include=["orders"])
        rows = []
        for i in inputs:
            if getattr(i, "vended", None) is not None:
                rows.append(("credential vending", i.vended_seconds, len(i.vended)))
            if getattr(i, "spark_result", None) is not None:
                rows.append(("databricks spark", i.spark_seconds, len(i.spark_result)))
            if getattr(i, "warehouse_result", None) is not None:
                rows.append(
                    ("sql warehouse", i.warehouse_seconds, len(i.warehouse_result))
                )

        print("\n%-22s %10s %10s" % ("path", "seconds", "groups"))
        for label, taken, groups in rows:
            print("%-22s %10.1f %10d" % (label, taken, groups))
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        pass


if __name__ == "__main__":
    ThreeWaysFlow()
