"""Chapter 0: create the table every other chapter reads.

    python seed.py run --warehouse-id <id>
    python seed.py run --schema my_catalog.retail      # if main is not writable

Writes `<schema>.orders` from `samples.bakehouse.sales_transactions`, which every
workspace ships with, and a view over it without `customer_id`. Both are SQL statements
on a warehouse: no Spark session, no cluster, and the first write of the walkthrough.
"""

from _env import step_env

from metaflow import FlowSpec, Parameter, UnityCatalogTable, step
from metaflow_extensions.spark.plugins.warehouse import query

ORDERS_SQL = """
CREATE OR REPLACE TABLE {orders} AS
SELECT
    'ord-' || CAST(transactionID AS STRING) AS order_id,
    customerID                              AS customer_id,
    DATE(dateTime)                          AS order_date,
    CAST(totalPrice AS DOUBLE)              AS amount
FROM samples.bakehouse.sales_transactions
"""

VIEW_SQL = """
CREATE OR REPLACE VIEW {view} AS
SELECT order_id, order_date, amount FROM {orders}
"""

# Chapters 3 and 4 overwrite it; created here so chapter 4's vending writer has a
# managed table to be refused by even if chapter 3 has not run.
DAILY_SQL = """
CREATE TABLE IF NOT EXISTS {daily} (order_date DATE, orders BIGINT, revenue DOUBLE)
"""


class SeedFlow(FlowSpec):
    schema = Parameter("schema", default="main.retail")
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        orders = "%s.orders" % self.schema
        view = "%s.orders_public" % self.schema
        for statement in (
            "CREATE SCHEMA IF NOT EXISTS %s" % self.schema,
            ORDERS_SQL.format(orders=orders),
            VIEW_SQL.format(view=view, orders=orders),
            DAILY_SQL.format(daily="%s.orders_daily" % self.schema),
        ):
            query(statement, warehouse_id=self.warehouse_id, output_format="none")
        # Pinning reads the Delta log through credential vending, so this line also
        # checks that the later vending branches will work.
        self.orders = UnityCatalogTable(orders)
        self.view = view
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print("seeded %r" % self.orders)
        print("and the view %s, without customer_id" % self.view)


if __name__ == "__main__":
    SeedFlow()
