"""Chapter 5b: move the table on, so the pin has something to prove.

    python mutate_orders.py run --table main.retail.orders --warehouse-id <id>

Appends a day of orders with an INSERT on a SQL warehouse, which bumps the table's
Delta version. No Spark is needed to change a table. Run this between
`governed_read.py` and `reproduce.py`.
"""

from _env import step_env
from _orders import ORDERS

from metaflow import FlowSpec, Parameter, UnityCatalogTable, step
from metaflow_extensions.spark.plugins.warehouse import query

INSERT_SQL = """
INSERT INTO {table} (order_id, order_date, amount)
SELECT
    'demo-' || CAST(id AS STRING) AS order_id,
    CURRENT_DATE()                AS order_date,
    ROUND(rand(7) * 50, 2)        AS amount
FROM range({rows})
"""


class MutateOrdersFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)
    rows = Parameter("rows", default=1000)
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        before = UnityCatalogTable(self.table)
        query(
            INSERT_SQL.format(table=before.full_name, rows=int(self.rows)),
            warehouse_id=self.warehouse_id,
            output_format="none",
        )
        self.after = before.latest()
        print("%r -> %r" % (before, self.after))
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "The table is now at v%s; earlier runs still point at their own version."
            % self.after.version
        )


if __name__ == "__main__":
    MutateOrdersFlow()
