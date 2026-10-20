"""Demo 2b: move the table on, so the pin has something to prove.

    python mutate_orders.py run --table main.retail.orders --warehouse-id <id>

Appends a day of rows through a SQL warehouse, which bumps the table's Delta version.
Run this between `governed_read.py` and `reproduce.py`.
"""

from _env import step_env

from metaflow import FlowSpec, Parameter, step
from metaflow_extensions.spark.plugins.warehouse import query


class MutateOrdersFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    rows = Parameter("rows", default=1000)
    warehouse_id = Parameter("warehouse-id", default=None)

    def latest_version(self):
        history = query(
            "DESCRIBE HISTORY %s LIMIT 1" % self.table, warehouse_id=self.warehouse_id
        )
        return int(history["version"].iloc[0])

    @step_env()
    @step
    def start(self):
        print("version before: %s" % self.latest_version())
        query(
            """
            INSERT INTO {table}
            SELECT
                'demo-' || CAST(id AS STRING)      AS order_id,
                CURRENT_DATE()                     AS order_date,
                ROUND(rand(7) * 500, 2)            AS amount
            FROM range({rows})
            """.format(table=self.table, rows=int(self.rows)),
            warehouse_id=self.warehouse_id,
            output_format="none",
        )
        self.version = self.latest_version()
        print("version after:  %s" % self.version)
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "table is now at v%s; earlier runs still point at their own version"
            % self.version
        )


if __name__ == "__main__":
    MutateOrdersFlow()
