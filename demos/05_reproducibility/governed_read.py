"""Chapter 5a: pin the table, then aggregate the pinned version.

    python governed_read.py run --table main.retail.orders

`UnityCatalogTable(self.table)` in `start` reads the table's metadata through Unity
Catalog and records its current Delta version on the artifact. Nothing is copied, and
every later read of `self.orders`, in this run or any run that loads the artifact,
reads that version.

If the caller lacks SELECT on the table, that line is where the run fails, with Unity
Catalog's own error rather than a Spark stack trace:

    DATABRICKS_CONFIG_PROFILE=restricted python governed_read.py run
"""

from _env import step_env
from _orders import DAILY_SQL, ORDERS

from metaflow import FlowSpec, Parameter, UnityCatalogTable, step


class GovernedReadFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)

    @step_env("vending")
    @step
    def start(self):
        # "required", because a reproducibility demo with an unpinned reference would
        # prove nothing.
        self.orders = UnityCatalogTable(self.table, pin="required")
        print("pinned %r" % self.orders)
        self.next(self.summarize)

    @step_env("vending")
    @step
    def summarize(self):
        # Vending, the cheapest reader from chapter 2. to_spark() would honour the pin
        # the same way.
        connection = self.orders.to_duckdb(view_name="orders")
        self.daily = connection.execute(DAILY_SQL.format(source="orders")).df()
        self.rows = connection.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(self.daily.tail(5).to_string(index=False))
        print("\n%d rows read at %r" % (self.rows, self.orders))
        print("Now run mutate_orders.py, then reproduce.py.")


if __name__ == "__main__":
    GovernedReadFlow()
