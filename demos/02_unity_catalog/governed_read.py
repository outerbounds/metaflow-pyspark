"""Demo 2a: read governed data, write governed data, keep the pin.

    python governed_read.py run --table main.retail.orders --warehouse-id <id>

The interesting line is `UnityCatalogTable(self.table)` in `start`. It reads the table's
metadata through Unity Catalog and records the table's *current Delta version* on the
artifact. Nothing is copied: the artifact is a reference, so the run stays reproducible
without duplicating governed data into the Metaflow datastore.

If the caller lacks SELECT on the table, that line is where the run fails, with UC's own
error rather than a stack trace from somewhere else. Try it with a profile for a user
without the grant:

    DATABRICKS_CONFIG_PROFILE=restricted python governed_read.py run
"""

from _env import step_env

from metaflow import FlowSpec, Parameter, UnityCatalogTable, current, step
from metaflow_extensions.spark.plugins.warehouse import query


def pinned(ref):
    """`ref`'s table name with Delta time travel to the version it was pinned at."""
    if ref.version is None:
        return ref.full_name
    return "%s VERSION AS OF %d" % (ref.full_name, ref.version)


class GovernedReadFlow(FlowSpec):
    table = Parameter("table", default="main.retail.orders")
    output_table = Parameter("output-table", default="main.retail.orders_daily")
    warehouse_id = Parameter("warehouse-id", default=None)

    @step_env("vending")
    @step
    def start(self):
        self.orders = UnityCatalogTable(self.table, pin="required")
        print("pinned %r" % self.orders)
        print("storage location: %s" % self.orders.storage_location)
        self.next(self.summarize)

    @step_env("vending")
    @step
    def summarize(self):
        """Aggregate on a SQL warehouse, honouring the pin, and write the result to UC."""
        query(
            """
            CREATE OR REPLACE TABLE {output} AS
            SELECT order_date, COUNT(order_id) AS orders, SUM(amount) AS revenue
            FROM {source}
            GROUP BY order_date
            """.format(output=self.output_table, source=pinned(self.orders)),
            warehouse_id=self.warehouse_id,
            output_format="none",
        )

        # UC records table-level lineage for this write on its own. The tags are what
        # connect it back to the exact Metaflow task, which UC cannot infer.
        query(
            "ALTER TABLE %s SET TAGS ("
            "'metaflow_pathspec' = '%s', "
            "'metaflow_flow' = '%s', "
            "'metaflow_run_id' = '%s')"
            % (self.output_table, current.pathspec, current.flow_name, current.run_id),
            warehouse_id=self.warehouse_id,
            output_format="none",
        )

        self.daily = UnityCatalogTable(self.output_table)
        self.preview = query(
            "SELECT * FROM %s ORDER BY order_date LIMIT 20" % pinned(self.daily),
            warehouse_id=self.warehouse_id,
        )
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(self.preview.to_string(index=False))
        print("\nread    %r" % self.orders)
        print("wrote   %r" % self.daily)
        print(
            "\nBoth artifacts are references. Run reproduce.py to read this run's "
            "input at the version it saw."
        )


if __name__ == "__main__":
    GovernedReadFlow()
