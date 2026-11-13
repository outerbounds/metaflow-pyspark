"""Chapter 4: one summary, three writers.

    python writers.py run --warehouse-id <id>
    python writers.py run --warehouse-id <id> --vending-table <external table>

Each branch computes `DAILY_SQL` over the same pinned orders and writes the result to a
Unity Catalog table of its own, then ends the same way: a `UnityCatalogTable` pinned to
the version it wrote, tagged with the Metaflow task that wrote it.

* `connect`: `saveAsTable` from a Spark Connect session.
* `warehouse`: a `CREATE OR REPLACE TABLE ... AS` statement through `query()`.
* `vending`: DuckDB computes the summary in this task and `UnityCatalogTable.write()`
  commits it to the table's files with vended READ_WRITE credentials. That only works
  for an external table; the default target is the managed `orders_daily` the seed
  created, so the first command shows the refusal and the second, given an external
  table with the same three columns, shows the write.
"""

from _env import step_env
from _orders import DAILY_SQL, ORDERS, ORDERS_DAILY, same_answer, source

from metaflow import (
    FlowSpec,
    Parameter,
    Step,
    UnityCatalogTable,
    catch,
    current,
    spark,
    step,
)
from metaflow_extensions.spark.plugins.warehouse import query

WRITERS = ("connect", "warehouse", "vending")


def target(writer):
    return "%s_%s" % (ORDERS_DAILY, writer)


class WritersFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)
    warehouse_id = Parameter("warehouse-id", default=None)
    vending_table = Parameter("vending-table", default=ORDERS_DAILY)

    @step_env("vending")
    @step
    def start(self):
        orders = UnityCatalogTable(self.table)
        self.orders = orders
        self.table_name = orders.full_name
        self.version = orders.version
        print("summarizing %r" % orders)
        self.next(self.connect, self.warehouse, self.vending)

    @step_env("connect")
    @catch(var="failure")
    @spark(backend="databricks")
    @step
    def connect(self):
        statement = DAILY_SQL.format(source=source(self.table_name, self.version))
        self.spark.sql(statement).write.mode("overwrite").saveAsTable(target("connect"))
        self.written = UnityCatalogTable(target("connect"))
        # Inside a @spark step, stamp_lineage runs through this session.
        self.written.stamp_lineage(current)
        self.next(self.compare)

    @step_env("vending")
    @catch(var="failure")
    @step
    def warehouse(self):
        statement = DAILY_SQL.format(source=source(self.table_name, self.version))
        query(
            "CREATE OR REPLACE TABLE %s AS %s" % (target("warehouse"), statement),
            warehouse_id=self.warehouse_id,
            output_format="none",
            tags={"write_path": "warehouse"},
        )
        self.written = UnityCatalogTable(target("warehouse"))
        self.written.stamp_lineage(current, warehouse_id=self.warehouse_id)
        self.next(self.compare)

    @step_env("vending")
    @catch(var="failure")
    @step
    def vending(self):
        connection = self.orders.to_duckdb(view_name="orders")
        summary = connection.execute(
            DAILY_SQL.format(source="orders")
        ).fetch_arrow_table()
        self.written = UnityCatalogTable(self.vending_table).write(
            summary, mode="overwrite"
        )
        self.written.stamp_lineage(current, warehouse_id=self.warehouse_id)
        self.next(self.compare)

    @step_env("vending")
    @step
    def compare(self, inputs):
        reference = None
        self.outputs = {}
        failures = []
        print("\n%-10s %8s  %-58s %s" % ("writer", "step s", "wrote", "answer"))
        for name in WRITERS:
            branch = getattr(inputs, name)
            task = Step("%s/%s/%s" % (current.flow_name, current.run_id, name)).task
            seconds = (task.finished_at - task.created_at).total_seconds()
            failure = getattr(branch, "failure", None)
            if failure is not None:
                failures.append((name, failure))
                print("%-10s %8.1f  %-58s" % (name, seconds, "nothing"))
                continue

            ref = branch.written
            self.outputs[name] = ref
            # Read every output back the same way, so the comparison is about what was
            # written, not about how it is read.
            frame = ref.to_pandas()
            if reference is None:
                reference, answer = frame, "reference"
            else:
                answer = "same" if same_answer(reference, frame) else "DIFFERENT"
            print("%-10s %8.1f  %-58r %s" % (name, seconds, ref, answer))

        for name, failure in failures:
            # The reason is what matters on screen, not the traceback.
            print(
                "\n%s could not write: %s\n%s"
                % (name, failure.type.rsplit(".", 1)[-1], failure.exception.strip())
            )
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "\nEvery table that was written carries metaflow_pathspec as a Unity "
            "Catalog tag. Open one in Catalog Explorer and paste the tag into "
            "Task(...) to get back to the step that wrote it."
        )


if __name__ == "__main__":
    WritersFlow()
