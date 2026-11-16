"""Chapter 3: what a Spark step hands to the next one.

    python results.py run

The same Connect session and the same statement as chapter 2, three times, with a
different answer to "what goes in the artifact":

* `copy`: the aggregate as a pandas DataFrame, pickled into Metaflow's datastore.
* `everything`: every row of the table as a DataFrame, which is what `toPandas()` on an
  unaggregated read does. At real sizes this is the artifact nobody wants.
* `reference`: the step writes `ORDERS_DAILY` with `saveAsTable`, and the artifact is a
  `UnityCatalogTable` pinned to the version just written. Small, governed, and a re-run
  of the next step reads the same bytes.

`compare` prints each artifact's pickled size from the datastore and then reads each
one back, which is where the difference between a copy and a reference shows.
"""

from _env import step_env
from _orders import DAILY_SQL, ORDERS, ORDERS_DAILY, source

from metaflow import FlowSpec, Parameter, Step, UnityCatalogTable, current, spark, step

FORMS = ("copy", "everything", "reference")


class ResultsFlow(FlowSpec):
    table = Parameter("table", default=ORDERS)

    @step_env("vending")
    @step
    def start(self):
        orders = UnityCatalogTable(self.table)
        self.source = source(orders.full_name, orders.version)
        print("reading %r" % orders)
        self.next(self.copy, self.everything, self.reference)

    @step_env("connect")
    @spark(backend="databricks")
    @step
    def copy(self):
        self.result = self.spark.sql(DAILY_SQL.format(source=self.source)).toPandas()
        self.next(self.compare)

    @step_env("connect")
    @spark(backend="databricks")
    @step
    def everything(self):
        self.result = self.spark.sql("SELECT * FROM %s" % self.source).toPandas()
        self.next(self.compare)

    @step_env("connect")
    @spark(backend="databricks")
    @step
    def reference(self):
        daily = self.spark.sql(DAILY_SQL.format(source=self.source))
        daily.write.mode("overwrite").saveAsTable(ORDERS_DAILY)
        # Pinned right after the write, so it records the version this step produced.
        self.result = UnityCatalogTable(ORDERS_DAILY, pin="required")
        self.next(self.compare)

    @step_env("vending")
    @step
    def compare(self, inputs):
        print("\n%-11s %12s  %s" % ("form", "artifact", "holds"))
        self.sizes = {}
        for name in FORMS:
            task = Step("%s/%s/%s" % (current.flow_name, current.run_id, name)).task
            self.sizes[name] = task["result"].size
            print(
                "%-11s %10.1fKB  %s"
                % (
                    name,
                    self.sizes[name] / 1024.0,
                    _describe(getattr(inputs, name).result),
                )
            )

        daily = inputs.reference.result
        print("\ncopy        already here, %d rows" % len(inputs.copy.result))
        print("everything  already here, %d rows" % len(inputs.everything.result))
        print(
            "reference   %d rows read through vending at v%s, whatever the table "
            "has become since" % (daily.to_arrow().num_rows, daily.version)
        )
        self.daily = daily
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        print(
            "\nThe reference is the only form that is small, governed by Unity "
            "Catalog, and pinned. %r is what chapter 5 builds on." % self.daily
        )


def _describe(value):
    if isinstance(value, UnityCatalogTable):
        return repr(value)
    return "%s, %d rows" % (type(value).__name__, len(value))


if __name__ == "__main__":
    ResultsFlow()
