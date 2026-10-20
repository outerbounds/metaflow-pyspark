"""Demo 4b: the other direction, one job ported into a step.

    python ported_step.py run --as-of 2026-08-24

`orchestrate.py` triggers `customer-enrichment` as it stands. This flow contains the same
logic as a `@spark` step instead, so the two can be shown next to each other. What changes:

* the notebook or JAR becomes a plain Python function in `enrichment.py`, importable and
  unit-testable without a workspace
* parameters stop being a `dbutils.widgets` lookup and become function arguments
* the output stops being a side effect and becomes a Metaflow artifact, so the next step
  gets it without agreeing on a table name first
* `python ported_step.py run` runs it, which means a change can be tried without deploying
  anything

What does not change: it is the same Spark, on the same compute, reading the same governed
tables. Porting is a code-organization move, not a re-platforming one, and it can happen
one job at a time while `@databricks_job` covers the rest.
"""

import enrichment
from _env import step_env

from metaflow import FlowSpec, Parameter, spark, step


class PortedStepFlow(FlowSpec):
    as_of = Parameter("as-of", default="2026-08-24")
    orders_table = Parameter(
        "orders-table", default="samples.bakehouse.sales_transactions"
    )

    @step_env("pyspark")
    @step
    def start(self):
        self.run_date = self.as_of
        self.orders_table_name = self.orders_table
        self.next(self.enrich)

    @step_env("connect")
    @spark(
        backend="databricks",
        job=enrichment.enrich_customers,
        job_parameters=["run_date", "orders_table_name"],
        output_format="pandas",
        tags={"migrated_from": "customer-enrichment"},
    )
    @step
    def enrich(self):
        print(self.spark_df.head(20).to_string(index=False))
        self.next(self.end)

    @step_env("pyspark")
    @step
    def end(self):
        print(
            "%d enriched customers, as an artifact rather than a table"
            % len(self.spark_df)
        )


if __name__ == "__main__":
    PortedStepFlow()
