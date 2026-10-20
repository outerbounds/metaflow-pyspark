"""Demo 4a: put Metaflow in front of pipelines that already exist.

    python orchestrate.py run

Nothing on the Databricks side changes. The jobs keep their Asset Bundle definitions,
their permissions, their schedules, and their owners. Metaflow triggers them, waits, reads
their task values, and then does the part Databricks Workflows is not good at: fanning out
over the results and training somewhere else.

`@databricks_job` takes either `job_id` or `job_name`. Names are nicer to read and get
resolved to an id at submit time, so a job recreated by a bundle deploy keeps working.
"""

from _env import step_env

from metaflow import (
    FlowSpec,
    Parameter,
    UnityCatalogTable,
    databricks_job,
    databricks_notebook,
    retry,
    step,
    timeout,
)


#: Decorator attributes are read when the flow is defined, so paths and job names are
#: module constants rather than Parameters. Values that vary per run travel through
#: `parameters_from`, which reads them off the flow at submit time.
VALIDATE_NOTEBOOK = "/Repos/data-platform/etl/validate_features"
FALLBACK_TABLE = "samples.bakehouse.sales_transactions"


class OrchestrateFlow(FlowSpec):
    as_of = Parameter("as-of", default="2026-08-24")

    @step_env()
    @step
    def start(self):
        self.run_date = self.as_of
        self.next(self.ingest, self.enrich)

    # The SLA controls are Metaflow's own and work the same here as on any other step:
    # `timeout` cancels the Databricks run rather than orphaning it, and `retry`
    # resubmits. Deploy-time alerting is a scheduler flag, see the README.
    @step_env()
    @timeout(minutes=45)
    @retry(times=2)
    @databricks_job(job_name="raw-orders-ingest", parameters_from=["run_date"])
    @step
    def ingest(self):
        """Trigger the existing ingest job and read what its tasks returned."""
        print("ingest task values: %s" % self.databricks_result)
        self.next(self.join)

    @step_env()
    @databricks_job(
        job_name="customer-enrichment",
        parameters_from=["run_date"],
        output_artifact="enrichment",
    )
    @step
    def enrich(self):
        print("enrichment task values: %s" % self.enrichment)
        self.next(self.join)

    @step_env()
    @step
    def join(self, inputs):
        self.run_date = inputs.ingest.run_date
        # Task values from a Databricks job come back keyed by task_key, which is how a
        # multi-task job hands its outputs to whatever comes next.
        self.tables = sorted(
            {
                value
                for result in (
                    inputs.ingest.databricks_result,
                    inputs.enrich.enrichment,
                )
                for value in (result or {}).values()
                if isinstance(value, str) and value.count(".") == 2
            }
        )
        if not self.tables:
            # A job that does not set task values leaves nothing to fan out over, so fall
            # back to the table the pipelines are known to write.
            self.tables = [FALLBACK_TABLE]
        print("tables produced upstream: %s" % ", ".join(self.tables))
        self.next(self.validate)

    @step_env()
    @databricks_notebook(
        notebook_path=VALIDATE_NOTEBOOK,
        parameters_from=["run_date"],
        serverless=True,
    )
    @step
    def validate(self):
        """Run an existing notebook as a step, unchanged."""
        print("notebook returned: %s" % self.notebook_result)
        self.next(self.profile, foreach="tables")

    @step_env("vending")
    @step
    def profile(self):
        """One branch per upstream table, which is the part Workflows does not do well.

        Reads through credential vending, so the fan-out costs no DBUs however wide it
        gets. Swap the body for a real training step and the shape does not change.
        """
        self.table_name = self.input
        table = UnityCatalogTable(self.table_name)
        frame = table.to_pandas()
        self.table_stats = {
            "table": self.table_name,
            "version": table.version,
            "rows": len(frame),
            "columns": list(frame.columns),
        }
        print(self.table_stats)
        self.next(self.report)

    @step_env()
    @step
    def report(self, inputs):
        self.stats = [i.table_stats for i in inputs]
        self.next(self.end)

    @step_env()
    @step
    def end(self):
        for stats in self.stats:
            print(
                "%s v%s: %d rows, %d columns"
                % (
                    stats["table"],
                    stats["version"],
                    stats["rows"],
                    len(stats["columns"]),
                )
            )
        print(
            "\nThree pieces of existing Databricks work ran as Metaflow steps, then the "
            "flow fanned out over their output. Nothing on the Databricks side was "
            "rewritten."
        )


if __name__ == "__main__":
    OrchestrateFlow()
