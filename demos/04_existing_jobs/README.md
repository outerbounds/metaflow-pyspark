# Demo 4: "do I have to rewrite my pipelines?"

No. Two flows, showing both directions.

## Orchestrate what exists

```bash
python orchestrate.py run --as-of 2026-08-24
```

`orchestrate.py` triggers two existing Databricks Jobs by name and one existing notebook by path,
waits for each, reads their task values, and then fans out over the tables they produced. Nothing in
the workspace is modified: the jobs keep their Asset Bundle definitions, their cluster policies,
their permissions, and their owners. Metaflow is the thing calling them, not the thing replacing
them.

Names rather than ids on purpose. `job_name="raw-orders-ingest"` is resolved at submit time, so a job
that a bundle deploy recreates with a new id keeps working. `parameters_from=["run_date"]` reads the
value off the flow when the run is submitted, which is how a Metaflow Parameter reaches a job
parameter without templating anything into the decorator.

The fan-out is the part worth pausing on. Per-branch compute, per-branch retries, and artifacts that
carry between steps are what Metaflow adds; the upstream jobs stay exactly as good as they were.
The `profile` step reads through credential vending, so widening the fan-out costs no DBUs.

## SLA parity

The checklist question is whether timeouts, retries, and failure alerting work the same as in
Workflows or Airflow. They are Metaflow's own decorators and need nothing Databricks-specific:

```python
@timeout(minutes=45)
@retry(times=2)
@databricks_job(job_name="raw-orders-ingest", parameters_from=["run_date"])
```

`@timeout` interrupts the waiting task, and the backend translates that into a cancel call against
the Databricks run rather than leaving it running and billing. `@retry` resubmits. Alerting is a
deploy-time flag on whichever scheduler the flow is deployed to, for example:

```bash
python orchestrate.py argo-workflows create \
    --notify-on-error --notify-slack-webhook-url "$SLACK_WEBHOOK"
```

## Port one job, keep the rest

```bash
python ported_step.py run --as-of 2026-08-24
```

`ported_step.py` is the same `customer-enrichment` logic as a `@spark` step, with the Spark code in
`enrichment.py` as a plain function of `(spark, run_date, orders_table_name)`. Show the two flows
side by side: the migration is per job and reversible, and the same Spark runs on the same compute
either way. What you gain is that the logic is importable, unit-testable without a workspace,
runnable with one command, and returns an artifact instead of writing a table that the next step has
to know the name of.

The recommended sequence for a real migration is therefore: wrap everything with `@databricks_job`
first so the whole pipeline is orchestrated and observable, then port individual jobs where the
Python-side ergonomics are worth it, and leave the rest alone indefinitely.

## What the demo needs

Two jobs and one notebook in the workspace. If you do not have candidates, create jobs named
`raw-orders-ingest` and `customer-enrichment` whose tasks call `dbutils.jobs.taskValues.set` with a
table name, for example:

```python
dbutils.jobs.taskValues.set(key="table", value="samples.bakehouse.sales_transactions")
```

That is what `orchestrate.py` reads back as `self.databricks_result`, keyed by task key. If your
workspace has its own governed orders table, use that instead — `enrichment.py` expects
`transactionID`, `customerID`, `totalPrice`, and `dateTime` columns to match the
`samples.bakehouse.sales_transactions` shape used above; adjust its column names if your table's
schema differs.
