# Demo 6: "why did this month cost more?"

Databricks bills compute. It has no way to know that a given cluster-hour belonged to the feature
step of the churn flow, run by a specific person, on a specific attempt. That mapping is exactly what
Metaflow knows and the platform does not, so every job this extension submits carries it as
`custom_tags`:

```
metaflow_flow, metaflow_run_id, metaflow_step, metaflow_task_id,
metaflow_pathspec, metaflow_attempt, metaflow_user
```

plus anything you add with `@spark(tags={"cost_center": "ml-platform"})`. Those tags flow into
`system.billing.usage`, which turns a monthly total into a per-flow, per-run, per-step breakdown
using the customer's own billing data.

## Run it

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
export DEMO_INSTANCE_POOL_ID=0801-...-pool
export DATABRICKS_WAREHOUSE_ID=abc123

python tagged_runs.py run
python query_costs.py --run-id <run_id> --group-by step
```

`tagged_runs.py` runs the same shuffle-heavy workload three ways, on serverless, on an instance pool,
and on a fresh job cluster, plus a fourth branch that reads governed data with no Databricks compute
at all. Then it reads each run's `setup_duration` back from the Jobs API and prints the cold-start
comparison. Cluster versus pool versus serverless, measured on your workspace rather than asserted
from a slide.

Billing rows take a few hours to appear, so `query_costs.py` on a run from a minute ago will come up
empty. That is the usage pipeline's latency, not a bug in the tags. To see the shape of the output
immediately, run it against yesterday's runs, or use `--print-query` and paste the SQL into a
workspace SQL editor.

Useful groupings:

```bash
python query_costs.py --group-by flow           # which flows cost the most
python query_costs.py --group-by step --days 7  # which step inside them
python query_costs.py --group-by user           # who is spending
```

## Requirements

`system.billing` enabled on the metastore, `USE CATALOG` on `system`, and a SQL warehouse. Tags on
`system.billing.usage` come from the compute Databricks started for the job, so the demo uses
`mode="job"`.

## Connect mode is different

In `mode="connect"` the session runs against compute the extension did not create, usually a shared
serverless endpoint or an existing cluster, so there are no per-run `custom_tags` to set. What the
backend does instead is label the session: every query gets the Metaflow pathspec as its job
description and a session tag, so the queries are findable in Query History and in
`system.query.history` where the workspace has it enabled. That gives per-query attribution to a
pathspec, but the DBU line item still belongs to the shared compute.

The practical consequence, worth stating plainly in a call: if per-step cost attribution matters, use
`mode="job"` for the steps whose cost you need to attribute, or give each team its own compute and
attribute at that level. Connect mode is the better developer experience; job mode is the better
accounting.
