# Demos

Each demo maps to a question prospects ask. Numbering follows the original roadmap, so the gap at `05` is a demo that is not built yet rather than a missing file.

| Demo | Answers | Needs a Databricks account |
| --- | --- | --- |
| [`01_hello_spark`](01_hello_spark) | "is it real Spark", "how do I get started" | no, runs locally too |
| [`02_unity_catalog`](02_unity_catalog) | governance, reproducibility over governed data | yes |
| [`03_no_cluster`](03_no_cluster) | reading governed data without paying for a cluster | yes |
| [`04_existing_jobs`](04_existing_jobs) | "do I have to rewrite my pipelines" | yes |
| [`06_cost_attribution`](06_cost_attribution) | "why did this month cost more" | yes, plus system tables |

Each demo directory has its own README with the narrative and the exact commands. Run them from
inside their directory, since the flows import job modules sitting next to them.

## Setup

```bash
pip install -e '..[connect,catalog]'
export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
```

Any authentication the Databricks SDK understands works, including `~/.databrickscfg` profiles and
OAuth service principals, because the extension delegates the whole resolution chain to the SDK:

```bash
export DATABRICKS_CONFIG_PROFILE=my-workspace
```

Demo 1 needs none of this. Run it first.

Demo 6's zero-DBU branch reads `samples.bakehouse.sales_transactions`, which every workspace ships
with, so no seed data is needed there. Demos 2 and 3 both read `main.retail.orders`, and demo 2
mutates it, so both need a table you own; seed one from the same sample data using the
`CREATE TABLE` in [`02_unity_catalog/README.md`](02_unity_catalog/README.md#seed-data). Demos that
submit jobs also need a UC Volume to stage code on:

```bash
export METAFLOW_DATABRICKS_VOLUME=/Volumes/main/metaflow/staging
```

Demo 3's SQL warehouse branch needs a warehouse id, via `--warehouse-id` on the flow or:

```bash
export DATABRICKS_WAREHOUSE_ID=<warehouse-id>
```

## Running on Outerbounds

Install the extension next to `outerbounds` on the machine you launch from; Metaflow ships it to
the task pods in the code package. Then pick a virtual environment and send the steps to
Kubernetes:

```bash
python hello_spark.py --environment=fast-bakery run --with kubernetes
python hello_spark.py --environment=fast-bakery argo-workflows create
```

Every step carries a `@step_env(...)` from [`_env.py`](_env.py), which does nothing under the
default local environment and, under `fast-bakery`, gives the step:

- `@anaconda` from Anaconda's main channel, or `@pypi` for steps that need `databricks-connect`
  (PyPI only) or `deltalake` (conda-forge only, and incompatible with Anaconda's Python);
- `@secrets(sources=["outerbounds.databricks"])` for `DATABRICKS_HOST` and `DATABRICKS_TOKEN`;
- `@environment` carrying the settings above (`METAFLOW_DATABRICKS_VOLUME`,
  `DATABRICKS_WAREHOUSE_ID`, `METAFLOW_SPARK_BACKEND`, and the rest of `FORWARDED_SETTINGS`) from
  the launching shell into the pods. For a deployment they are captured at `create` time.

The pods need outbound HTTPS to the Databricks workspace and, for credential vending, to the
table's cloud storage.

## Which demo answers which objection

**"We already have Databricks, why add anything."** Demo 4 first, then 6. Metaflow orchestrates what
exists without touching it, and then tells them where their money went, which their platform cannot.

**"Does this respect our governance."** Demo 2. Every read goes through Unity Catalog, and Metaflow
adds version pinning on top rather than copying data out.

**"Spark is expensive for what we do."** Demo 3. Same table, same grants, no cluster: a Spark session,
credential vending, and a SQL warehouse statement side by side, so the trade-off is a measurement.

**"Is this actually Spark."** Demo 1, in the three-backend form, live.
