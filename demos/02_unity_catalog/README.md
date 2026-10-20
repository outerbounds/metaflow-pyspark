# Demo 2: Unity Catalog governance and reproducibility

Answers the question that comes up as "does Metaflow work with Unity Catalog, or does it go around
it": governance stays in UC, and Metaflow adds reproducibility on top of it.

Three claims, each with something to run:

**Governance is enforced, not bypassed.** Every read goes through UC. `UnityCatalogTable("...")`
calls the UC metadata API, so a user without SELECT fails on that line with UC's own message plus a
hint about which grants are missing. Nothing in this extension holds a cloud credential of its own.

**A table reference is a better artifact than a table copy.** `self.orders = UnityCatalogTable(name)`
stores a pinned reference, not data. The Metaflow datastore stays small and governed data stays
where the customer governs it.

**Re-running a flow re-reads the same bytes.** Constructing the reference records the table's current
Delta version. Reads through the reference replay that version with Delta time travel, so a run that
happens after the table changed still sees what the original run saw.

None of the three flows starts a cluster. `governed_read.py` and `mutate_orders.py` run their SQL on
a warehouse through `query()`, and `reproduce.py` reads both versions through credential vending.

## Run it

If `main.retail.orders` does not exist yet, see [Seed data](#seed-data) below first.

```bash
export DATABRICKS_WAREHOUSE_ID=<id>     # or pass --warehouse-id to the first two
python governed_read.py run --table main.retail.orders
python mutate_orders.py run --table main.retail.orders
python reproduce.py run
```

`reproduce.py` prints the pinned row count next to the current one. They differ, and the pinned side
matches the first run.

To show the permission case, use a profile for a user without the grant:

```bash
DATABRICKS_CONFIG_PROFILE=restricted python governed_read.py run
```

## Lineage

`governed_read.py` writes its result back as a UC table and sets `metaflow_pathspec`,
`metaflow_flow`, and `metaflow_run_id` as UC tags on it. UC already records that the table was
written by that statement; the tags are what point from the table back to the exact Metaflow task
that produced it, which UC has no way to know. In the workspace, open the output table and look at
the tags panel, then paste the pathspec into `Task("...")` in a notebook to get to the run.

## Seed data

`mutate_orders.py` appends rows to the same table `governed_read.py` reads, so this demo needs a
table you can write to — unlike demo 6, it cannot point at `samples.bakehouse` directly, since that
catalog is Databricks-managed and read-only. Demo 3 also reads this same table, so seed it once here
and both demos are covered. If you do not have a table of your own yet, seed one from the real
`samples.bakehouse.sales_transactions` data rather than synthetic rows (swap `main.retail` for a
catalog and schema you actually have `CREATE TABLE` on):

```sql
CREATE CATALOG IF NOT EXISTS main;
CREATE SCHEMA IF NOT EXISTS main.retail;
CREATE TABLE main.retail.orders AS
SELECT
    'ord-' || CAST(transactionID AS STRING) AS order_id,
    DATE(dateTime)                          AS order_date,
    CAST(totalPrice AS DOUBLE)              AS amount
FROM samples.bakehouse.sales_transactions;
```
