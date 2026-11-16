# Chapter 5: governance and reproducibility

Answers the question that comes up as "does Metaflow work with Unity Catalog, or does it go around it": governance stays in Unity Catalog, and Metaflow adds reproducibility on top of it. Three claims, each with something to run.

**Governance is enforced, not bypassed.** `UnityCatalogTable("...")` calls the Unity Catalog metadata API, so a user without `SELECT` fails on that line with Unity Catalog's own message plus a hint about which grants are missing. Nothing in this extension holds a cloud credential of its own; every path in chapters 2 and 4 got its access from Unity Catalog.

**A reference is a better artifact than a copy.** `self.orders = UnityCatalogTable(name)` stores a pinned reference, not data, which chapter 3 measured. The datastore stays small and governed data stays where it is governed.

**Re-running a flow re-reads the same bytes.** Constructing the reference records the table's current Delta version, and every reader honours it: `to_spark()` through `versionAsOf`, vending through the Delta log, and the `VERSION AS OF` clause the earlier chapters put in their statements. A run after the table changed still sees what the original run saw.

## Run it

```bash
python governed_read.py run
python mutate_orders.py run --warehouse-id <id>
python reproduce.py run
```

Each step uses the cheapest path from the earlier chapters for its job. `governed_read.py` pins the table and computes the daily summary through vending. `mutate_orders.py` appends a day of orders with an `INSERT` on a warehouse, since moving a table on needs no Spark. `reproduce.py` loads the original run's reference, reads both the pinned and the latest version through vending, and recomputes the summary from the pin:

```
pinned v6: 3333 rows
latest v7: 4333 rows

The table moved and the replay did not: the daily summary recomputed from the pin matches the original run's.
```

Nothing in `reproduce.py` names a version. The artifact carries it, so reproducibility is a property of the run rather than a number someone remembered to write down.

If the workspace withholds `EXTERNAL USE SCHEMA`, vending is not available and the pin cannot be read at assignment, and `governed_read.py` fails on its `start` step rather than storing an unpinned reference. Read through Spark instead, with `to_spark(self.spark)` in a `@spark` step, and pin explicitly with `UnityCatalogTable(name, version=N)`.

## The permission case

Run the first flow with a profile for a user without the grant:

```bash
DATABRICKS_CONFIG_PROFILE=restricted python governed_read.py run
```

The run fails on the `UnityCatalogTable(...)` line in `start`, with Unity Catalog's error, before any data is touched.
