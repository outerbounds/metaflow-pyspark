# Chapter 4: one summary, three writers

```bash
python writers.py run --warehouse-id <id>
python writers.py run --warehouse-id <id> --vending-table <external table>
```

The write-side mirror of chapter 2. Each branch computes the daily summary over the same pinned orders and writes it to a Unity Catalog table of its own, `main.retail.orders_daily_<writer>`:

| Writer | How it writes |
| --- | --- |
| `connect` | `self.spark.sql(...).write.saveAsTable(...)` in a Connect session |
| `warehouse` | `query("CREATE OR REPLACE TABLE ... AS SELECT ...")` |
| `vending` | DuckDB computes the summary in the task, and `UnityCatalogTable.write()` commits it to the table's files with vended `READ_WRITE` credentials |

Every branch ends the same way. The artifact is a `UnityCatalogTable` pinned to the version just written, and `stamp_lineage(current)` sets `metaflow_pathspec`, `metaflow_flow`, and `metaflow_run_id` as Unity Catalog tags on the table. `compare` reads every output back through vending and checks they hold the same answer.

## What differs

| | Connect | Warehouse | Vending |
| --- | --- | --- | --- |
| Grants | `MODIFY` / `CREATE TABLE` | the same, plus `CAN USE` on the warehouse | `MODIFY` plus `EXTERNAL USE SCHEMA` |
| Table types | managed or external | managed or external | external only |
| Unity Catalog lineage | yes, the engine records it | yes | no, the engine never sees the write |
| Constraints, generated columns | enforced | enforced | not evaluated |
| DBUs | serverless | SQL | none |

Unity Catalog lineage records that a query wrote the table. The tags add what it cannot know: which Metaflow task ran that query. In Catalog Explorer, open any of the output tables and paste `metaflow_pathspec` into `Task("...")` in a notebook to get back to the step. For the vending writer the tags are the only lineage there is.

## The vending writer and managed tables

Databricks has to commit writes to a managed table, so `write()` refuses one before touching any files:

```
UnityCatalogError: main.retail.orders_daily is a managed table, so it cannot be written
through credential vending: Databricks has to commit writes to managed tables. ...
```

The default `--vending-table` is the managed `orders_daily` the seed created, so the first command shows that refusal while the other two writers succeed. To see the vending write go through, create an external table with the same three columns, `(order_date DATE, orders BIGINT, revenue DOUBLE)`, under an external location you can write to, and pass it as `--vending-table`.
