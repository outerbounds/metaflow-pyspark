# Chapter 0: the table every chapter reads

```bash
python seed.py run --warehouse-id <id>
python seed.py run --warehouse-id <id> --schema my_catalog.retail   # if main is not writable
```

Every data chapter asks the same question, daily orders and revenue, of the same table, so that the only thing changing between them is how the data is reached. This flow creates that table from `samples.bakehouse.sales_transactions`, which every workspace ships with:

| Object | What it is |
| --- | --- |
| `<schema>.orders` | `order_id`, `customer_id`, `order_date`, `amount`, one row per bakehouse transaction |
| `<schema>.orders_public` | a view over `orders` without `customer_id`, the usual way to share a table with people who should not see who bought what |
| `<schema>.orders_daily` | an empty `(order_date, orders, revenue)` table that chapters 3 and 4 overwrite |

All three are SQL statements sent to a warehouse through `query()`, so this is also the first write of the walkthrough, and it starts no Spark session and no cluster. The last line assigns a `UnityCatalogTable`, which pins the new table's Delta version through credential vending; if that prints a warning instead of a version, the vending branches in later chapters will report the same problem.

The other chapters default to `main.retail`. With a different `--schema`, pass `--table <schema>.orders` to them and change `ORDERS_DAILY` in [`../_orders.py`](../_orders.py).

## What the chapters share

[`../_orders.py`](../_orders.py) holds the table names and one statement:

```sql
SELECT order_date, COUNT(*) AS orders, SUM(amount) AS revenue
FROM {source}
GROUP BY order_date
ORDER BY order_date
```

`{source}` is `main.retail.orders VERSION AS OF <n>` for an engine that reads the table itself, or a DuckDB view over vended files. The same module provides `same_answer()`, which compares results from different engines. It imports no pyspark, so any step can import it. Each demo directory links to it the same way it links to `_env.py`.
