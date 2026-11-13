# Chapter 3: what a Spark step hands to the next one

```bash
python results.py run
```

Chapter 2 compared ways of reading. This chapter holds the reader fixed, a Connect session running chapter 2's statement, and varies what goes into the artifact:

| Branch | The artifact holds |
| --- | --- |
| `copy` | the daily aggregate as a DataFrame, pickled into Metaflow's datastore |
| `everything` | every row of the table, which is what `toPandas()` on an unaggregated read does |
| `reference` | a `UnityCatalogTable` for `ORDERS_DAILY`, written with `saveAsTable` and pinned to the version just written |

`compare` prints each artifact's pickled size, read from the datastore, and then reads each one back. The output looks like this, with sizes taken from the job-based version of this flow:

```
form            artifact  holds
copy               1.3KB  DataFrame, 17 rows
everything       115.0KB  DataFrame, 3333 rows
reference          0.7KB  UnityCatalogTable(main.retail.orders_daily @v10)
```

## Copy or reference

The two copies are convenient: the next step has the data in hand and needs nothing else. They are also copies of governed data in a store Unity Catalog does not govern, and their size grows with the result. The seeded table is small enough that `everything` stays well under a megabyte, but on a real table it is the artifact that fills the datastore and makes every downstream step slow to load.

`reference` stays small whatever the table's size, and the data stays governed. The step writes `ORDERS_DAILY`, then pins a `UnityCatalogTable` to the version it just wrote, with `pin="required"` so that a failed pin fails the step instead of storing a reference that silently reads the latest version. Any later step, or a re-run of one, reads exactly what this step wrote, through Spark or through vending, while the table itself stays in Unity Catalog under its grants. That property is what chapter 5 builds on.

The reference costs a `MODIFY` or `CREATE TABLE` grant and a table in the catalog, so copies remain the right choice for small results such as metrics or a few thousand rows.
