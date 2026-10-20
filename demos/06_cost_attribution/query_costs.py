"""Demo 6b: turn the tags into a cost breakdown.

    python query_costs.py --warehouse-id abc123
    python query_costs.py --run-id 1724500000000000 --group-by step

Groups the Databricks system billing tables by the tags the submitted jobs carried, through
a SQL warehouse. This is a parameterized version of `plugins/cost.DATABRICKS_USAGE_QUERY`,
which is the copy-paste form for a workspace SQL editor; `--print-query` prints the SQL if
you would rather run it there. Nothing here is Metaflow-specific machinery: it is the
customer's own billing data, grouped by tags they can also group by themselves.

Requirements: `system.billing` enabled on the metastore, USE on the `system` catalog, and a
SQL warehouse. Usage rows land within a few hours, so a run submitted a minute ago will not
appear yet.
"""

import argparse
import os
import sys

GROUPINGS = {
    "run": ["flow", "run_id"],
    "step": ["flow", "run_id", "step"],
    "flow": ["flow"],
    "user": ["user"],
}

QUERY = """
SELECT
    {group_columns},
    SUM(u.usage_quantity) AS dbus,
    ROUND(SUM(u.usage_quantity * COALESCE(p.pricing.effective_list.default, 0)), 2)
        AS est_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.custom_tags['metaflow_flow'] IS NOT NULL
  AND u.usage_date >= DATEADD(day, -{days}, CURRENT_DATE())
  {run_filter}
GROUP BY ALL
ORDER BY est_cost_usd DESC
LIMIT {limit}
"""

COLUMNS = {
    "flow": "u.custom_tags['metaflow_flow']   AS flow",
    "run_id": "u.custom_tags['metaflow_run_id'] AS run_id",
    "step": "u.custom_tags['metaflow_step']   AS step",
    "user": "u.custom_tags['metaflow_user']   AS user",
}


def build_query(group_by, days, run_id, limit):
    columns = ",\n    ".join(COLUMNS[name] for name in GROUPINGS[group_by])
    run_filter = "AND u.custom_tags['metaflow_run_id'] = :run_id" if run_id else ""
    return QUERY.format(
        group_columns=columns, days=int(days), run_filter=run_filter, limit=int(limit)
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--warehouse-id",
        default=os.environ.get("DATABRICKS_WAREHOUSE_ID"),
        help="SQL warehouse to run the query on (or set DATABRICKS_WAREHOUSE_ID)",
    )
    parser.add_argument("--run-id", help="restrict to one Metaflow run")
    parser.add_argument("--group-by", choices=sorted(GROUPINGS), default="step")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--print-query", action="store_true", help="print the SQL and exit"
    )
    args = parser.parse_args()

    statement = build_query(args.group_by, args.days, args.run_id, args.limit)
    if args.print_query:
        print(statement)
        return
    if not args.warehouse_id:
        sys.exit("--warehouse-id is required (or set DATABRICKS_WAREHOUSE_ID)")

    import metaflow  # noqa: F401  (initializes the extension)
    from metaflow_extensions.spark.plugins.warehouse import query

    usage = query(
        statement,
        params={"run_id": args.run_id} if args.run_id else None,
        warehouse_id=args.warehouse_id,
    )
    if usage.empty:
        print(
            "No tagged usage found. Billing rows take a few hours to appear, and only "
            "jobs submitted with mode='job' carry custom_tags."
        )
        return
    print(usage.to_string(index=False))


if __name__ == "__main__":
    main()
