"""Cost attribution tags.

Remote work this extension starts is tagged with where it came from. Job tags land in
the compute provider's billing records, and statement tags in the query history, which
is what makes spend answerable per flow, per run, and per step rather than per cluster.
"""

import re

TAG_PREFIX = "metaflow_"

#: Databricks rejects tag values outside this character set, and silently truncates
#: long ones. Normalizing up front avoids a submit-time error on an odd username.
_SAFE = re.compile(r"[^A-Za-z0-9_\-\.\s]")
MAX_TAG_LENGTH = 255


def sanitize(value):
    if value is None:
        return None
    return _SAFE.sub("_", str(value))[:MAX_TAG_LENGTH]


def build_tags(ctx, extra=None):
    """Build the tag dict for a submitted statement or job."""
    tags = {
        TAG_PREFIX + "flow": ctx.flow_name,
        TAG_PREFIX + "run_id": ctx.run_id,
        TAG_PREFIX + "step": ctx.step_name,
        TAG_PREFIX + "task_id": ctx.task_id,
        TAG_PREFIX + "pathspec": ctx.pathspec,
        TAG_PREFIX + "attempt": str(ctx.attempt),
    }
    if ctx.user:
        tags[TAG_PREFIX + "user"] = ctx.user
    tags.update(extra or {})
    return {sanitize(k): sanitize(v) for k, v in tags.items() if v is not None}


#: Ready-made drill-down query for the Databricks system billing tables, covering
#: submitted jobs. Referenced by the cost demo, and useful to paste into a workspace SQL
#: editor as-is.
DATABRICKS_USAGE_QUERY = """
SELECT
    custom_tags['metaflow_flow']   AS flow,
    custom_tags['metaflow_run_id'] AS run_id,
    custom_tags['metaflow_step']   AS step,
    custom_tags['metaflow_user']   AS user,
    SUM(usage_quantity)            AS dbus,
    SUM(usage_quantity * COALESCE(p.pricing.effective_list.default, 0)) AS est_cost_usd
FROM system.billing.usage u
LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE custom_tags['metaflow_flow'] IS NOT NULL
  AND usage_date >= DATEADD(day, -30, CURRENT_DATE())
GROUP BY ALL
ORDER BY est_cost_usd DESC
"""
