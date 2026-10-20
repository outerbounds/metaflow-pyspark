"""Cost attribution tags.

Remote work this extension starts is tagged with where it came from, which is what
makes spend answerable per flow, per run, and per step rather than per warehouse.
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
    """Build the tag dict for a submitted statement."""
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
