from metaflow_extensions.spark.plugins.context import TaskContext
from metaflow_extensions.spark.plugins.cost import MAX_TAG_LENGTH, build_tags, sanitize


def make_ctx(**kwargs):
    base = dict(
        step_name="features",
        pathspec="RetailFlow/42/features/7",
        flow_name="RetailFlow",
        run_id="42",
        task_id="7",
        attempt=1,
        user="philipp",
        tags={},
    )
    base.update(kwargs)
    return TaskContext(**base)


def test_tags_carry_the_whole_pathspec():
    tags = build_tags(make_ctx())
    assert tags["metaflow_flow"] == "RetailFlow"
    assert tags["metaflow_run_id"] == "42"
    assert tags["metaflow_step"] == "features"
    assert tags["metaflow_task_id"] == "7"
    assert tags["metaflow_attempt"] == "1"
    assert tags["metaflow_user"] == "philipp"


def test_tags_are_sanitized_for_databricks():
    tags = build_tags(make_ctx(user="first.last+ci@example.com"))
    assert tags["metaflow_user"] == "first.last_ci_example.com"


def test_long_tag_values_are_truncated_not_rejected():
    assert len(sanitize("x" * 500)) == MAX_TAG_LENGTH


def test_extra_tags_are_merged():
    tags = build_tags(make_ctx(), extra={"cost_center": "ml-platform"})
    assert tags["cost_center"] == "ml-platform"


def test_missing_user_is_omitted_rather_than_tagged_none():
    tags = build_tags(make_ctx(user=None))
    assert "metaflow_user" not in tags
