import json

import pytest

from metaflow_extensions.spark.plugins.config import (
    DEFAULT_TIMEOUT_MINUTES,
    backend_config,
    require,
    resolve_config,
)
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for env in (
        "DATABRICKS_HOST",
        "DATABRICKS_TOKEN",
        "DATABRICKS_CONFIG_PROFILE",
        "DATABRICKS_CLIENT_ID",
        "DATABRICKS_CLIENT_SECRET",
        "DATABRICKS_WAREHOUSE_ID",
    ):
        monkeypatch.delenv(env, raising=False)
        monkeypatch.delenv("METAFLOW_" + env, raising=False)
    monkeypatch.delenv("DATABRICKS_CLUSTER_ID", raising=False)
    monkeypatch.delenv("METAFLOW_DATABRICKS_CLUSTER_ID", raising=False)
    monkeypatch.delenv("METAFLOW_SPARK_BACKEND", raising=False)
    for env in (
        "METAFLOW_SPARK_MODE",
        "METAFLOW_DATABRICKS_VOLUME",
        "METAFLOW_DATABRICKS_RUNTIME_VERSION",
        "METAFLOW_SPARK_EMR_APPLICATION_ID",
        "METAFLOW_SPARK_EMR_EXECUTION_ROLE",
        "METAFLOW_SPARK_EMR_S3_PREFIX",
    ):
        monkeypatch.delenv(env, raising=False)


class Flow:
    spark_config: object = None


def test_defaults_when_nothing_is_configured():
    config = resolve_config(Flow(), "spark_config", {})
    assert config["backend"] == "local"
    assert config["timeout"] == DEFAULT_TIMEOUT_MINUTES
    assert config["databricks"] == {}


def test_json_string_config_is_parsed():
    flow = Flow()
    flow.spark_config = json.dumps({"databricks": {"host": "h"}})
    config = resolve_config(flow, "spark_config", {})
    assert config["databricks"]["host"] == "h"


def test_explicit_overrides_beat_the_config_artifact():
    flow = Flow()
    flow.spark_config = {"timeout": 5, "databricks": {"warehouse_id": "flow"}}
    config = resolve_config(
        flow, "spark_config", {"timeout": 90, "databricks": {"warehouse_id": "arg"}}
    )
    assert config["timeout"] == 90
    assert config["databricks"]["warehouse_id"] == "arg"


def test_none_overrides_do_not_clobber():
    flow = Flow()
    flow.spark_config = {"timeout": 5}
    config = resolve_config(flow, "spark_config", {"timeout": None})
    assert config["timeout"] == 5


def test_config_can_be_passed_directly_instead_of_by_artifact_name():
    config = resolve_config(None, {"databricks": {"host": "h"}}, {})
    assert config["databricks"]["host"] == "h"


def test_env_is_below_the_config_artifact(monkeypatch):
    monkeypatch.setenv("DATABRICKS_HOST", "https://env.databricks.com")
    flow = Flow()
    flow.spark_config = {"databricks": {"host": "https://flow.databricks.com"}}
    config = resolve_config(flow, "spark_config", {})
    assert config["databricks"]["host"] == "https://flow.databricks.com"


def test_env_fills_in_what_the_flow_omits(monkeypatch):
    monkeypatch.setenv("DATABRICKS_TOKEN", "dapi-secret")
    flow = Flow()
    flow.spark_config = {"databricks": {"host": "https://flow.databricks.com"}}
    config = resolve_config(flow, "spark_config", {})
    assert config["databricks"]["token"] == "dapi-secret"


def test_metaflow_prefixed_env_wins_over_the_standard_one(monkeypatch):
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "standard")
    monkeypatch.setenv("METAFLOW_DATABRICKS_WAREHOUSE_ID", "metaflow")
    config = resolve_config(None, None, {})
    assert config["databricks"]["warehouse_id"] == "metaflow"


def test_bad_json_names_the_source():
    flow = Flow()
    flow.spark_config = "{not json"
    with pytest.raises(SparkConfigError, match="spark_config"):
        resolve_config(flow, "spark_config", {})


def test_the_backend_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("METAFLOW_SPARK_BACKEND", "databricks")
    monkeypatch.setenv("DATABRICKS_CLUSTER_ID", "0101-abc")
    config = resolve_config(None, None, {})
    assert config["backend"] == "databricks"
    assert config["databricks"]["cluster_id"] == "0101-abc"


def test_backend_specific_spark_parameters_win():
    config = {
        "spark-parameters": {"spark.executor.memory": "4g", "spark.shuffle.spill": "1"},
        "databricks": {"spark-parameters": {"spark.executor.memory": "16g"}},
    }
    section = backend_config(config, "databricks")
    assert section["spark-parameters"] == {
        "spark.executor.memory": "16g",
        "spark.shuffle.spill": "1",
    }


def test_backend_config_inherits_the_top_level_timeout():
    section = backend_config({"databricks": {"host": "h"}, "timeout": 30}, "databricks")
    assert section == {"host": "h", "timeout": 30}


def test_require_explains_how_to_set_the_key():
    with pytest.raises(SparkConfigError) as exc:
        require({}, "warehouse_id", "databricks-sql", hint="Set it with ...")
    assert "warehouse_id" in str(exc.value)
    assert "Set it with ..." in str(exc.value)


def test_a_legacy_flat_pyspark_config_becomes_an_emr_section():
    flow = Flow()
    flow.spark_config = json.dumps(
        {"application-id": "app-1", "execution-role": "arn:r", "s3-prefix": "s3://b/p"}
    )
    config = resolve_config(flow, "spark_config", {})
    assert config["backend"] == "emr-serverless"
    assert config["emr-serverless"] == {
        "application-id": "app-1",
        "execution-role": "arn:r",
        "s3-prefix": "s3://b/p",
    }
    assert "application-id" not in config


def test_an_explicit_emr_section_wins_over_legacy_keys():
    flow = Flow()
    flow.spark_config = {
        "application-id": "legacy",
        "emr-serverless": {"application-id": "explicit"},
    }
    config = resolve_config(flow, "spark_config", {})
    assert config["emr-serverless"]["application-id"] == "explicit"


def test_job_settings_come_from_the_environment(monkeypatch):
    monkeypatch.setenv("METAFLOW_SPARK_MODE", "job")
    monkeypatch.setenv("METAFLOW_DATABRICKS_VOLUME", "/Volumes/main/mf/staging")
    monkeypatch.setenv("METAFLOW_SPARK_EMR_APPLICATION_ID", "app-1")
    config = resolve_config(None, None, {})
    assert config["mode"] == "job"
    assert config["databricks"]["volume"] == "/Volumes/main/mf/staging"
    assert config["emr-serverless"]["application-id"] == "app-1"
    assert backend_config(config, "databricks")["mode"] == "job"
