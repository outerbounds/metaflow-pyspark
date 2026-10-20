"""Tests for the Databricks Connect session backend, against a fake databricks.connect."""

import sys
import types
from types import SimpleNamespace

import pytest

from metaflow_extensions.spark.plugins.backends.databricks import connect
from metaflow_extensions.spark.plugins.exceptions import (
    SparkBackendUnavailable,
    SparkConfigError,
)


class FakeSdkConfig:
    host = "https://adb-1.azuredatabricks.net"
    token = "dapi"
    cluster_id = None
    serverless_compute_id = None

    def copy(self):
        clone = FakeSdkConfig()
        clone.__dict__.update(self.__dict__)
        return clone


class FakeSession:
    def __init__(self):
        self.tags = []
        self.interrupted = []
        self.descriptions = []
        self.stopped = False

    def addTag(self, tag):
        self.tags.append(tag)

    def interruptTag(self, tag):
        self.interrupted.append(tag)

    def setJobDescription(self, text):
        self.descriptions.append(text)

    def stop(self):
        self.stopped = True


class FakeBuilder:
    def __init__(self):
        self.sdk_config = None
        self.conf = {}
        self.session = FakeSession()

    def sdkConfig(self, config):
        self.sdk_config = config
        return self

    def config(self, key, value):
        self.conf[key] = value
        return self

    def getOrCreate(self):
        return self.session


@pytest.fixture
def builder(monkeypatch):
    fake = FakeBuilder()
    module = types.ModuleType("databricks.connect")
    module.DatabricksSession = SimpleNamespace(builder=fake)
    monkeypatch.setitem(sys.modules, "databricks.connect", module)
    return fake


def make_backend(config=None):
    backend = connect.DatabricksConnectBackend(config or {})
    backend.client = SimpleNamespace(
        sdk=SimpleNamespace(config=FakeSdkConfig()), host="https://adb-1"
    )
    return backend


def ctx():
    return SimpleNamespace(pathspec="Flow/1/start/2", log=lambda *a, **k: None)


def test_nothing_configured_means_serverless(builder):
    with make_backend().session(ctx()):
        pass
    assert builder.sdk_config.serverless_compute_id == "auto"
    assert builder.sdk_config.cluster_id is None


def test_a_cluster_id_targets_that_cluster(builder):
    with make_backend({"cluster_id": "0101-abc"}).session(ctx()):
        pass
    assert builder.sdk_config.cluster_id == "0101-abc"
    assert builder.sdk_config.serverless_compute_id is None


def test_serverless_and_a_cluster_id_conflict(builder):
    backend = make_backend({"serverless": True, "cluster_id": "0101-abc"})
    with pytest.raises(SparkConfigError, match="mutually exclusive"):
        with backend.session(ctx()):
            pass


def test_spark_parameters_are_set_on_the_builder(builder):
    with make_backend(
        {"spark-parameters": {"spark.sql.shuffle.partitions": 8}}
    ).session(ctx()):
        pass
    assert builder.conf == {"spark.sql.shuffle.partitions": "8"}


def test_queries_are_tagged_and_described_with_the_pathspec(builder):
    with make_backend().session(ctx()) as session:
        pass
    assert session.tags == ["metaflow-Flow-1-start-2"]
    assert session.descriptions == ["Flow/1/start/2"]


def test_an_interrupt_cancels_in_flight_queries(builder):
    with pytest.raises(KeyboardInterrupt):
        with make_backend().session(ctx()):
            raise KeyboardInterrupt()
    assert builder.session.interrupted == ["metaflow-Flow-1-start-2"]


def test_the_session_is_left_running_by_default(builder):
    with make_backend().session(ctx()):
        pass
    assert not builder.session.stopped


def test_missing_databricks_connect_names_the_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.connect", None)
    monkeypatch.setattr(connect.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(SparkBackendUnavailable, match=r"metaflow-pyspark\[connect\]"):
        with make_backend().session(ctx()):
            pass


def test_a_pyspark_clash_is_explained(monkeypatch):
    monkeypatch.setitem(sys.modules, "databricks.connect", None)
    monkeypatch.setattr(connect.importlib.util, "find_spec", lambda name: object())
    with pytest.raises(SparkConfigError, match="pip uninstall -y pyspark"):
        with make_backend().session(ctx()):
            pass
