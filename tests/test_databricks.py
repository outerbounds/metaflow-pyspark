import json
from types import SimpleNamespace

import pytest

from metaflow_extensions.spark.plugins.backends.databricks import (
    _BY_MODE,
    DEFAULT_MODE,
    jobs,
)
from metaflow_extensions.spark.plugins.backends.databricks.compute import (
    DEFAULT_RUNTIME_VERSION,
    EXISTING_CLUSTER,
    INSTANCE_POOL,
    NEW_CLUSTER,
    SERVERLESS,
    resolve_compute,
)
from metaflow_extensions.spark.plugins.backends.databricks.storage import (
    is_cloud_uri,
    is_volume_path,
    normalize_volume,
)
from metaflow_extensions.spark.plugins.context import JobState, TaskContext
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError
from metaflow_extensions.spark.plugins.packaging import CodePackage

terminal_state = jobs.DatabricksJobsBackend._terminal_state


# ----------------------------------------------------------------------
# run-state classification: cancellation, failure, and success are kept distinct
# ----------------------------------------------------------------------
def test_api_21_result_state_is_authoritative():
    assert terminal_state("TERMINATED", "SUCCESS", {}) == JobState.SUCCESS
    assert terminal_state("TERMINATED", "FAILED", {}) == JobState.FAILED
    assert terminal_state("TERMINATED", "CANCELED", {}) == JobState.CANCELLED
    assert terminal_state("TERMINATED", "TIMEDOUT", {}) == JobState.FAILED


def test_skipped_and_internal_error_are_failures():
    assert terminal_state("SKIPPED", None, {}) == JobState.FAILED
    assert terminal_state("INTERNAL_ERROR", None, {}) == JobState.FAILED


def test_api_22_termination_details():
    assert (
        terminal_state("TERMINATED", None, {"code": "SUCCESS", "type": "SUCCESS"})
        == JobState.SUCCESS
    )
    assert (
        terminal_state(
            "TERMINATED", None, {"code": "USER_CANCELED", "type": "CLIENT_ERROR"}
        )
        == JobState.CANCELLED
    )
    assert (
        terminal_state(
            "TERMINATED", None, {"code": "DRIVER_ERROR", "type": "CLIENT_ERROR"}
        )
        == JobState.FAILED
    )
    assert (
        terminal_state(
            "TERMINATED", None, {"code": "CLOUD_FAILURE", "type": "INTERNAL_ERROR"}
        )
        == JobState.FAILED
    )


def test_terminated_with_no_outcome_is_not_invented_as_a_failure():
    assert terminal_state("TERMINATED", None, {}) == JobState.SUCCESS


# ----------------------------------------------------------------------
# compute shapes
# ----------------------------------------------------------------------
def test_nothing_configured_means_serverless():
    spec = resolve_compute({})
    assert spec.kind == SERVERLESS
    assert spec.task_fields() == {"environment_key": "metaflow-spark"}
    assert spec.submit_fields()["environments"][0]["spec"] == {"client": "3"}


def test_existing_cluster_shorthand():
    spec = resolve_compute({"cluster_id": "0101-abc"})
    assert spec.kind == EXISTING_CLUSTER
    assert spec.task_fields() == {"existing_cluster_id": "0101-abc"}
    assert spec.submit_fields() == {}


def test_instance_pool_omits_node_type():
    spec = resolve_compute({"instance_pool_id": "pool-1", "num_workers": 8})
    assert spec.kind == INSTANCE_POOL
    assert "node_type_id" not in spec.new_cluster
    assert spec.new_cluster["instance_pool_id"] == "pool-1"
    assert spec.new_cluster["num_workers"] == 8


def test_new_cluster_defaults_and_photon():
    spec = resolve_compute(
        {"runtime_version": "14.3.x-scala2.12", "photon": True}, cloud="azure"
    )
    assert spec.kind == NEW_CLUSTER
    assert spec.new_cluster["spark_version"] == "14.3.x-scala2.12"
    assert spec.new_cluster["node_type_id"] == "Standard_DS3_v2"
    assert spec.new_cluster["runtime_engine"] == "PHOTON"
    assert spec.new_cluster["data_security_mode"] == "SINGLE_USER"


def test_node_type_alone_builds_a_new_cluster():
    spec = resolve_compute({"node_type_id": "r5.4xlarge"})
    assert spec.kind == NEW_CLUSTER
    assert spec.new_cluster["spark_version"] == DEFAULT_RUNTIME_VERSION


def test_conflicting_compute_is_rejected():
    with pytest.raises(SparkConfigError, match="Conflicting compute"):
        resolve_compute({"serverless": True, "cluster_id": "0101-abc"})


def test_tags_land_on_the_new_cluster():
    spec = resolve_compute({"node_type_id": "r5.xlarge"}, tags={"metaflow_flow": "F"})
    assert spec.new_cluster["custom_tags"]["metaflow_flow"] == "F"


def test_spark_parameters_become_spark_conf():
    spec = resolve_compute(
        {
            "node_type_id": "r5.xlarge",
            "spark-parameters": {"spark.sql.shuffle.partitions": 200},
        }
    )
    assert spec.new_cluster["spark_conf"]["spark.sql.shuffle.partitions"] == "200"


def test_unknown_compute_string_is_rejected():
    with pytest.raises(SparkConfigError, match="not understood"):
        resolve_compute({"compute": "gpu"})


# ----------------------------------------------------------------------
# storage paths
# ----------------------------------------------------------------------
def test_volume_and_cloud_paths_are_distinguished():
    assert is_volume_path("/Volumes/main/default/staging")
    assert not is_volume_path("s3://bucket/key")
    assert is_cloud_uri("s3://bucket/key")
    assert is_cloud_uri("abfss://c@a.dfs.core.windows.net/p")
    assert not is_cloud_uri("/Volumes/main/default/staging")


def test_normalize_volume_strips_trailing_slashes():
    assert normalize_volume("/Volumes/main/default/staging/") == (
        "/Volumes/main/default/staging"
    )


def test_dbfs_style_volume_path_is_accepted():
    assert normalize_volume("dbfs:/Volumes/main/default/s") == "/Volumes/main/default/s"


# ----------------------------------------------------------------------
# mode dispatch
# ----------------------------------------------------------------------
def test_every_mode_maps_to_a_backend():
    assert DEFAULT_MODE in _BY_MODE
    for mode, backend in _BY_MODE.items():
        assert callable(backend), mode


# ----------------------------------------------------------------------
# what actually goes over the wire, against a recording client
# ----------------------------------------------------------------------


class RecordingClient:
    host = "https://adb-1.azuredatabricks.net"

    def __init__(self):
        self.calls = []
        self.uploaded = {}
        self.workspace_files = {}
        files = SimpleNamespace(
            upload=lambda path, stream, overwrite: self.uploaded.update(
                {path: stream.read()}
            ),
            create_directory=lambda path: None,
            delete=lambda path: self.uploaded.pop(path, None),
        )
        workspace = SimpleNamespace(
            mkdirs=lambda path: None,
            upload=lambda path, stream, format, overwrite: self.workspace_files.update(
                {path: stream.read()}
            ),
            delete=lambda path, recursive: [
                self.workspace_files.pop(p)
                for p in list(self.workspace_files)
                if p.startswith(path + "/")
            ],
        )
        current_user = SimpleNamespace(
            me=lambda: SimpleNamespace(user_name="philipp@example.com")
        )
        self.sdk = SimpleNamespace(
            files=files, workspace=workspace, current_user=current_user
        )

    def api(self, method, path, body=None, query=None):
        self.calls.append((method, path, body, query))
        if path.endswith("/runs/submit"):
            return {"run_id": 77}
        return {"run_page_url": "https://adb-1/runs/77"}

    def run_url(self, run_id):
        return "%s/jobs/runs/%s" % (self.host, run_id)

    def spark_ui_url(self, cluster_id):
        return None


def task_ctx(**kwargs):
    base = dict(
        step_name="features",
        pathspec="RetailFlow/42/features/7",
        flow_name="RetailFlow",
        run_id="42",
        task_id="7",
        attempt=0,
        user="philipp",
        tags={"metaflow_flow": "RetailFlow"},
        timeout_minutes=30,
        logger=lambda *a, **k: None,
    )
    base.update(kwargs)
    return TaskContext(**base)


def make_backend(cls, config, client):
    backend = cls.__new__(cls)
    backend.config = config
    backend.ctx = None
    backend.client = client
    backend.storage = jobs.DatabricksStorage(client)
    return backend


def summarize(spark):
    return None


@pytest.fixture
def fake_package(monkeypatch):
    package = CodePackage(b"tarball", "etl.jobs", ["etl"])
    monkeypatch.setattr(jobs, "build_package", lambda *a, **k: package)
    return package


def submitted(client):
    return next(body for _, path, body, _ in client.calls if path.endswith("/submit"))


def test_a_job_is_staged_on_the_volume_and_submitted(fake_package):
    client = RecordingClient()
    backend = make_backend(
        jobs.DatabricksJobsBackend,
        {"volume": "/Volumes/main/mf/staging/", "serverless": True},
        client,
    )
    handle = backend.submit(task_ctx(job_func=summarize, inputs={"cutoff": 5}))

    staging = "/Volumes/main/mf/staging/metaflow/RetailFlow/42/features/7"
    assert set(client.uploaded) == {
        staging + "/code.tar.gz",
        staging + "/inputs.pickle",
    }
    # Serverless cannot run a python_file from a Volume.
    driver = (
        "/Users/philipp@example.com/metaflow/RetailFlow/42/features/7/remote_driver.py"
    )
    assert set(client.workspace_files) == {driver}
    payload = submitted(client)
    assert payload["tasks"][0]["spark_python_task"]["python_file"] == driver
    # runs/submit has no run-level tags; they only reach billing on a job cluster.
    assert "custom_tags" not in payload
    assert payload["timeout_seconds"] == 1800
    assert payload["tasks"][0]["environment_key"] == "metaflow-spark"
    assert handle.job_id == "77"
    assert handle.ui_url == "https://adb-1/runs/77"


def test_a_job_needs_a_volume_to_stage_on(fake_package):
    backend = make_backend(jobs.DatabricksJobsBackend, {}, RecordingClient())
    with pytest.raises(SparkConfigError, match="METAFLOW_DATABRICKS_VOLUME"):
        backend.submit(task_ctx(job_func=summarize))


def test_serverless_rejects_cluster_libraries(fake_package):
    backend = make_backend(
        jobs.DatabricksJobsBackend,
        {"volume": "/Volumes/a/b/c", "serverless": True, "libraries": [{"pypi": {}}]},
        RecordingClient(),
    )
    with pytest.raises(SparkConfigError, match="dependencies"):
        backend.submit(task_ctx(job_func=summarize))


@pytest.mark.parametrize(
    "host, cloud",
    [
        ("https://adb-1.4.azuredatabricks.net", "azure"),
        ("https://1.2.gcp.databricks.com", "gcp"),
        ("https://dbc-1.cloud.databricks.com", "aws"),
        (None, "aws"),
    ],
)
def test_the_cloud_is_inferred_from_the_workspace_host(host, cloud):
    assert jobs.cloud_from_host(host) == cloud


def test_a_new_cluster_gets_a_node_type_for_the_workspace_cloud(fake_package):
    client = RecordingClient()
    backend = make_backend(
        jobs.DatabricksJobsBackend,
        {"volume": "/Volumes/a/b/c", "runtime_version": "15.4.x-scala2.12"},
        client,
    )
    backend.submit(task_ctx(job_func=summarize))
    cluster = submitted(client)["tasks"][0]["new_cluster"]
    assert cluster["node_type_id"] == "Standard_DS3_v2"


def test_cleanup_removes_the_staged_code(fake_package):
    client = RecordingClient()
    backend = make_backend(
        jobs.DatabricksJobsBackend, {"volume": "/Volumes/a/b/c"}, client
    )
    handle = backend.submit(task_ctx(job_func=summarize))
    backend.cleanup(handle, None)
    assert client.uploaded == {}
    assert client.workspace_files == {}


def test_a_failed_job_shows_the_marker_traceback_alone():
    client = RecordingClient()
    backend = make_backend(jobs.DatabricksJobsBackend, {}, client)
    backend.storage.read_text = lambda path: json.dumps(
        {"ok": False, "error": "Traceback ...\nValueError: bad input"}
    )
    handle = SimpleNamespace(extra={"result_path": "/Volumes/a/b/c/result.json"})
    assert (
        backend.fetch_logs(handle, "stderr") == "Traceback ...\nValueError: bad input"
    )
    assert not client.calls


def test_workspace_dir_overrides_where_the_driver_goes(fake_package):
    client = RecordingClient()
    backend = make_backend(
        jobs.DatabricksJobsBackend,
        {"volume": "/Volumes/a/b/c", "workspace_dir": "/Shared/metaflow"},
        client,
    )
    backend.submit(task_ctx(job_func=summarize))
    assert set(client.workspace_files) == {
        "/Shared/metaflow/RetailFlow/42/features/7/remote_driver.py"
    }


def test_table_output_returns_a_unity_catalog_reference(monkeypatch):
    import metaflow_extensions.spark.plugins.catalog.unity as unity

    created = []

    class FakeTable:
        def __init__(self, name, client=None, config=None):
            created.append(name)

    monkeypatch.setattr(unity, "UnityCatalogTable", FakeTable)
    backend = make_backend(jobs.DatabricksJobsBackend, {}, RecordingClient())
    handle = SimpleNamespace(extra={"output_table": "main.ml.features"})
    assert isinstance(backend.read_output(handle, "table"), FakeTable)
    assert backend.read_output(handle, "url") == "main.ml.features"
    assert created == ["main.ml.features"]
