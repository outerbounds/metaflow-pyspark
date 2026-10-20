from contextlib import contextmanager

import pytest

from metaflow_extensions.spark.plugins.decorator import (
    SparkDecorator,
    _collect_inputs,
    _resolve,
    _run_session,
)
from metaflow_extensions.spark.plugins.exceptions import (
    SparkConfigError,
    SparkException,
)
from metaflow_extensions.spark.plugins.output import (
    FORMATS,
    strip_spark_attrs,
    validate_format,
)


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for env in (
        "METAFLOW_SPARK_BACKEND",
        "DATABRICKS_CLUSTER_ID",
        "METAFLOW_DATABRICKS_CLUSTER_ID",
    ):
        monkeypatch.delenv(env, raising=False)


class Flow:
    """Stands in for a FlowSpec instance; attributes are set as steps would set them."""


def attrs(**overrides):
    merged = dict(SparkDecorator.defaults)
    merged.update(overrides)
    return merged


# ----------------------------------------------------------------------
def test_the_default_backend_is_local():
    assert _resolve(Flow(), attrs())["backend"] == "local"


def test_backend_attributes_land_in_the_backend_section():
    config = _resolve(Flow(), attrs(backend="databricks", cluster_id="0101-abc"))
    assert config["backend"] == "databricks"
    assert config["databricks"]["cluster_id"] == "0101-abc"


def test_backend_aliases_resolve_to_the_canonical_section():
    config = _resolve(Flow(), attrs(backend="databricks-connect", serverless=True))
    assert config["backend"] == "databricks"
    assert config["databricks"]["serverless"] is True


def test_the_environment_selects_the_backend(monkeypatch):
    monkeypatch.setenv("METAFLOW_SPARK_BACKEND", "databricks")
    assert _resolve(Flow(), attrs())["backend"] == "databricks"


def test_the_decorator_attribute_beats_the_environment(monkeypatch):
    monkeypatch.setenv("METAFLOW_SPARK_BACKEND", "databricks")
    assert _resolve(Flow(), attrs(backend="local"))["backend"] == "local"


def test_the_flow_config_artifact_selects_the_backend():
    flow = Flow()
    flow.spark_config = {"backend": "databricks", "databricks": {"cluster_id": "c-1"}}
    config = _resolve(flow, attrs())
    assert config["backend"] == "databricks"
    assert config["databricks"]["cluster_id"] == "c-1"


def test_spark_parameters_stay_backend_neutral():
    config = _resolve(
        Flow(), attrs(backend="local", spark_parameters={"spark.ui.enabled": "false"})
    )
    assert config["spark-parameters"] == {"spark.ui.enabled": "false"}


def test_unset_attributes_do_not_appear_in_the_section():
    config = _resolve(Flow(), attrs(backend="databricks"))
    assert "cluster_id" not in config["databricks"]
    assert "serverless" not in config["databricks"]


def test_an_unknown_backend_lists_the_valid_ones():
    with pytest.raises(SparkException, match="local"):
        _resolve(Flow(), attrs(backend="emr"))


# ----------------------------------------------------------------------
def test_job_parameters_are_read_off_the_flow():
    flow = Flow()
    flow.start_date = "2026-08-01"
    flow.region = "eu"
    assert _collect_inputs(flow, ["start_date", "region"]) == {
        "start_date": "2026-08-01",
        "region": "eu",
    }


def test_a_missing_job_parameter_names_the_attribute():
    with pytest.raises(SparkConfigError) as exc:
        _collect_inputs(Flow(), ["start_date"])
    assert "self.start_date" in str(exc.value)


def test_every_advertised_output_format_validates():
    for fmt in FORMATS:
        validate_format(fmt)


def test_an_unknown_output_format_lists_the_valid_ones():
    with pytest.raises(SparkConfigError, match="pandas"):
        validate_format("hdf5")


# ----------------------------------------------------------------------
def make_decorator(**kwargs):
    deco = SparkDecorator(attributes=kwargs)
    deco.step_init(None, None, "features", [], None, None, None)
    return deco


def test_a_non_callable_job_is_rejected_at_step_init():
    with pytest.raises(SparkConfigError, match="callable"):
        make_decorator(job="myjob.run")


def test_a_bad_output_format_is_rejected_at_step_init():
    with pytest.raises(SparkConfigError):
        make_decorator(output_format="hdf5")


def test_an_unknown_backend_is_rejected_at_step_init():
    with pytest.raises(SparkException, match="Unknown @spark backend"):
        make_decorator(backend="emr-serverless")


# ----------------------------------------------------------------------
class PlanMetrics:
    """Stands in for the pyspark object Spark Connect's toPandas() puts in attrs."""


PlanMetrics.__module__ = "pyspark.sql.connect.client.core"


def _connect_frame():
    pandas = pytest.importorskip("pandas")
    frame = pandas.DataFrame({"bucket": [0, 1]})
    frame.attrs["metrics"] = [PlanMetrics()]
    frame.attrs["source"] = "orders"
    return frame


class FakeDataFrame:
    def toPandas(self):
        return _connect_frame()


class FakeBackend:
    def __init__(self):
        self.session_obj = object()

    @contextmanager
    def session(self, ctx):
        yield self.session_obj


def test_pyspark_attrs_are_stripped_and_others_kept():
    frame = strip_spark_attrs(_connect_frame())
    assert frame.attrs == {"source": "orders"}


def test_the_session_is_on_the_flow_and_current_only_during_the_step():
    from metaflow import current

    backend = FakeBackend()
    flow = Flow()
    seen = {}

    def step_func():
        seen["flow"] = flow.spark
        seen["current"] = current.spark

    _run_session(backend, None, step_func, flow, attrs())
    assert seen == {"flow": backend.session_obj, "current": backend.session_obj}
    assert not hasattr(flow, "spark")
    assert current.spark is None


def test_the_session_is_detached_even_when_the_step_fails():
    flow = Flow()

    def step_func():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        _run_session(FakeBackend(), None, step_func, flow, attrs())
    assert not hasattr(flow, "spark")


def test_a_session_step_strips_pyspark_attrs_from_its_artifacts():
    flow = Flow()

    def step_func():
        flow.summary = _connect_frame()

    _run_session(FakeBackend(), None, step_func, flow, attrs())
    assert "metrics" not in flow.summary.attrs


def test_a_job_function_gets_the_session_and_its_parameters():
    backend = FakeBackend()
    flow = Flow()
    flow.cutoff = 5
    calls = []

    def job(spark, cutoff):
        calls.append((spark, cutoff))
        return FakeDataFrame()

    _run_session(
        backend, None, lambda: None, flow, attrs(job=job, job_parameters=["cutoff"])
    )
    assert calls == [(backend.session_obj, 5)]
    assert list(flow.spark_df.bucket) == [0, 1]
    assert "metrics" not in flow.spark_df.attrs
    # A job function never exposes the session as an attribute.
    assert not hasattr(flow, "spark")


def test_a_job_function_result_honours_output_format():
    flow = Flow()
    _run_session(
        FakeBackend(),
        None,
        lambda: None,
        flow,
        attrs(job=lambda spark: FakeDataFrame(), output_format="none"),
    )
    assert flow.spark_df is None
