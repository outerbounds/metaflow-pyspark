import tarfile
import textwrap

import pytest

from metaflow_extensions.spark.plugins.context import TaskContext
from metaflow_extensions.spark.plugins.exceptions import SparkConfigError
from metaflow_extensions.spark.plugins.packaging import (
    build_package,
    pickle_inputs,
    resolve_job_module,
)


def test_job_name_is_derived_from_the_pathspec():
    ctx = TaskContext(
        step_name="features",
        pathspec="RetailFlow/42/features/7",
        flow_name="RetailFlow",
        run_id="42",
        task_id="7",
        attempt=0,
        user=None,
        tags={},
    )
    assert ctx.job_name == "metaflow-RetailFlow-42-features-7"


# ----------------------------------------------------------------------
def write_package(tmp_path):
    """A two-module package, which the original single-file driver could not ship."""
    pkg = tmp_path / "myjobs"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "helpers.py").write_text("def clean(df):\n    return df\n")
    (pkg / "etl.py").write_text(
        textwrap.dedent(
            """
            from .helpers import clean

            def run(spark, **kwargs):
                return clean(None)
            """
        )
    )
    return pkg


def test_package_ships_the_whole_package_not_just_the_job_file(tmp_path, monkeypatch):
    import sys

    write_package(tmp_path)
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("myjobs", None)
    sys.modules.pop("myjobs.etl", None)
    from myjobs import etl

    package = build_package(etl.run)
    assert package.module_name == "myjobs.etl"

    import io

    with tarfile.open(fileobj=io.BytesIO(package.blob), mode="r:gz") as tar:
        names = set(tar.getnames())
    assert "myjobs/etl.py" in names
    assert "myjobs/helpers.py" in names


def test_include_adds_extra_paths_at_their_basename(tmp_path, monkeypatch):
    import io
    import sys

    write_package(tmp_path)
    extra = tmp_path / "lookup.json"
    extra.write_text("{}")
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("myjobs", None)
    sys.modules.pop("myjobs.etl", None)
    from myjobs import etl

    package = build_package(etl.run, include=[str(extra)])
    with tarfile.open(fileobj=io.BytesIO(package.blob), mode="r:gz") as tar:
        assert "lookup.json" in set(tar.getnames())


def test_pycache_is_excluded(tmp_path, monkeypatch):
    import io
    import sys

    pkg = write_package(tmp_path)
    cache = pkg / "__pycache__"
    cache.mkdir()
    (cache / "etl.cpython-314.pyc").write_bytes(b"\x00")
    monkeypatch.syspath_prepend(str(tmp_path))
    sys.modules.pop("myjobs", None)
    sys.modules.pop("myjobs.etl", None)
    from myjobs import etl

    package = build_package(etl.run)
    with tarfile.open(fileobj=io.BytesIO(package.blob), mode="r:gz") as tar:
        names = tar.getnames()
    assert not any("__pycache__" in n for n in names)


def test_a_lambda_cannot_be_packaged():
    with pytest.raises(SparkConfigError):
        resolve_job_module(eval("lambda spark: None", {"__builtins__": {}}))


def test_unpicklable_parameters_explain_themselves():
    with pytest.raises(SparkConfigError, match="picklable"):
        pickle_inputs({"session": lambda: None})


def test_picklable_parameters_round_trip():
    import pickle

    assert pickle.loads(pickle_inputs({"date": "2026-08-24"})) == {"date": "2026-08-24"}
