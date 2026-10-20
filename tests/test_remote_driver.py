"""The packaged-job round trip, end to end on local Spark.

Builds a code package from a two-module package, stages it and the pickled parameters
on local paths, and runs `remote_driver.py` in a subprocess exactly as a cluster would.
Skipped unless pyspark and a JDK are available.
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

from metaflow_extensions.spark.plugins import remote_driver
from metaflow_extensions.spark.plugins.backends.local import _ensure_java_home
from metaflow_extensions.spark.plugins.packaging import build_package, pickle_inputs


def _can_run_local_spark():
    import importlib.util

    if importlib.util.find_spec("pyspark") is None:
        return False
    _ensure_java_home()
    java = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")
    try:
        return (
            subprocess.run(
                [java if os.path.exists(java) else "java", "-version"],
                capture_output=True,
            ).returncode
            == 0
        )
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _can_run_local_spark(), reason="needs pyspark and a JDK"
)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    pkg = tmp_path / "src" / "driverjobs"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text("")
    (pkg / "helpers.py").write_text(
        textwrap.dedent(
            """
            def buckets(df, n):
                return df.selectExpr("id %% %d AS bucket" % n).groupBy("bucket").count()
            """
        )
    )
    (pkg / "etl.py").write_text(
        textwrap.dedent(
            """
            from .helpers import buckets

            def run(spark, rows, n):
                return buckets(spark.range(rows), n)

            def broken(spark):
                raise ValueError("the job itself failed")
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path / "src"))
    for name in ("driverjobs", "driverjobs.etl", "driverjobs.helpers"):
        sys.modules.pop(name, None)
    from driverjobs import etl

    stage = tmp_path / "stage"
    stage.mkdir()
    (stage / "code.tar.gz").write_bytes(build_package(etl.run).blob)
    (stage / "inputs.pickle").write_bytes(pickle_inputs({"rows": 100, "n": 4}))
    return stage


def run_driver(stage, func_name, inputs=True):
    conf = {
        "package_path": str(stage / "code.tar.gz"),
        "inputs_path": str(stage / "inputs.pickle") if inputs else None,
        "module_name": "driverjobs.etl",
        "func_name": func_name,
        "output_path": str(stage / "out"),
        "result_path": str(stage / "result.json"),
        "spark_parameters": {"spark.ui.enabled": "false"},
        "app_name": "metaflow-test",
    }
    # Run from an unrelated directory so only the extracted package is importable.
    env = {k: v for k, v in os.environ.items() if k != "PYTHONPATH"}
    return subprocess.run(
        [sys.executable, remote_driver.__file__, json.dumps(conf)],
        capture_output=True,
        text=True,
        cwd=str(stage),
        env=env,
        timeout=300,
    )


def test_a_packaged_job_runs_and_writes_its_output(staged):
    import pyarrow.dataset as ds

    result = run_driver(staged, "run")
    assert result.returncode == 0, result.stdout + result.stderr
    marker = json.loads((staged / "result.json").read_text())
    assert marker["ok"] and marker["wrote"] == "parquet"
    table = ds.dataset(str(staged / "out"), format="parquet").to_table()
    assert sorted(table.column("count").to_pylist()) == [25, 25, 25, 25]


def test_a_failing_job_reports_its_traceback_in_the_marker(staged):
    result = run_driver(staged, "broken", inputs=False)
    assert result.returncode == 1
    marker = json.loads((staged / "result.json").read_text())
    assert not marker["ok"]
    assert "the job itself failed" in marker["error"]
