import os

import pytest

from metaflow_extensions.spark.plugins.backends import local
from metaflow_extensions.spark.plugins.backends.local import _ensure_java_home


def _make_java(root):
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    (bin_dir / "java").write_text("")


@pytest.fixture
def no_java(monkeypatch):
    monkeypatch.delenv("JAVA_HOME", raising=False)


@pytest.mark.parametrize("layout", [("lib", "jvm"), ()])
def test_java_home_found_in_environment_prefix(tmp_path, no_java, layout):
    home = tmp_path.joinpath(*layout)
    _make_java(home)
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == str(home)


def test_java_home_prefers_conda_jvm_dir(tmp_path, no_java):
    _make_java(tmp_path / "lib" / "jvm")
    _make_java(tmp_path)
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == str(tmp_path / "lib" / "jvm")


def test_existing_java_home_is_kept(tmp_path, monkeypatch):
    monkeypatch.setenv("JAVA_HOME", "/opt/jdk")
    _make_java(tmp_path / "lib" / "jvm")
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == "/opt/jdk"


def test_the_environment_jdk_beats_java_on_path(tmp_path, no_java, monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    _make_java(tmp_path / "lib" / "jvm")
    _ensure_java_home(str(tmp_path))
    assert os.environ["JAVA_HOME"] == str(tmp_path / "lib" / "jvm")


def test_no_jdk_leaves_java_home_unset(tmp_path, no_java):
    _ensure_java_home(str(tmp_path))
    assert "JAVA_HOME" not in os.environ


# ----------------------------------------------------------------------
# a real local session, when this environment can start one
# ----------------------------------------------------------------------
def _can_run_local_spark():
    import importlib.util

    if importlib.util.find_spec("pyspark") is None:
        return False
    import subprocess

    _ensure_java_home()
    java = os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")
    try:
        # `java` on PATH may be a stub that only prompts for an install.
        return (
            subprocess.run(
                [java if os.path.exists(java) else "java", "-version"],
                capture_output=True,
            ).returncode
            == 0
        )
    except OSError:
        return False


@pytest.mark.skipif(not _can_run_local_spark(), reason="needs pyspark and a JDK")
def test_a_real_local_session_runs_and_is_stopped():
    from types import SimpleNamespace

    ctx = SimpleNamespace(pathspec="Flow/1/start/2", log=lambda *a, **k: None)
    backend = local.LocalSparkBackend(
        {"master": "local[1]", "spark-parameters": {"spark.ui.enabled": "false"}}
    )
    with backend.session(ctx) as session:
        assert session.range(10).count() == 10
        assert session.conf.get("spark.ui.enabled") == "false"
        assert session.sparkContext.appName == "metaflow-Flow-1-start-2"
    assert session.sparkContext._jsc is None
