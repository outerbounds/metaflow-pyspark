"""Integration tests that run real Metaflow flows in a subprocess.

The backend is a stand-in, so no cloud account is needed, but everything between the
decorator and Metaflow's task lifecycle is the real thing.
"""

import os
import subprocess
import sys

FLOW_DIR = os.path.join(os.path.dirname(__file__), "flows")
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def run_flow(name, datastore):
    env = dict(os.environ)
    env.update(
        {
            # Keep the test off any configured metadata service.
            "METAFLOW_DEFAULT_METADATA": "local",
            "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(datastore),
            "METAFLOW_USER": "spark-tests",
            "PYTHONPATH": REPO_ROOT + os.pathsep + env.get("PYTHONPATH", ""),
        }
    )
    return subprocess.run(
        [sys.executable, os.path.join(FLOW_DIR, name), "--no-pylint", "run"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(datastore),
        timeout=600,
    )


def test_a_session_step_persists_its_artifacts_without_the_session(tmp_path):
    result = run_flow("session_flow.py", tmp_path)
    assert "session flow ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0


def test_a_submitted_job_sets_its_output_and_records_metadata(tmp_path):
    result = run_flow("submit_flow.py", tmp_path)
    assert "submit flow ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0
    # The remote URLs are surfaced in the task log, not buried in metadata only.
    assert "https://example.invalid/runs/fake-run-1" in result.stdout


def test_a_failed_job_fails_the_step_with_the_remote_error(tmp_path):
    result = run_flow("failure_flow.py", tmp_path)
    assert result.returncode != 0
    combined = result.stdout + result.stderr
    assert "Spark job failed" in combined
    assert "AnalysisException" in combined
    assert "the step body must not run" not in combined


def test_crash_on_failure_false_lets_the_step_continue(tmp_path):
    result = run_flow("tolerated_failure_flow.py", tmp_path)
    assert "tolerated failure ok" in result.stdout, result.stdout + result.stderr
    assert result.returncode == 0
