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
