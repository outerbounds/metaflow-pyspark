"""Databricks backends.

`mode` selects how an `@spark(backend="databricks")` step uses Databricks:

    connect   remote Spark, local Python. The default: nothing to package, no
              serialization boundary, and Unity Catalog governance applies as-is.
    job       package the code and run it as a one-off Databricks job. For a pinned
              runtime, Photon, cluster libraries, or a driver on the cluster.

Existing jobs and notebooks are run by `@databricks_job` and `@databricks_notebook`.
"""

from ...exceptions import SparkConfigError
from .client import DatabricksClient
from .connect import DatabricksConnectBackend
from .jobs import (
    DatabricksExistingJobBackend,
    DatabricksJobsBackend,
    DatabricksNotebookBackend,
)
from .sql import DatabricksSqlBackend

CONNECT_MODES = ("connect", "spark-connect", "serverless-connect")
JOB_MODES = ("job", "jobs", "submit", "spark_python_task")

DEFAULT_MODE = "connect"

_BY_MODE = {}
_BY_MODE.update({m: DatabricksConnectBackend for m in CONNECT_MODES})
_BY_MODE.update({m: DatabricksJobsBackend for m in JOB_MODES})


def DatabricksBackend(config, ctx=None):
    """Instantiate the Databricks backend selected by `config['mode']`."""
    mode = (config.get("mode") or DEFAULT_MODE).lower()
    backend_class = _BY_MODE.get(mode)
    if backend_class is None:
        raise SparkConfigError(
            "Unknown Databricks mode %r. Choose one of: connect (remote Spark, local "
            "Python) or job (package and submit). Existing jobs and notebooks are run "
            "with @databricks_job and @databricks_notebook." % mode
        )
    return backend_class(config, ctx)


__all__ = [
    "DatabricksBackend",
    "DatabricksClient",
    "DatabricksConnectBackend",
    "DatabricksExistingJobBackend",
    "DatabricksJobsBackend",
    "DatabricksNotebookBackend",
    "DatabricksSqlBackend",
]
