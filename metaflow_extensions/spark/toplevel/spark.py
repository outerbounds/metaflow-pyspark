"""Names this extension adds to the `metaflow` namespace.

Loaded while `metaflow/__init__.py` is still executing, so nothing here may import
anything from `metaflow` at module level beyond `metaflow.exception`, and nothing here
may import pyspark, boto3, or the Databricks SDK.
"""

from ..plugins.catalog.unity import UnityCatalogTable, read_table
from ..plugins.exceptions import (
    ControlPlaneError,
    QueryCancelled,
    QueryFailed,
    QueryTimeout,
    SparkConfigError,
    SparkException,
    UnityCatalogError,
)

__all__ = [
    "UnityCatalogTable",
    "read_table",
    "SparkException",
    "SparkConfigError",
    "QueryFailed",
    "QueryCancelled",
    "QueryTimeout",
    "ControlPlaneError",
    "UnityCatalogError",
]
