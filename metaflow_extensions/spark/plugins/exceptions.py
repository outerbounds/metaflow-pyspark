from metaflow.exception import MetaflowException


class SparkException(MetaflowException):
    headline = "Spark extension error"


class SparkConfigError(SparkException):
    headline = "Spark extension configuration error"


class SparkBackendUnavailable(SparkException):
    """Raised when a backend is selected but its dependencies are not installed."""

    headline = "Spark extension backend unavailable"

    def __init__(self, backend, package, extra=None):
        extra = extra or backend
        msg = (
            "The '%s' backend requires '%s', which is not installed.\n"
            "Install it with:\n"
            "    pip install metaflow-pyspark[%s]\n"
            "or add it to the step environment with @pypi(packages={'%s': ''})."
            % (backend, package, extra, package)
        )
        super().__init__(msg)


class QueryFailed(SparkException):
    """A SQL statement reached a terminal failure state.

    Carries the warehouse's own error class and message.
    """

    headline = "SQL statement failed"

    def __init__(self, status, handle=None):
        self.status = status
        self.handle = handle
        lines = ["Statement failed with state '%s'." % status.state]
        if status.error_class:
            lines.append("Error class: %s" % status.error_class)
        if status.message:
            lines.append("Message: %s" % status.message)
        if status.ui_url:
            lines.append("Warehouse: %s" % status.ui_url)
        super().__init__("\n".join(lines))


class QueryCancelled(SparkException):
    headline = "SQL statement cancelled"


class QueryTimeout(SparkException):
    headline = "SQL statement timed out"

    def __init__(self, timeout_minutes, handle=None):
        self.handle = handle
        msg = "Statement did not finish within the %d minute timeout." % timeout_minutes
        if handle is not None and handle.ui_url:
            msg += "\nWarehouse: %s" % handle.ui_url
        super().__init__(msg)


class ControlPlaneError(SparkException):
    """The statement may or may not be running: we lost the ability to ask.

    Deliberately distinct from QueryFailed so that a transient outage in the
    provider's API is never reported to the user as a failed statement.
    """

    headline = "Lost contact with the Databricks control plane"


class UnityCatalogError(SparkException):
    headline = "Unity Catalog error"
