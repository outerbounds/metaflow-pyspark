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
    """The remote work may or may not be running: we lost the ability to ask.

    Deliberately distinct from QueryFailed and SparkJobFailed so that a transient outage
    in the provider's API is never reported to the user as a failure.
    """

    headline = "Lost contact with the control plane"


class SparkJobFailed(SparkException):
    """A submitted Spark job reached a terminal failure state.

    Carries the backend's own error classification and the tail of the driver output,
    so the user does not have to go log spelunking to find out what happened.
    """

    headline = "Spark job failed"

    def __init__(self, status, handle=None, logs=None):
        self.status = status
        self.handle = handle
        lines = ["Spark job failed with state '%s'." % status.state]
        if status.error_class:
            lines.append("Error class: %s" % status.error_class)
        if status.message:
            lines.append("Message: %s" % status.message)
        ui_url = status.ui_url or (handle.ui_url if handle is not None else None)
        if ui_url:
            lines.append("Run details: %s" % ui_url)
        if status.spark_ui_url:
            lines.append("Spark UI: %s" % status.spark_ui_url)
        if logs:
            tail = logs.strip().splitlines()[-40:]
            lines.append("\nLast %d lines of output:" % len(tail))
            lines.extend("    " + line for line in tail)
        super().__init__("\n".join(lines))


class SparkJobCancelled(SparkException):
    headline = "Spark job cancelled"


class SparkJobTimeout(SparkException):
    headline = "Spark job timed out"

    def __init__(self, timeout_minutes, handle=None):
        self.handle = handle
        msg = "Spark job did not finish within the %d minute timeout." % timeout_minutes
        if handle is not None and handle.ui_url:
            msg += "\nRun details: %s" % handle.ui_url
        super().__init__(msg)


class UnityCatalogError(SparkException):
    headline = "Unity Catalog error"
