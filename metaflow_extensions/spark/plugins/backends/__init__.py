"""Backend abstractions.

``SessionBackend``
    Hands a live ``SparkSession`` to an ``@spark`` step, which runs in the Metaflow
    task process. Spark operations execute in-process (local Spark) or on Databricks
    compute (Spark Connect).

``RemoteBackend``
    Submits remote work, polls it, and cancels it: a SQL statement
    (``StatementBackend``) or a packaged Spark job (``JobBackend``). Only the
    provider-specific calls live in a backend; polling, backoff, timeouts, cancellation,
    and error classification live here, so every backend gets the same behaviour.
"""

import signal
import threading
import time
from contextlib import contextmanager
from importlib import import_module

from ..context import JobState
from ..exceptions import (
    ControlPlaneError,
    QueryCancelled,
    QueryFailed,
    QueryTimeout,
    SparkException,
    SparkJobCancelled,
    SparkJobFailed,
    SparkJobTimeout,
)

POLL_INTERVAL_SECONDS = 5
STATUS_MESSAGE_INTERVAL_SECONDS = 60
CONTROL_PLANE_RETRIES = 6


class SessionBackend:
    #: registry key, e.g. "local"
    name = None

    def __init__(self, config, ctx=None):
        self.config = config
        self.ctx = ctx

    def session(self, ctx):
        """A context manager yielding a live SparkSession for the step's duration."""
        raise NotImplementedError


class RemoteBackend:
    #: identifies the backend in handles and log lines, e.g. "databricks-sql"
    name = None
    #: what the remote work is called in log lines and errors
    noun = "job"

    def __init__(self, config, ctx=None):
        self.config = config
        self.ctx = ctx

    def submit(self, ctx):
        """Start the remote work. Returns a JobHandle."""
        raise NotImplementedError

    def poll(self, handle):
        """Return the current JobStatus. May raise to signal a control-plane fault."""
        raise NotImplementedError

    def cancel(self, handle):
        """Best-effort cancellation of running work. Must not raise."""
        raise NotImplementedError

    def fetch_logs(self, handle, stream="stdout") -> "str | None":
        """Return the work's driver output on `stream`, or None if unavailable."""
        return None

    def read_output(self, handle, output_format):
        """Materialize the result in `output_format`."""
        raise NotImplementedError

    def failure_error(self, status, handle):
        raise NotImplementedError

    def cancelled_error(self, handle):
        raise NotImplementedError

    def timeout_error(self, timeout_minutes, handle):
        raise NotImplementedError

    # ------------------------------------------------------------------
    # shared driver
    # ------------------------------------------------------------------
    def wait(self, ctx, handle, timeout_minutes=None, poll_interval=None):
        """Poll until the work reaches a terminal state.

        Cancels it on interrupt, timeout, or any unexpected exception, so that killing a
        flow does not leave it running and billing.
        """
        poll_interval = poll_interval or POLL_INTERVAL_SECONDS
        deadline = None
        if timeout_minutes:
            deadline = time.monotonic() + timeout_minutes * 60

        last_message = 0.0
        last_state = None

        with _cancel_on_signal(self, ctx, handle):
            try:
                while True:
                    status = self._poll_with_retries(ctx, handle)

                    if status.state != last_state:
                        ctx.log("state: %s" % status.state, job_id=handle.job_id)
                        last_message = time.monotonic()
                        last_state = status.state

                    if status.terminal:
                        return status

                    now = time.monotonic()
                    if deadline and now > deadline:
                        ctx.log(
                            "timeout of %d minutes exceeded, cancelling"
                            % timeout_minutes,
                            job_id=handle.job_id,
                            stream="stderr",
                        )
                        self.safe_cancel(ctx, handle)
                        raise self.timeout_error(timeout_minutes, handle)

                    if now - last_message > STATUS_MESSAGE_INTERVAL_SECONDS:
                        ctx.log(
                            _progress_message(status, deadline, now),
                            job_id=handle.job_id,
                        )
                        last_message = now

                    time.sleep(poll_interval)
            except (KeyboardInterrupt, SystemExit):
                ctx.log(
                    "interrupted, cancelling", job_id=handle.job_id, stream="stderr"
                )
                self.safe_cancel(ctx, handle)
                raise
            except SparkException:
                # Already classified. Leave the work alone: a control-plane fault does
                # not mean it is unhealthy, and cancelling blind would be worse.
                raise
            except Exception:
                self.safe_cancel(ctx, handle)
                raise

    def _poll_with_retries(self, ctx, handle):
        """Poll, tolerating transient control-plane failures.

        A provider API outage is reported as ControlPlaneError, never as a failure, so
        users are not sent to debug code that ran fine.
        """
        last_exc = None
        for attempt in range(CONTROL_PLANE_RETRIES):
            try:
                return self.poll(handle)
            except (KeyboardInterrupt, SystemExit):
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < CONTROL_PLANE_RETRIES - 1:
                    backoff = min(2**attempt, 30)
                    ctx.log(
                        "could not read status (%s), retrying in %ds" % (exc, backoff),
                        job_id=handle.job_id,
                        stream="stderr",
                    )
                    time.sleep(backoff)
        raise ControlPlaneError(
            "Failed to read the status of %s %s after %d attempts. It may still be "
            "running.\nLast error: %s%s"
            % (
                self.noun,
                handle.job_id,
                CONTROL_PLANE_RETRIES,
                last_exc,
                "\nDetails: %s" % handle.ui_url if handle.ui_url else "",
            )
        ) from last_exc

    def safe_cancel(self, ctx, handle):
        try:
            self.cancel(handle)
            ctx.log("cancellation requested", job_id=handle.job_id)
        except Exception as exc:
            ctx.log(
                "failed to cancel: %s. It may still be running and billing: %s"
                % (exc, handle.ui_url or handle.job_id),
                job_id=handle.job_id,
                stream="stderr",
            )

    def run(self, ctx, crash_on_failure=True, show_stdout=False, show_stderr=False):
        """Submit, wait, surface logs, and classify the outcome."""
        handle = self.submit(ctx)
        ctx.log(
            "submitted %s%s"
            % (self.noun, " (%s)" % handle.ui_url if handle.ui_url else ""),
            job_id=handle.job_id,
        )
        try:
            status = self.wait(ctx, handle, timeout_minutes=ctx.timeout_minutes)
        finally:
            if show_stdout:
                self._show_logs(ctx, handle, "stdout")
            if show_stderr:
                self._show_logs(ctx, handle, "stderr")
        if status.state == JobState.CANCELLED:
            raise self.cancelled_error(handle)
        if not status.ok and crash_on_failure:
            raise self.failure_error(status, handle)
        return handle, status

    def _show_logs(self, ctx, handle, stream):
        try:
            text = self.fetch_logs(handle, stream)
        except Exception as exc:
            ctx.log(
                "could not retrieve %s: %s" % (stream, exc),
                job_id=handle.job_id,
                stream="stderr",
            )
            return
        if text:
            ctx.log(
                "%s %s\n-----\n%s\n-----" % (self.noun, stream, text.rstrip()),
                job_id=handle.job_id,
            )


class StatementBackend(RemoteBackend):
    """A SQL statement: errors are Query*, and the statement's error is the log."""

    noun = "statement"

    def failure_error(self, status, handle):
        return QueryFailed(status, handle=handle)

    def cancelled_error(self, handle):
        return QueryCancelled(
            "Statement %s was cancelled.%s"
            % (handle.job_id, "\n%s" % handle.ui_url if handle.ui_url else "")
        )

    def timeout_error(self, timeout_minutes, handle):
        return QueryTimeout(timeout_minutes, handle=handle)


class JobBackend(RemoteBackend):
    """A packaged Spark job: errors are SparkJob*, and carry the driver's output."""

    noun = "job"

    def failure_error(self, status, handle):
        return SparkJobFailed(
            status, handle=handle, logs=self.fetch_logs(handle, "stderr")
        )

    def cancelled_error(self, handle):
        return SparkJobCancelled(
            "Spark job %s was cancelled.%s"
            % (handle.job_id, "\n%s" % handle.ui_url if handle.ui_url else "")
        )

    def timeout_error(self, timeout_minutes, handle):
        return SparkJobTimeout(timeout_minutes, handle=handle)

    def cleanup(self, handle, ctx):
        """Remove anything staged for the job. Must not raise."""


def _progress_message(status, deadline, now):
    msg = "state: %s" % status.state
    if status.stages:
        done = sum(s.num_completed or 0 for s in status.stages)
        total = sum(s.num_tasks or 0 for s in status.stages)
        if total:
            msg += " (%d/%d tasks)" % (done, total)
    if deadline:
        msg += ", %d min until timeout" % max(0, int((deadline - now) / 60))
    return msg


@contextmanager
def _cancel_on_signal(backend, ctx, handle):
    """Translate SIGINT/SIGTERM into remote cancellation plus the normal exception.

    Only installs handlers on the main thread; `signal.signal` is not usable
    elsewhere, and Metaflow may run steps off the main thread.
    """
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    previous = {}

    def handler(signum, frame):
        backend.safe_cancel(ctx, handle)
        old = previous.get(signum)
        if callable(old):
            old(signum, frame)
        elif signum == signal.SIGINT:
            raise KeyboardInterrupt()
        else:
            raise SystemExit(128 + signum)

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[signum] = signal.signal(signum, handler)
        except (ValueError, OSError):
            previous.pop(signum, None)
    try:
        yield
    finally:
        for signum, old in previous.items():
            try:
                signal.signal(signum, old)
            except (ValueError, OSError):
                pass


# ----------------------------------------------------------------------
# @spark backend registry
# ----------------------------------------------------------------------
# Lazily resolved so that importing this package never pulls in pyspark, boto3, or
# databricks-connect. Values are "module:attribute" relative to this package; the
# attribute is a class, or a factory taking (config, ctx) that returns an instance.
_BACKENDS = {
    "local": ".local:LocalSparkBackend",
    "databricks": ".databricks:DatabricksBackend",
    "databricks-connect": ".databricks.connect:DatabricksConnectBackend",
    "databricks-jobs": ".databricks.jobs:DatabricksJobsBackend",
    "emr-serverless": ".emr_serverless:EMRServerlessBackend",
}

#: Accepted spellings for backend names.
_ALIASES = {
    "databricks_connect": "databricks-connect",
    "connect": "databricks-connect",
    "databricks_jobs": "databricks-jobs",
    "dbx": "databricks",
    "emr": "emr-serverless",
    "emr_serverless": "emr-serverless",
    "emrserverless": "emr-serverless",
}


def canonical_backend(name):
    """The registry key for `name`."""
    if not isinstance(name, str):
        raise SparkException(
            "@spark(backend=...) must be a string, got %r. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    key = _ALIASES.get(name, name)
    if key not in _BACKENDS:
        raise SparkException(
            "Unknown @spark backend '%s'. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    return key


def config_section(name):
    """The config section a backend reads: every Databricks variant shares one."""
    key = canonical_backend(name)
    return "databricks" if key.startswith("databricks") else key


def register_backend(name, target):
    """Register a backend. `target` is a class or a "module:attribute" string."""
    _BACKENDS[name] = target


def available_backends():
    return sorted(_BACKENDS)


def get_backend_class(name):
    key = canonical_backend(name)
    target = _BACKENDS[key]
    if isinstance(target, str):
        module_path, _, attr = target.partition(":")
        target = getattr(import_module(module_path, package=__name__), attr)
        _BACKENDS[key] = target
    return target
