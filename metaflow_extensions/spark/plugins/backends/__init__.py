"""Backend abstractions.

``SessionBackend``
    Hands a live ``SparkSession`` to an ``@spark`` step, which runs in the Metaflow
    task process. Spark operations execute in-process (local Spark) or on Databricks
    compute (Spark Connect).

``StatementBackend``
    Submits a SQL statement, polls it, and cancels it. Only the provider-specific calls
    live in a backend; polling, backoff, timeouts, cancellation, and error
    classification live here.
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


class StatementBackend:
    #: identifies the backend in handles and log lines, e.g. "databricks-sql"
    name = None

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
        """Best-effort cancellation of a running statement. Must not raise."""
        raise NotImplementedError

    def read_output(self, handle, output_format):
        """Materialize the statement's result in `output_format`."""
        raise NotImplementedError

    # ------------------------------------------------------------------
    # shared driver
    # ------------------------------------------------------------------
    def wait(self, ctx, handle, timeout_minutes=None, poll_interval=None):
        """Poll until the statement reaches a terminal state.

        Cancels the statement on interrupt, timeout, or any unexpected exception, so
        that killing a flow does not leave it running and billing.
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
                        raise QueryTimeout(timeout_minutes, handle=handle)

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
                # Already classified. Leave the statement alone: a control-plane fault
                # does not mean it is unhealthy, and cancelling blind would be worse.
                raise
            except Exception:
                self.safe_cancel(ctx, handle)
                raise

    def _poll_with_retries(self, ctx, handle):
        """Poll, tolerating transient control-plane failures.

        A provider API outage is reported as ControlPlaneError, never as a failed
        statement, so users are not sent to debug SQL that ran fine.
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
            "Failed to read the status of statement %s after %d attempts. It may still "
            "be running.\nLast error: %s%s"
            % (
                handle.job_id,
                CONTROL_PLANE_RETRIES,
                last_exc,
                "\nWarehouse: %s" % handle.ui_url if handle.ui_url else "",
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

    def run(self, ctx, crash_on_failure=True):
        """Submit, wait, and classify the outcome."""
        handle = self.submit(ctx)
        ctx.log(
            "submitted%s" % (" (%s)" % handle.ui_url if handle.ui_url else ""),
            job_id=handle.job_id,
        )
        status = self.wait(ctx, handle, timeout_minutes=ctx.timeout_minutes)
        if status.state == JobState.CANCELLED:
            raise QueryCancelled(
                "Statement %s was cancelled.%s"
                % (handle.job_id, "\n%s" % handle.ui_url if handle.ui_url else "")
            )
        if not status.ok and crash_on_failure:
            raise QueryFailed(status, handle=handle)
        return handle, status


def _progress_message(status, deadline, now):
    msg = "state: %s" % status.state
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
# session backend registry
# ----------------------------------------------------------------------
# Lazily resolved so that importing this package never pulls in pyspark or
# databricks-connect. Values are "module:attribute" relative to this package.
_SESSION_BACKENDS = {
    "local": ".local:LocalSparkBackend",
    "databricks": ".databricks.connect:DatabricksConnectBackend",
}

#: Accepted spellings for backend names.
_ALIASES = {
    "databricks-connect": "databricks",
    "databricks_connect": "databricks",
    "connect": "databricks",
    "dbx": "databricks",
}


def canonical_backend(name):
    """The registry key for `name`, which is also its config section."""
    if not isinstance(name, str):
        raise SparkException(
            "@spark(backend=...) must be a string, got %r. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    key = _ALIASES.get(name, name)
    if key not in _SESSION_BACKENDS:
        raise SparkException(
            "Unknown @spark backend '%s'. Available backends: %s."
            % (name, ", ".join(available_backends()))
        )
    return key


def register_backend(name, target):
    """Register a session backend. `target` is a class or a "module:attribute" string."""
    _SESSION_BACKENDS[name] = target


def available_backends():
    return sorted(_SESSION_BACKENDS)


def get_backend_class(name):
    key = canonical_backend(name)
    target = _SESSION_BACKENDS[key]
    if isinstance(target, str):
        module_path, _, attr = target.partition(":")
        target = getattr(import_module(module_path, package=__name__), attr)
        _SESSION_BACKENDS[key] = target
    return target
