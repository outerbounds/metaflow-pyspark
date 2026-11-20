"""Databricks Connect: remote Spark, local Python.

The step keeps running wherever Metaflow put it and gets a ``SparkSession`` bound to
Databricks compute. Only the query plan crosses the wire.

There is no code package, since nothing but the plan is shipped, and no pickle round
trip, since the step's own local variables are already in the right process. Unity
Catalog governance is preserved by construction: every read is executed by Databricks
under the caller's UC credentials, so existing grants apply with nothing new to
configure.
"""

import importlib.util
from contextlib import contextmanager

from .. import SessionBackend
from ...exceptions import SparkBackendUnavailable, SparkConfigError
from .client import DatabricksClient


class DatabricksConnectBackend(SessionBackend):
    name = "databricks-connect"

    def __init__(self, config, ctx=None):
        super().__init__(config, ctx)
        self.client = DatabricksClient(config)

    @contextmanager
    def session(self, ctx):
        builder = self._builder(ctx)
        session = builder.getOrCreate()

        tag = _session_tag(ctx)
        tagged = _add_tag(session, tag)
        _describe_job(session, ctx)
        ctx.log(
            "connected to Databricks (%s)%s"
            % (self._target_description(), ", session tag %s" % tag if tagged else "")
        )

        try:
            yield session
        except (KeyboardInterrupt, SystemExit):
            # Interrupt the in-flight query rather than abandoning it. Without this the
            # cluster keeps computing a result nobody is waiting for.
            if tagged:
                _interrupt(session, tag, ctx)
            raise
        finally:
            if self.config.get("stop_session", False):
                try:
                    session.stop()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    def _builder(self, ctx):
        try:
            from databricks.connect import DatabricksSession
        except ImportError as exc:
            raise _connect_unavailable(exc)

        config = self.config
        builder = DatabricksSession.builder

        serverless = config.get("serverless")
        cluster_id = config.get("cluster_id")
        if serverless and cluster_id:
            raise SparkConfigError(
                "@spark(serverless=True) and a cluster_id are mutually exclusive. "
                "Pick one."
            )
        if serverless is None and not cluster_id:
            # Serverless needs no cluster to exist and no warm-up, which makes it the
            # right default for a step that just wants a session.
            serverless = True

        if serverless:
            if not hasattr(builder, "sdkConfig") and not hasattr(builder, "serverless"):
                raise SparkConfigError(
                    "The installed databricks-connect does not support serverless "
                    "compute. Upgrade it, or set a cluster_id."
                )
            # Serverless resolves auth from the environment or the SDK config below.
            # Do not also call builder.serverless(True): recent databricks-connect
            # treats that as "connection parameters explicitly configured" and raises
            # if sdkConfig(...) is set as well. Requesting serverless compute on the
            # Config object itself avoids the conflict.
            builder = self._apply_sdk_config(builder, serverless=True)
        else:
            builder = self._apply_sdk_config(builder, cluster_id=cluster_id)

        for key, value in (config.get("spark-parameters") or {}).items():
            try:
                builder = builder.config(key, str(value))
            except Exception:
                ctx.log(
                    "could not set Spark conf %s through Connect; set it on the "
                    "cluster instead" % key,
                    stream="stderr",
                )
        return builder

    def _apply_sdk_config(self, builder, cluster_id=None, serverless=False):
        """Hand the SDK's resolved config to the Connect builder.

        Reusing the SDK config rather than passing host and token by hand means Connect,
        `query()`, and `UnityCatalogTable` authenticate identically, including OAuth and
        Azure paths.
        """
        sdk_config = self.client.sdk.config
        if cluster_id or serverless:
            try:
                sdk_config = sdk_config.copy()
            except AttributeError:
                pass
        if cluster_id:
            sdk_config.cluster_id = cluster_id
        if serverless:
            sdk_config.serverless_compute_id = "auto"
        if hasattr(builder, "sdkConfig"):
            return builder.sdkConfig(sdk_config)
        # Older databricks-connect: fall back to the explicit remote()/serverless() form.
        if serverless and hasattr(builder, "serverless"):
            builder = builder.serverless(True)
        remote_kwargs = {"host": sdk_config.host, "token": sdk_config.token}
        if cluster_id:
            remote_kwargs["cluster_id"] = cluster_id
        return builder.remote(**{k: v for k, v in remote_kwargs.items() if v})

    def _target_description(self):
        cluster_id = self.config.get("cluster_id")
        host = self.client.host or "workspace"
        if cluster_id:
            return "%s, cluster %s" % (host, cluster_id)
        return "%s, serverless" % host


def _connect_unavailable(exc):
    """Explain the databricks-connect install failure people actually hit.

    ``databricks-connect`` ships its own ``pyspark``, so having both installed breaks
    imports in a way whose error message points nowhere useful. Naming that explicitly
    saves the hour it otherwise costs.
    """
    hint = ""
    if importlib.util.find_spec("pyspark") is not None:
        hint = (
            "\n\nNote: 'pyspark' is installed in this environment. databricks-connect "
            "bundles its own copy of pyspark and the two conflict. Uninstall pyspark "
            "first:\n"
            "    pip uninstall -y pyspark\n"
            "    pip install --upgrade 'databricks-connect'\n"
            "Use backend='local' in a separate environment if you need plain pyspark."
        )
    error = SparkBackendUnavailable("databricks", "databricks-connect", extra="connect")
    if hint:
        error = SparkConfigError(str(error) + hint)
    error.__cause__ = exc
    return error


def _session_tag(ctx):
    return "metaflow-%s" % ctx.pathspec.replace("/", "-")


def _add_tag(session, tag):
    """Tag every query in this session so it can be interrupted as a group."""
    add = getattr(session, "addTag", None)
    if add is None:
        return False
    try:
        add(tag)
        return True
    except Exception:
        return False


def _interrupt(session, tag, ctx):
    interrupt = getattr(session, "interruptTag", None)
    if interrupt is None:
        return
    try:
        interrupt(tag)
        ctx.log("interrupted in-flight Databricks queries for %s" % tag)
    except Exception as exc:
        ctx.log("could not interrupt queries: %s" % exc, stream="stderr")


def _describe_job(session, ctx):
    """Label queries so they are identifiable in the Databricks query history.

    Connect mode cannot set billing tags, because those belong to the cluster rather
    than the session. A job description is what is actually available here, and it at
    least makes the flow's queries findable in the UI.
    """
    for attr, args in (
        ("setJobDescription", (ctx.pathspec,)),
        ("setJobGroup", ("metaflow", ctx.pathspec, False)),
    ):
        target = getattr(session, attr, None) or getattr(
            getattr(session, "sparkContext", None), attr, None
        )
        if target is None:
            continue
        try:
            target(*args)
            return
        except Exception:
            continue
