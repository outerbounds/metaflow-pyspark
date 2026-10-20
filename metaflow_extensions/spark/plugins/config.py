"""Configuration resolution.

Precedence, lowest to highest:

    1. defaults
    2. the Metaflow config and environment variables (METAFLOW_SPARK_*,
       METAFLOW_DATABRICKS_*, DATABRICKS_*)
    3. the flow-level config artifact named by the `config` argument
    4. explicit arguments

The flow-level artifact may be a dict, a JSON string, or an ``IncludeFile``, which
keeps the original ``spark_config`` idiom working. It holds a top-level ``backend``
and ``mode``, backend-neutral ``spark-parameters``, and one section per backend
(``databricks``, ``emr-serverless``, ``local``). A legacy flat ``@pyspark`` config, with
``application-id`` and ``execution-role`` at the top level, is read as an
``emr-serverless`` section.
"""

import json
import os

from .exceptions import SparkConfigError

DEFAULT_TIMEOUT_MINUTES = 60
DEFAULT_BACKEND = "local"

#: Keys that must never be written into an artifact.
SECRET_KEYS = ("token", "client_secret")

# Keys that a legacy flat spark_config.json uses. Their presence at the top level
# means the config predates backend sections and targets EMR Serverless.
_LEGACY_EMR_KEYS = ("application-id", "execution-role", "s3-prefix")


def _deep_merge(base, override):
    """Recursively merge `override` into `base`, returning a new dict."""
    out = dict(base)
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        elif value is not None:
            out[key] = value
    return out


def _coerce(value, what):
    """Turn a dict, a JSON string, or an IncludeFile into a dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except ValueError as exc:
            raise SparkConfigError(
                "Could not parse %s as JSON: %s" % (what, exc)
            ) from exc
        if not isinstance(parsed, dict):
            raise SparkConfigError(
                "%s must be a JSON object, got %s." % (what, type(parsed).__name__)
            )
        return parsed
    raise SparkConfigError(
        "%s must be a dict, a JSON string, or an IncludeFile, got %s."
        % (what, type(value).__name__)
    )


def _normalize(config):
    """Move legacy flat EMR keys into their backend section."""
    config = dict(config)
    legacy = {k: config.pop(k) for k in _LEGACY_EMR_KEYS if k in config}
    if legacy:
        section = dict(config.get("emr-serverless") or {})
        # An explicit section wins over the legacy top-level key.
        for key, value in legacy.items():
            section.setdefault(key, value)
        config["emr-serverless"] = section
        if "application-id" in legacy or "execution-role" in legacy:
            config.setdefault("backend", "emr-serverless")
    return config


def _env_config():
    """Read configuration out of the Metaflow config and the environment."""
    # Imported here: this module is reachable from plugin modules Metaflow imports
    # while `metaflow/__init__` is still executing.
    from metaflow.metaflow_config_funcs import from_conf

    config = {}
    backend = from_conf("SPARK_BACKEND")
    if backend:
        config["backend"] = backend
    mode = from_conf("SPARK_MODE")
    if mode:
        config["mode"] = mode

    databricks = {}
    # Honour the standard Databricks environment variables so that a workspace
    # already configured for the CLI or the SDK needs no extra Metaflow config.
    for key, envs in (
        ("host", ("METAFLOW_DATABRICKS_HOST", "DATABRICKS_HOST")),
        ("token", ("METAFLOW_DATABRICKS_TOKEN", "DATABRICKS_TOKEN")),
        ("profile", ("METAFLOW_DATABRICKS_PROFILE", "DATABRICKS_CONFIG_PROFILE")),
        ("client_id", ("METAFLOW_DATABRICKS_CLIENT_ID", "DATABRICKS_CLIENT_ID")),
        (
            "client_secret",
            ("METAFLOW_DATABRICKS_CLIENT_SECRET", "DATABRICKS_CLIENT_SECRET"),
        ),
        (
            "warehouse_id",
            ("METAFLOW_DATABRICKS_WAREHOUSE_ID", "DATABRICKS_WAREHOUSE_ID"),
        ),
        ("cluster_id", ("METAFLOW_DATABRICKS_CLUSTER_ID", "DATABRICKS_CLUSTER_ID")),
        ("volume", ("METAFLOW_DATABRICKS_VOLUME",)),
        ("runtime_version", ("METAFLOW_DATABRICKS_RUNTIME_VERSION",)),
    ):
        for env in envs:
            value = os.environ.get(env)
            if value:
                databricks[key] = value
                break
    if databricks:
        config["databricks"] = databricks

    emr = {}
    for key, env in (
        ("application-id", "METAFLOW_SPARK_EMR_APPLICATION_ID"),
        ("execution-role", "METAFLOW_SPARK_EMR_EXECUTION_ROLE"),
        ("s3-prefix", "METAFLOW_SPARK_EMR_S3_PREFIX"),
    ):
        value = os.environ.get(env)
        if value:
            emr[key] = value
    if emr:
        config["emr-serverless"] = emr
    return config


def flow_config(flow, config_attr):
    """The config layer supplied by the flow.

    `config_attr` is either a dict/JSON string used directly, or the name of a flow
    attribute holding one. A missing named attribute is not an error: the environment
    and explicit arguments may configure everything.
    """
    if isinstance(config_attr, str):
        raw = getattr(flow, config_attr, None)
        return (
            _coerce(raw, "the '%s' artifact" % config_attr) if raw is not None else {}
        )
    return _coerce(config_attr, "the config argument")


def resolve_config(flow, config_attr, overrides):
    """Build the effective config dict from every layer."""
    config = _deep_merge(_env_config(), _normalize(flow_config(flow, config_attr)))
    config = _deep_merge(config, _normalize(overrides or {}))

    config.setdefault("backend", DEFAULT_BACKEND)
    config.setdefault("timeout", DEFAULT_TIMEOUT_MINUTES)
    config.setdefault("databricks", {})
    return config


def backend_config(config, backend_name):
    """Return the config section a single backend should see.

    Backend-neutral `spark-parameters` are merged underneath the backend's own, so a
    backend-specific value wins.
    """
    section = dict(config.get(backend_name) or {})
    neutral = config.get("spark-parameters") or {}
    if neutral:
        section["spark-parameters"] = _deep_merge(
            neutral, section.get("spark-parameters") or {}
        )
    section.setdefault("timeout", config.get("timeout"))
    if config.get("mode"):
        section.setdefault("mode", config["mode"])
    return section


def require(section, key, backend_name, hint=None):
    """Fetch a required config key, or raise a message that says how to set it."""
    value = section.get(key)
    if value in (None, ""):
        msg = "The '%s' backend requires '%s' to be set." % (backend_name, key)
        if hint:
            msg += "\n" + hint
        raise SparkConfigError(msg)
    return value
