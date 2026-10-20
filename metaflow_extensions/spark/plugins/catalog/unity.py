"""Unity Catalog tables as Metaflow artifacts.

The central idea: **the artifact is a table reference, not a copy of the table.**

    self.orders = UnityCatalogTable("main.retail.orders")

Assignment pins the table's current Delta version, so re-running the flow reads the same
bytes even if the table has moved on. That extends Metaflow's reproducibility guarantee
over data the customer governs in Unity Catalog, without copying it and without taking
governance away from UC.

Reads can happen two ways:

*Through Spark*, with ``to_spark(session)``, which is what you want when the data is
large or the work is a join.

*Without any cluster*, with ``to_arrow()``, which asks Unity Catalog to vend temporary,
scoped cloud credentials for the table's storage location and reads the Delta files
directly. Governance is preserved because UC issues the credentials and enforces the
grants; there is simply no Spark involved. For mid-size tables this is dramatically
cheaper than starting a cluster.

A table with deletion vectors enabled, the default on recent Databricks Runtimes for
many write patterns, cannot be read by the ``deltalake`` package's reader at all: it does
not support that protocol feature on any of its read methods, as of the latest release at
the time this was written (1.6.3). ``to_arrow()`` falls back to DuckDB's own Delta
reader for that case, which does apply deletion vectors correctly. Covers AWS and Azure
SAS credentials (see ``_read_via_duckdb``'s docstring for exactly how well-verified each
is); GCP has no DuckDB secret shape for the OAuth bearer token UC vends. Install
``duckdb`` to get the fallback; without it, or on GCP, a table with deletion vectors fails
with an explanation rather than a raw stack trace, pointing at ``query()`` on a SQL
warehouse.
"""

import os
import sys
import time

from ..config import SECRET_KEYS, _deep_merge, flow_config, resolve_config
from ..exceptions import SparkBackendUnavailable, UnityCatalogError

UC_API = "/api/2.1/unity-catalog"

#: Vended credentials are short-lived. Re-vend slightly before expiry rather than
#: racing it, since a long read can outlive a token issued at the start.
CREDENTIAL_REFRESH_MARGIN_SECONDS = 60
DEFAULT_REGION = "us-east-1"


def _delta_protocol_error_types():
    """The exception type(s) that mean "this table's protocol is not supported".

    A tuple rather than a single class, and resolved lazily: `deltalake` is an
    optional dependency (the `catalog` extra), and an empty tuple here just means
    nothing gets special-cased, not an ImportError at module load.
    """
    try:
        from deltalake.exceptions import DeltaProtocolError

        return (DeltaProtocolError,)
    except ImportError:
        return ()


def _azure_storage_account(storage_location):
    """Pull the storage account name out of an ADLS Gen2 storage location.

    UC's Azure storage locations look like
    `abfss://<container>@<account>.dfs.core.windows.net/<path>`. Needed for the
    `AccountName=`/`BlobEndpoint=https://<account>.blob.core.windows.net;...`
    connection string DuckDB's Azure secret takes; note that is the *blob* endpoint
    even though the URL scheme is `abfss`, per Microsoft's own connection-string
    format.
    """
    if not storage_location:
        return None
    from urllib.parse import urlparse

    host = urlparse(storage_location).netloc.rsplit("@", 1)[-1]
    return host.split(".", 1)[0] if host else None


class UnityCatalogTable:
    """A pinned reference to a Unity Catalog table.

    Picklable, so it round-trips as a Metaflow artifact. Live clients, vended
    credentials, and any ``token`` or ``client_secret`` in ``config`` are excluded from
    the pickled state, so a step that reads the artifact authenticates from its own
    environment.

    Connection settings resolve like ``query()``'s: ``DATABRICKS_*`` /
    ``METAFLOW_DATABRICKS_*`` environment variables, then the ``databricks`` section of
    ``flow.spark_config`` when ``flow`` is given, then ``config``.

    ``pin=True`` records the current Delta version, or warns and leaves the reference
    unpinned if it cannot be read (no ``deltalake``, or vending refused).
    ``pin="required"`` raises instead, and ``pin=False`` skips pinning.
    """

    def __init__(
        self,
        name,
        version=None,
        timestamp=None,
        client=None,
        pin=True,
        config=None,
        flow=None,
    ):
        if name.count(".") != 2:
            raise UnityCatalogError(
                "Unity Catalog tables are named catalog.schema.table, got %r." % name
            )
        if pin not in (True, False, "required"):
            raise UnityCatalogError(
                "pin must be True, False, or 'required', got %r." % (pin,)
            )
        self.full_name = name
        self.catalog, self.schema, self.table = name.split(".")
        self.version = version
        self.timestamp = timestamp
        self.table_id = None
        self.storage_location = None
        self.table_type = None
        self.data_source_format = None

        self._pin = pin
        self._client = client
        # The environment layer is applied when the client is built, not captured
        # here, so a reading step uses its own environment.
        from_flow = flow_config(flow, "spark_config").get("databricks") or {}
        self._config = _deep_merge(from_flow, config or {})
        self._credentials = None
        self._credentials_expiry = 0

        self._describe()
        if pin and self.version is None and self.timestamp is None:
            self.version = self._current_version()

    # ------------------------------------------------------------------
    # pickling
    # ------------------------------------------------------------------
    def __getstate__(self):
        state = self.__dict__.copy()
        for key in ("_client", "_credentials", "_credentials_expiry"):
            state.pop(key, None)
        state["_config"] = {
            k: v for k, v in self._config.items() if k not in SECRET_KEYS
        }
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._client = None
        self._credentials = None
        self._credentials_expiry = 0
        self._config = getattr(self, "_config", {}) or {}
        self._pin = getattr(self, "_pin", True)

    def __repr__(self):
        pin = ""
        if self.version is not None:
            pin = " @v%s" % self.version
        elif self.timestamp:
            pin = " @%s" % self.timestamp
        return "UnityCatalogTable(%s%s)" % (self.full_name, pin)

    # ------------------------------------------------------------------
    @property
    def client(self):
        if self._client is None:
            from ..backends.databricks.client import DatabricksClient

            config = resolve_config(None, None, {"databricks": self._config})
            self._client = DatabricksClient(config["databricks"])
        return self._client

    @property
    def pinned(self):
        return self.version is not None or self.timestamp is not None

    def _describe(self):
        try:
            info = self.client.api("GET", "%s/tables/%s" % (UC_API, self.full_name))
        except Exception as exc:
            raise UnityCatalogError(
                "Could not read Unity Catalog metadata for %s: %s\n"
                "Check that the table exists and that you have SELECT on it plus USE "
                "CATALOG and USE SCHEMA on its parents." % (self.full_name, exc)
            ) from exc
        self.table_id = info.get("table_id")
        self.storage_location = info.get("storage_location")
        self.table_type = info.get("table_type")
        self.data_source_format = info.get("data_source_format")

    def _current_version(self):
        """Read the table's current Delta version, without needing a cluster."""
        if self.data_source_format not in (None, "DELTA", "UNITY_CATALOG"):
            if self._pin == "required":
                raise UnityCatalogError(
                    "%s is %s, not Delta, so it cannot be pinned to a version."
                    % (self.full_name, self.data_source_format)
                )
            return None
        try:
            delta = self._delta_table()
            return delta.version() if delta is not None else None
        except Exception as exc:
            if self._pin == "required":
                raise UnityCatalogError(
                    "Could not pin %s to its current Delta version: %s\n"
                    "Pinning reads the Delta log through credential vending, which "
                    "needs deltalake installed and EXTERNAL USE SCHEMA granted. Or pin "
                    'explicitly with version=N, using N from query("DESCRIBE HISTORY '
                    '%s").' % (self.full_name, exc, self.full_name)
                ) from exc
            # Silent divergence is the outcome to avoid: an unpinned reference reads
            # whatever the table looks like at call time, and that has to be visible,
            # not a `version=None` nobody notices until a reproduce run comes up empty.
            # warnings.warn() is not enough here: Metaflow's own CLI calls
            # warnings.filterwarnings("ignore") globally (metaflow/cli.py), which
            # silently swallows warnings from user code too. Write to stderr directly.
            print(
                "[UnityCatalogTable] %s was assigned without a pinned Delta version: %s\n"
                "Reads through this reference will see the table's current state on "
                "every call instead of a fixed snapshot. This usually means credential "
                "vending is unavailable (no EXTERNAL USE SCHEMA, or deltalake is not "
                "installed). Pin explicitly with UnityCatalogTable(%r, version=N), "
                "using N from query(\"DESCRIBE HISTORY %s\"), or pass pin='required' "
                "to fail instead of warning."
                % (self.full_name, exc, self.full_name, self.full_name),
                file=sys.stderr,
            )
            return None

    def at_version(self, version):
        """Return a new reference pinned to `version`."""
        return UnityCatalogTable(
            self.full_name, version=version, client=self._client, config=self._config
        )

    def at_timestamp(self, timestamp):
        return UnityCatalogTable(
            self.full_name,
            timestamp=timestamp,
            client=self._client,
            config=self._config,
        )

    def latest(self):
        """Return a new reference pinned to the table's current version."""
        return UnityCatalogTable(
            self.full_name,
            client=self._client,
            config=self._config,
            pin=self._pin or True,
        )

    def _fallback_hint(self):
        """How to read this reference when credential vending cannot."""
        source = self.full_name
        if self.version is not None:
            source += " VERSION AS OF %d" % self.version
        elif self.timestamp is not None:
            source += " TIMESTAMP AS OF '%s'" % self.timestamp
        return (
            "Read it through a SQL warehouse instead, which applies the table's "
            'grants, views, and protocol, e.g. query("SELECT * FROM %s"), or through '
            "Spark with to_spark(self.spark) in an @spark step." % source
        )

    # ------------------------------------------------------------------
    # credential vending
    # ------------------------------------------------------------------
    def credentials(self, operation="READ"):
        """Ask Unity Catalog for temporary, scoped credentials for this table.

        This is the mechanism that lets governed data be read outside Databricks without
        going around governance: UC checks the grant and issues a short-lived credential
        for that table's storage only.
        """
        now = time.time()
        if self._credentials and now < self._credentials_expiry:
            return self._credentials

        if not self.table_id:
            raise UnityCatalogError(
                "No table_id for %s, so credentials cannot be vended." % self.full_name
            )
        try:
            response = self.client.api(
                "POST",
                "%s/temporary-table-credentials" % UC_API,
                body={"table_id": self.table_id, "operation": operation},
            )
        except Exception as exc:
            raise UnityCatalogError(
                "Unity Catalog refused to vend credentials for %s: %s\n"
                "Credential vending requires EXTERNAL USE SCHEMA on the schema (or "
                "equivalent) in addition to SELECT on the table. %s"
                % (self.full_name, exc, self._fallback_hint())
            ) from exc

        expiry_ms = response.get("expiration_time")
        if expiry_ms:
            # The API reports an absolute epoch in milliseconds.
            self._credentials_expiry = (
                expiry_ms / 1000.0
            ) - CREDENTIAL_REFRESH_MARGIN_SECONDS
        else:
            self._credentials_expiry = now + 300
        self._credentials = response
        return response

    def storage_options(self, operation="READ"):
        """Translate vended credentials into delta-rs / object_store options."""
        creds = self.credentials(operation=operation)
        options = {}

        aws = creds.get("aws_temp_credentials")
        if aws:
            options.update(
                {
                    "AWS_ACCESS_KEY_ID": aws.get("access_key_id"),
                    "AWS_SECRET_ACCESS_KEY": aws.get("secret_access_key"),
                    "AWS_SESSION_TOKEN": aws.get("session_token"),
                }
            )
            options["AWS_REGION"] = self._resolve_region(aws)
            return {k: v for k, v in options.items() if v}

        sas = creds.get("azure_user_delegation_sas")
        if sas:
            return {"AZURE_STORAGE_SAS_TOKEN": sas.get("sas_token")}

        aad = creds.get("azure_aad")
        if aad:
            return {"AZURE_STORAGE_TOKEN": aad.get("aad_token")}

        gcp = creds.get("gcp_oauth_token")
        if gcp:
            return {"GOOGLE_BEARER_TOKEN": gcp.get("oauth_token")}

        raise UnityCatalogError(
            "Unity Catalog returned credentials in a form this version does not "
            "understand: %s. %s" % (", ".join(sorted(creds)), self._fallback_hint())
        )

    def _resolve_region(self, aws_credentials):
        """Find the bucket's region, which delta-rs needs but UC does not return."""
        for env in ("AWS_REGION", "AWS_DEFAULT_REGION"):
            if os.environ.get(env):
                return os.environ[env]
        location = self.storage_location or ""
        if location.startswith("s3://"):
            bucket = location[len("s3://") :].split("/")[0]
            try:
                import boto3

                client = boto3.client(
                    "s3",
                    aws_access_key_id=aws_credentials.get("access_key_id"),
                    aws_secret_access_key=aws_credentials.get("secret_access_key"),
                    aws_session_token=aws_credentials.get("session_token"),
                )
                region = client.get_bucket_location(Bucket=bucket).get(
                    "LocationConstraint"
                )
                # us-east-1 is reported as None for historical reasons.
                return region or "us-east-1"
            except Exception:
                pass
        return DEFAULT_REGION

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------
    def _delta_table(self):
        try:
            from deltalake import DeltaTable
        except ImportError:
            raise SparkBackendUnavailable(
                "unity-catalog credential vending", "deltalake", extra="catalog"
            )
        if not self.storage_location:
            raise UnityCatalogError(
                "%s has no storage location, so credential vending cannot read it. "
                "Views and foreign tables need the query engine. %s"
                % (self.full_name, self._fallback_hint())
            )
        kwargs = {"storage_options": self.storage_options()}
        if self.version is not None:
            kwargs["version"] = self.version
        return DeltaTable(self.storage_location, **kwargs)

    def to_arrow(self, columns=None, filters=None):
        """Read the table into an Arrow table with no cluster involved."""
        delta = self._delta_table()
        if self.timestamp is not None:
            delta.load_as_version(self.timestamp)
        try:
            dataset = delta.to_pyarrow_dataset()
            table = dataset.to_table(columns=columns, filter=filters)
        except _delta_protocol_error_types() as exc:
            # The log still loads when the data cannot, so this is the pinned (or
            # timestamp-resolved) version for DuckDB to read, not the latest.
            table = self._read_via_duckdb(exc, version=delta.version())
            if columns:
                table = table.select(columns)
            if filters is not None:
                table = table.filter(filters)
        return table

    def _translate_protocol_error(self, exc, duckdb_reason=None):
        """Turn a delta-rs protocol error into an explanation, not a stack trace.

        Deletion vectors are on by default for many tables created on recent
        Databricks Runtimes (predictive optimization enables them), and delta-rs's
        Python reader does not support them as of the pinned minimum
        (``deltalake>=0.18``) through at least 1.6.3, on any of its read methods
        (``to_pyarrow_dataset``, ``to_pyarrow_table``, ``to_pandas``): confirmed by
        reproducing this against a local table with deletion vectors enabled, not
        assumed from the error message alone. ``to_arrow()`` tries DuckDB's own Delta
        reader first, which does apply deletion vectors correctly (also confirmed by
        reproduction, not assumed); this is what is left to say when that has already
        failed too, or was not applicable (Azure AAD and GCP; see
        ``_read_via_duckdb``).

        ``duckdb_reason`` says specifically why the fallback did not apply or did not
        work, so the log line itself answers "why didn't it fall back to DuckDB"
        without anyone having to go digging through a swallowed exception chain.
        """
        message = "%s cannot be read through credential vending: %s\n%s" % (
            self.full_name,
            exc,
            self._fallback_hint(),
        )
        if duckdb_reason:
            message += "\n(DuckDB fallback not used: %s)" % duckdb_reason
        return UnityCatalogError(message)

    def _read_via_duckdb(self, original_exc, version=None):
        """Fall back to DuckDB's own Delta reader for a table delta-rs cannot read.

        This is a genuinely different implementation from `deltalake`'s Python
        bindings, not the same engine through a different API, and it applies
        deletion vectors where delta-rs's Python reader does not (verified locally:
        `deltalake` fails on a deletion-vector table on every read method it has,
        DuckDB's `delta_scan` reads the same table correctly). Using it here is what
        keeps "no cluster" true for these tables instead of conceding them to Spark.

        Wired up for AWS and Azure SAS credentials, the two shapes UC vends that map
        onto something DuckDB's secret model accepts. GCP does not: DuckDB's GCS
        secret needs HMAC keys, not the OAuth bearer token UC vends, and there is no
        documented parameter that takes one. Azure AAD tokens (`azure_aad`, vended
        when the storage credential is a managed identity / Access Connector rather
        than SAS-capable) have the same problem: DuckDB's Azure secret has no
        parameter for an already-obtained bearer token either. AWS is verified end to
        end, including against a real deletion-vector table (see
        tests/test_unity_catalog.py). Azure SAS was verified against a real Azure
        workspace and initially failed: the connection string needs `AccountName=`
        in it, not just `BlobEndpoint=`/`SharedAccessSignature=` (Microsoft's own
        documented minimal format for a SAS-based connection string, which omits
        `AccountName`); duckdb-azure's own pre-check
        (`ConnectionStringMatchStorageAccountName` in
        `azure_storage_account_client.cpp`) requires it and throws before ever
        reaching the real Azure SDK parser if it is missing. With it, reads against a
        real Azure workspace succeed, including pinned reads of older versions.

        Every failure here, including a vending refusal on top of the protocol
        error, ends up at the same explanation, but with a specific reason attached
        rather than a second, unrelated-looking error: `credentials()`'s own
        "refused to vend" message escaping instead would bury the actual cause (the
        table's protocol, not a missing grant).
        """
        try:
            creds = self.credentials()
        except Exception as exc:
            raise self._translate_protocol_error(
                original_exc,
                "vending credentials for the fallback itself failed: %s" % exc,
            )

        secret = self._duckdb_secret_for(creds)
        if secret is None:
            raise self._translate_protocol_error(
                original_exc,
                "the vended credentials were shaped %s, and only "
                "aws_temp_credentials or azure_user_delegation_sas (with a "
                "parseable abfss:// account) have a matching DuckDB secret"
                % sorted(creds),
            )
        extension, secret_sql, secret_params = secret

        try:
            import duckdb
        except ImportError:
            raise self._translate_protocol_error(
                original_exc, "duckdb is not installed"
            )

        con = duckdb.connect()
        try:
            con.install_extension(extension)
            con.load_extension(extension)
            if extension == "azure":
                # The Azure SDK's default transport looks for a CA bundle at a path
                # many container images lack, failing with "Problem with the SSL CA
                # cert". The curl transport probes the usual locations instead.
                con.execute("SET azure_transport_option_type = 'curl'")
            con.install_extension("delta")
            con.load_extension("delta")
            con.execute(secret_sql, secret_params)
            try:
                # `.fetch_arrow_table()` rather than the lazy `.arrow()` reader:
                # the connection closes in `finally` below, and a lazy
                # RecordBatchReader consumed after that point silently reads as
                # empty rather than raising, which is a worse bug than a slower one.
                if version is None:
                    scan, params = "delta_scan(?)", [self.storage_location]
                else:
                    scan = "delta_scan(?, version => ?)"
                    params = [self.storage_location, version]
                return con.sql(
                    "SELECT * FROM %s" % scan, params=params
                ).fetch_arrow_table()
            except Exception as exc:
                raise self._translate_protocol_error(
                    original_exc, "DuckDB's own reader also failed: %s" % exc
                ) from exc
        finally:
            con.execute("DROP SECRET IF EXISTS metaflow_uc_credential")
            con.close()

    def _duckdb_secret_for(self, creds):
        """Build the `(extension, CREATE SECRET sql, params)` for these credentials.

        Returns None when the credential shape has no DuckDB secret that accepts it
        (AAD tokens, GCP), so the caller falls back to the Spark-pointer error rather
        than attempting a connection that cannot possibly authenticate.
        """
        aws = creds.get("aws_temp_credentials")
        if aws:
            return (
                "httpfs",
                "CREATE OR REPLACE SECRET metaflow_uc_credential (TYPE s3, "
                "PROVIDER config, KEY_ID ?, SECRET ?, SESSION_TOKEN ?, REGION ?, "
                "SCOPE ?)",
                [
                    aws.get("access_key_id"),
                    aws.get("secret_access_key"),
                    aws.get("session_token"),
                    self._resolve_region(aws),
                    self.storage_location,
                ],
            )

        sas = creds.get("azure_user_delegation_sas")
        if sas and sas.get("sas_token"):
            account = _azure_storage_account(self.storage_location)
            if account:
                # AccountName= has to be present and has to match the account name
                # DuckDB itself parses out of the abfss:// URL: duckdb-azure's own
                # ConnectionStringMatchStorageAccountName check requires it and
                # throws "A invalid connection string has been provided" (its exact
                # wording) if it is missing, before the connection string ever
                # reaches the real Azure SDK parser. The general Azure
                # BlobEndpoint+SAS format Microsoft documents does not need
                # AccountName at all; DuckDB's own extension does, regardless.
                connection_string = (
                    "AccountName=%s;BlobEndpoint=https://%s.blob.core.windows.net;"
                    "SharedAccessSignature=%s" % (account, account, sas["sas_token"])
                )
                return (
                    "azure",
                    "CREATE OR REPLACE SECRET metaflow_uc_credential "
                    "(TYPE azure, CONNECTION_STRING ?)",
                    [connection_string],
                )

        return None

    def to_pandas(self, columns=None, filters=None):
        return self.to_arrow(columns=columns, filters=filters).to_pandas()

    def to_polars(self, columns=None, filters=None):
        import polars

        return polars.from_arrow(self.to_arrow(columns=columns, filters=filters))

    def to_duckdb(self, connection=None, view_name=None):
        """Register the table as a DuckDB view for SQL without a cluster."""
        import duckdb

        connection = connection or duckdb.connect()
        arrow_table = self.to_arrow()
        connection.register(view_name or self.table, arrow_table)
        return connection

    def to_spark(self, session):
        """Read through Spark, honouring the pinned version.

        Pass the session of an ``@spark`` step (``self.spark``). This path keeps every
        read inside Unity Catalog's enforcement, and is the right choice for large
        tables or anything join-heavy.
        """
        reader = session.read
        if self.version is not None:
            reader = reader.option("versionAsOf", self.version)
        elif self.timestamp is not None:
            reader = reader.option("timestampAsOf", self.timestamp)
        return reader.table(self.full_name)

    # ------------------------------------------------------------------
    def history(self, limit=20):
        """Delta history entries, most recent first."""
        delta = self._delta_table()
        return delta.history(limit)

    def schema_fields(self):
        try:
            info = self.client.api("GET", "%s/tables/%s" % (UC_API, self.full_name))
        except Exception:
            return []
        return [
            {"name": c.get("name"), "type": c.get("type_text")}
            for c in info.get("columns") or []
        ]


def read_table(name, version=None, columns=None, spark=None, config=None):
    """Convenience reader.

    With `spark`, reads through the session. Without it, reads via credential vending
    and returns an Arrow table.
    """
    ref = UnityCatalogTable(name, version=version, config=config)
    if spark is not None:
        return ref.to_spark(spark)
    return ref.to_arrow(columns=columns)
