"""Workspace connection handling for every Databricks call.

Auth is delegated entirely to ``databricks-sdk`` rather than reimplemented. The SDK
already resolves PAT, OAuth machine-to-machine, Azure MSI, CLI profiles, and notebook
context in the documented precedence order, and getting that wrong is the single most
common source of "it works in my IDE but not in the job" confusion. Anything the
installed SDK version does not model yet goes through ``api()``, which speaks raw REST
over the SDK's already-authenticated client.
"""

from ...exceptions import SparkBackendUnavailable, SparkConfigError


class DatabricksClient:
    def __init__(self, config=None):
        self.config = config or {}
        self._sdk = None

    # ------------------------------------------------------------------
    @property
    def sdk(self):
        if self._sdk is None:
            self._sdk = self._build_sdk()
        return self._sdk

    def _build_sdk(self):
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise SparkBackendUnavailable(
                "databricks", "databricks-sdk", extra="databricks"
            )

        config = self.config
        kwargs = {}
        # Only pass through what was explicitly configured. Passing None for a field
        # the SDK would otherwise discover from the environment defeats its own
        # resolution chain.
        for key in (
            "host",
            "token",
            "profile",
            "client_id",
            "client_secret",
            "auth_type",
            "azure_workspace_resource_id",
            "google_service_account",
        ):
            value = config.get(key)
            if value:
                kwargs[key] = value

        try:
            return WorkspaceClient(**kwargs)
        except Exception as exc:
            raise SparkConfigError(
                "Could not connect to Databricks: %s\n\n"
                "Set a host and credentials in one of the supported ways:\n"
                "  - DATABRICKS_HOST and DATABRICKS_TOKEN in the step environment\n"
                "  - a CLI profile, via DATABRICKS_CONFIG_PROFILE\n"
                "  - OAuth, via DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET\n"
                "  - host=..., token=... in the 'databricks' section of the config "
                "artifact, or in UnityCatalogTable(config=...)" % exc
            ) from exc

    @property
    def host(self):
        host = self.sdk.config.host
        return host.rstrip("/") if host else None

    # ------------------------------------------------------------------
    def api(self, method, path, body=None, query=None):
        """Call a REST endpoint through the SDK's authenticated client.

        Used for endpoints that the installed SDK version may not expose as a typed
        method, so a slightly older SDK does not block a feature.
        """
        return self.sdk.api_client.do(method, path, body=body, query=query) or {}

    # ------------------------------------------------------------------
    def warehouse_url(self, warehouse_id):
        """Deep link to a SQL warehouse's detail page.

        The Statement Execution API has no documented deep link for a single
        statement, so this points at the warehouse a statement ran on instead of
        guessing at an unstable per-statement URL.
        """
        host = self.host
        if not host or not warehouse_id:
            return None
        return "%s/sql/warehouses/%s" % (host, warehouse_id)
