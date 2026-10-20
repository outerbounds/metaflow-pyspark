"""Tests for the table-reference artifact.

The behaviour worth protecting: assignment pins a version, the reference survives
pickling as a Metaflow artifact, and vended credentials never end up inside that
artifact.
"""

import pickle

import pytest

from metaflow_extensions.spark.plugins.catalog.unity import UnityCatalogTable
from metaflow_extensions.spark.plugins.exceptions import UnityCatalogError

TABLE_INFO = {
    "table_id": "tbl-123",
    "storage_location": "s3://lake/retail/orders",
    "table_type": "EXTERNAL",
    "data_source_format": "DELTA",
    "columns": [
        {"name": "order_id", "type_text": "bigint"},
        {"name": "amount", "type_text": "decimal(10,2)"},
    ],
}


class FakeClient:
    """Stands in for DatabricksClient, recording what was asked of Unity Catalog."""

    def __init__(self, info=None, credentials=None, fail=None):
        self.info = info if info is not None else dict(TABLE_INFO)
        self.credentials = credentials
        self.fail = fail
        self.calls = []

    def api(self, method, path, body=None, query=None):
        self.calls.append((method, path, body))
        if self.fail:
            raise self.fail
        if path.endswith("temporary-table-credentials"):
            if self.credentials is None:
                raise RuntimeError("PERMISSION_DENIED: EXTERNAL USE SCHEMA")
            return self.credentials
        return self.info


def table(client=None, **kwargs):
    kwargs.setdefault("pin", False)
    return UnityCatalogTable(
        "main.retail.orders", client=client or FakeClient(), **kwargs
    )


# ----------------------------------------------------------------------
def test_name_must_be_three_parts():
    with pytest.raises(UnityCatalogError, match="catalog.schema.table"):
        UnityCatalogTable("orders", client=FakeClient())
    with pytest.raises(UnityCatalogError):
        UnityCatalogTable("retail.orders", client=FakeClient())


def test_metadata_is_read_from_unity_catalog():
    ref = table()
    assert ref.catalog == "main"
    assert ref.schema == "retail"
    assert ref.table == "orders"
    assert ref.table_id == "tbl-123"
    assert ref.storage_location == "s3://lake/retail/orders"


def test_a_missing_table_explains_the_grants_needed():
    client = FakeClient(fail=RuntimeError("TABLE_DOES_NOT_EXIST"))
    with pytest.raises(UnityCatalogError) as exc:
        UnityCatalogTable("main.retail.orders", client=client)
    assert "USE CATALOG" in str(exc.value)


def test_explicit_version_is_kept_and_reported():
    ref = table(version=17)
    assert ref.pinned
    assert repr(ref) == "UnityCatalogTable(main.retail.orders @v17)"


def test_timestamp_pinning():
    ref = table(timestamp="2026-08-01T00:00:00Z")
    assert ref.pinned
    assert "2026-08-01" in repr(ref)


def test_unpinned_reference_is_not_pinned():
    assert not table().pinned


def test_a_failed_pin_warns_and_leaves_the_reference_unpinned(capsys):
    ref = table(client=FakeClient(credentials=None), pin=True)
    assert not ref.pinned
    assert "without a pinned Delta version" in capsys.readouterr().err


def test_a_required_pin_raises_instead_of_warning():
    with pytest.raises(UnityCatalogError, match="Could not pin"):
        table(client=FakeClient(credentials=None), pin="required")


def test_a_required_pin_rejects_non_delta_tables():
    client = FakeClient(info=dict(TABLE_INFO, data_source_format="PARQUET"))
    with pytest.raises(UnityCatalogError, match="not Delta"):
        table(client=client, pin="required")


def test_pin_must_be_a_known_mode():
    with pytest.raises(UnityCatalogError, match="pin must be"):
        table(pin="yes")


# ----------------------------------------------------------------------
def test_pickling_drops_the_client_and_credentials():
    ref = table(version=5)
    ref._credentials = {"aws_temp_credentials": {"access_key_id": "AKIA-secret"}}
    ref._credentials_expiry = 1e12

    blob = pickle.dumps(ref)
    assert b"AKIA-secret" not in blob

    restored = pickle.loads(blob)
    assert restored.full_name == "main.retail.orders"
    assert restored.version == 5
    assert restored._client is None
    assert restored._credentials is None


def test_pickling_drops_configured_secrets_but_keeps_the_rest():
    ref = table(
        config={"host": "https://h", "token": "dapi-secret", "client_secret": "s"}
    )
    blob = pickle.dumps(ref)
    assert b"dapi-secret" not in blob
    assert pickle.loads(blob)._config == {"host": "https://h"}
    # The live object still has what it was given.
    assert ref._config["token"] == "dapi-secret"


@pytest.fixture
def recorded_client_config(monkeypatch):
    """Capture the config the reference hands to DatabricksClient."""
    import metaflow_extensions.spark.plugins.backends.databricks.client as client_mod

    for env in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_WAREHOUSE_ID"):
        monkeypatch.delenv(env, raising=False)
        monkeypatch.delenv("METAFLOW_" + env, raising=False)
    seen = []

    class RecordingClient(FakeClient):
        def __init__(self, config=None):
            super().__init__()
            seen.append(config)

    monkeypatch.setattr(client_mod, "DatabricksClient", RecordingClient)
    return seen


def test_connection_settings_follow_the_query_precedence(
    recorded_client_config, monkeypatch
):
    monkeypatch.setenv("METAFLOW_DATABRICKS_HOST", "https://env")
    monkeypatch.setenv("DATABRICKS_TOKEN", "env-token")

    class Flow:
        spark_config = {"databricks": {"host": "https://flow", "profile": "flow"}}

    UnityCatalogTable(
        "main.retail.orders", pin=False, flow=Flow(), config={"profile": "explicit"}
    )
    assert recorded_client_config == [
        {"host": "https://flow", "token": "env-token", "profile": "explicit"}
    ]


def test_an_unpickled_reference_authenticates_from_the_reading_environment(
    recorded_client_config, monkeypatch
):
    ref = UnityCatalogTable("main.retail.orders", pin=False, config={"token": "writer"})
    restored = pickle.loads(pickle.dumps(ref))
    monkeypatch.setenv("METAFLOW_DATABRICKS_TOKEN", "reader")
    restored.schema_fields()
    assert recorded_client_config[-1] == {"token": "reader"}


def test_pickled_reference_still_knows_its_storage_location():
    restored = pickle.loads(pickle.dumps(table(version=1)))
    assert restored.storage_location == "s3://lake/retail/orders"
    assert restored.table_id == "tbl-123"


# ----------------------------------------------------------------------
def test_credential_vending_asks_for_the_table_id():
    creds = {
        "aws_temp_credentials": {
            "access_key_id": "AKIA",
            "secret_access_key": "secret",
            "session_token": "token",
        },
        "expiration_time": 4_000_000_000_000,
    }
    client = FakeClient(credentials=creds)
    ref = table(client=client)
    options = ref.storage_options()
    assert options["AWS_ACCESS_KEY_ID"] == "AKIA"
    assert options["AWS_SESSION_TOKEN"] == "token"
    assert options["AWS_REGION"]
    assert (
        "POST",
        "/api/2.1/unity-catalog/temporary-table-credentials",
        {
            "table_id": "tbl-123",
            "operation": "READ",
        },
    ) in client.calls


def test_credentials_are_cached_until_they_near_expiry():
    creds = {
        "aws_temp_credentials": {"access_key_id": "AKIA"},
        "expiration_time": 4_000_000_000_000,
    }
    client = FakeClient(credentials=creds)
    ref = table(client=client)
    ref.credentials()
    before = len(client.calls)
    ref.credentials()
    assert len(client.calls) == before


def test_refused_vending_points_at_the_spark_path():
    ref = table(client=FakeClient(credentials=None))
    with pytest.raises(UnityCatalogError) as exc:
        ref.storage_options()
    assert "EXTERNAL USE SCHEMA" in str(exc.value)
    assert "query(" in str(exc.value)


def test_azure_sas_credentials_are_translated():
    client = FakeClient(
        credentials={"azure_user_delegation_sas": {"sas_token": "sv=2024"}}
    )
    assert table(client=client).storage_options() == {
        "AZURE_STORAGE_SAS_TOKEN": "sv=2024"
    }


def test_gcp_credentials_are_translated():
    client = FakeClient(credentials={"gcp_oauth_token": {"oauth_token": "ya29"}})
    assert table(client=client).storage_options() == {"GOOGLE_BEARER_TOKEN": "ya29"}


def test_unknown_credential_shape_is_reported_not_ignored():
    client = FakeClient(credentials={"martian_credentials": {}})
    with pytest.raises(UnityCatalogError, match="martian_credentials"):
        table(client=client).storage_options()


def test_region_comes_from_the_environment_when_set(monkeypatch):
    monkeypatch.setenv("AWS_REGION", "eu-central-1")
    client = FakeClient(credentials={"aws_temp_credentials": {"access_key_id": "AKIA"}})
    assert table(client=client).storage_options()["AWS_REGION"] == "eu-central-1"


# ----------------------------------------------------------------------
def test_to_spark_applies_version_as_of():
    class Reader:
        def __init__(self):
            self.options = {}

        def option(self, key, value):
            self.options[key] = value
            return self

        def table(self, name):
            return ("table", name, dict(self.options))

    class Session:
        def __init__(self):
            self.read = Reader()

    session = Session()
    result = table(version=9).to_spark(session)
    assert result == ("table", "main.retail.orders", {"versionAsOf": 9})


def test_to_spark_applies_timestamp_as_of():
    class Reader:
        def __init__(self):
            self.options = {}

        def option(self, key, value):
            self.options[key] = value
            return self

        def table(self, name):
            return dict(self.options)

    class Session:
        def __init__(self):
            self.read = Reader()

    assert table(timestamp="2026-08-01").to_spark(Session()) == {
        "timestampAsOf": "2026-08-01"
    }


def test_schema_fields_are_read_from_the_catalog():
    assert table().schema_fields() == [
        {"name": "order_id", "type": "bigint"},
        {"name": "amount", "type": "decimal(10,2)"},
    ]


# ----------------------------------------------------------------------
# reads deltalake's Python reader cannot do at all, cluster or not
# ----------------------------------------------------------------------
AWS_CREDS = {
    "aws_temp_credentials": {
        "access_key_id": "AKIA",
        "secret_access_key": "secret",
        "session_token": "token",
    },
    "expiration_time": 4_000_000_000_000,
}


def _deletion_vector_table(tmp_path):
    """A real local Delta table with a deletion vector, not a mocked error.

    The failure this protects against was only discoverable by running the demo
    against real Databricks data, where deletion vectors are on by default for many
    write patterns on recent runtimes. Building one for real here, rather than
    mocking the exception, is what makes this test worth having: it breaks again if
    a future `deltalake` upgrade changes how this fails, or starts supporting it.
    """
    import pandas as pd
    from deltalake import DeltaTable, write_deltalake

    path = str(tmp_path / "dv_table")
    write_deltalake(
        path,
        pd.DataFrame({"id": range(5), "val": [str(i) for i in range(5)]}),
        mode="overwrite",
        configuration={"delta.enableDeletionVectors": "true"},
    )
    DeltaTable(path).delete("id = 2")
    return path


def _table_over(path, credentials=None):
    """A real UnityCatalogTable whose storage_location is a real local table.

    Not `_delta_table()` overridden: this exercises `credentials()` and
    `_read_via_duckdb()` exactly as a real run would, against the same table
    `_delta_table()` itself points at.
    """
    info = dict(TABLE_INFO)
    info["storage_location"] = path
    return table(client=FakeClient(info=info, credentials=credentials))


def test_deletion_vectors_fall_back_to_duckdbs_own_reader(tmp_path):
    """DuckDB's delta_scan applies deletion vectors where delta-rs's reader does not.

    Verified against a real local table, not a mocked exception: this is the
    substantive fix, not just a clearer error message. If a future `deltalake`
    upgrade starts supporting deletion vectors, this test keeps passing (the
    fallback just stops being exercised) rather than breaking.
    """
    path = _deletion_vector_table(tmp_path)
    ref = _table_over(path, credentials=AWS_CREDS)

    result = ref.to_arrow()
    assert sorted(result.column("id").to_pylist()) == [0, 1, 3, 4]  # id 2 was deleted


def test_deletion_vector_fallback_reads_the_pinned_version(tmp_path):
    path = _deletion_vector_table(tmp_path)
    info = dict(TABLE_INFO, storage_location=path)
    ref = table(client=FakeClient(info=info, credentials=AWS_CREDS), version=0)

    result = ref.to_arrow()
    assert sorted(result.column("id").to_pylist()) == [0, 1, 2, 3, 4]


def test_deletion_vectors_are_handled_from_every_materializing_read(tmp_path):
    path = _deletion_vector_table(tmp_path)
    for reader in ("to_pandas", "to_polars"):
        ref = _table_over(path, credentials=AWS_CREDS)
        assert len(getattr(ref, reader)()) == 4


def test_deletion_vectors_still_get_an_explanation_when_no_secret_can_be_built(
    tmp_path,
):
    """GCP has no DuckDB secret shape for what UC vends; nor does a plain local path.

    `storage_location` here is a local `tmp_path`, not an `abfss://` URL, so there is
    no account name to parse and no secret `_duckdb_secret_for` can build even with
    valid Azure-shaped credentials. This is a materially different situation from a
    refused grant, and this table's protocol, not a permissions problem, is what
    should show up in the message.
    """
    path = _deletion_vector_table(tmp_path)
    gcp_creds = {"gcp_oauth_token": {"oauth_token": "ya29.fake"}}
    ref = _table_over(path, credentials=gcp_creds)

    with pytest.raises(UnityCatalogError) as exc:
        ref.to_arrow()
    assert "deletionVectors" in str(exc.value)
    assert "query(" in str(exc.value)
    # The point of this fix: the log line itself says why, not just that it failed.
    assert "DuckDB fallback not used" in str(exc.value)
    assert "gcp_oauth_token" in str(exc.value)


def test_deletion_vectors_get_an_explanation_when_vending_is_also_refused():
    """A vending refusal on top of a protocol error should not obscure the cause.

    `credentials()` raising its own "refused to vend" message here would bury the
    real problem (the table's protocol) behind a second, unrelated-looking error.
    """
    ref = table(client=FakeClient(credentials=None))  # vending refused entirely
    original_exc = ValueError(
        "The table has set these reader features: {'deletionVectors'} but these "
        "are not yet supported by the deltalake reader."
    )

    with pytest.raises(UnityCatalogError) as exc:
        ref._read_via_duckdb(original_exc)
    assert "deletionVectors" in str(exc.value)
    assert "DuckDB fallback not used" in str(exc.value)
    assert "vending credentials for the fallback itself failed" in str(exc.value)


def test_deletion_vectors_explanation_says_when_duckdb_was_tried_and_failed(tmp_path):
    """AWS credentials build a secret, but the read itself can still fail.

    A wrong or expired credential, a storage location DuckDB cannot reach, or any
    other duckdb-side failure should say so distinctly from "no secret could be
    built", since those point at different things to fix. A nonexistent local path
    fails this way fast and without any network access, which is what makes it a
    good stand-in for a real auth/network failure in a test.
    """
    missing_path = str(tmp_path / "does-not-exist")
    ref = _table_over(missing_path, credentials=AWS_CREDS)
    original_exc = ValueError(
        "The table has set these reader features: {'deletionVectors'} but these "
        "are not yet supported by the deltalake reader."
    )

    with pytest.raises(UnityCatalogError) as exc:
        ref._read_via_duckdb(original_exc)
    assert "deletionVectors" in str(exc.value)
    assert "DuckDB's own reader also failed" in str(exc.value)


def test_azure_fallback_uses_the_curl_transport(monkeypatch):
    """Without it, DuckDB's Azure reader fails on images lacking the SDK's CA path."""
    import sys
    import types

    statements = []

    class Connection:
        def install_extension(self, name):
            pass

        load_extension = install_extension

        def execute(self, sql, params=None):
            statements.append(sql)

        def sql(self, sql, params=None):
            raise RuntimeError("stop before reading")

        def close(self):
            pass

    fake_duckdb = types.SimpleNamespace(connect=Connection)
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    info = dict(
        TABLE_INFO,
        storage_location="abfss://data@acct.dfs.core.windows.net/retail/orders",
    )
    sas = {"azure_user_delegation_sas": {"sas_token": "sv=2024"}}
    ref = table(client=FakeClient(info=info, credentials=sas))

    with pytest.raises(UnityCatalogError, match="stop before reading"):
        ref._read_via_duckdb(ValueError("deletionVectors"))
    assert statements[0] == "SET azure_transport_option_type = 'curl'"


# ----------------------------------------------------------------------
# the Azure SAS fallback: verified as far as this suite can verify anything
# without a real Azure storage account (see _read_via_duckdb's docstring)
# ----------------------------------------------------------------------
def test_azure_storage_account_is_parsed_from_the_container_qualified_form():
    from metaflow_extensions.spark.plugins.catalog.unity import _azure_storage_account

    assert (
        _azure_storage_account(
            "abfss://mycontainer@mystorageacct.dfs.core.windows.net/retail/orders"
        )
        == "mystorageacct"
    )


def test_azure_storage_account_is_parsed_from_the_fully_qualified_form():
    from metaflow_extensions.spark.plugins.catalog.unity import _azure_storage_account

    assert (
        _azure_storage_account(
            "abfss://mystorageacct.dfs.core.windows.net/mycontainer/path"
        )
        == "mystorageacct"
    )


def test_azure_storage_account_is_none_for_a_non_url_storage_location():
    from metaflow_extensions.spark.plugins.catalog.unity import _azure_storage_account

    assert _azure_storage_account("/local/path/to/table") is None
    assert _azure_storage_account(None) is None
    assert _azure_storage_account("") is None


def test_azure_sas_credentials_build_the_connection_string_duckdb_requires():
    """`AccountName=` has to be present, or DuckDB's own extension rejects it.

    Found by actually reproducing the failure against a real Azure workspace, not
    predicted in advance: `BlobEndpoint=...;SharedAccessSignature=...` alone is
    Microsoft's own documented format and is what the first version of this fallback
    shipped with, but duckdb-azure's `ConnectionStringMatchStorageAccountName` (in
    `azure_storage_account_client.cpp`) requires `AccountName=` to literally appear
    and match the account parsed from the `abfss://` URL, and throws "A invalid
    connection string has been provided" before ever reaching the real Azure SDK
    parser if it does not. What is still not confirmed anywhere in this suite is that
    DuckDB's `azure` extension actually authenticates a `delta_scan` over `abfss://`
    with this connection string; that needs a real Azure storage account.
    """
    ref = table(
        client=FakeClient(
            info={
                **TABLE_INFO,
                "storage_location": (
                    "abfss://mycontainer@mystorageacct.dfs.core.windows.net/orders"
                ),
            }
        )
    )
    secret = ref._duckdb_secret_for(
        {"azure_user_delegation_sas": {"sas_token": "sv=2024-08-04&ss=b&sig=abc"}}
    )
    assert secret is not None
    extension, sql, params = secret
    assert extension == "azure"
    assert "CONNECTION_STRING" in sql
    assert params == [
        "AccountName=mystorageacct;"
        "BlobEndpoint=https://mystorageacct.blob.core.windows.net;"
        "SharedAccessSignature=sv=2024-08-04&ss=b&sig=abc"
    ]


def test_azure_connection_string_passes_duckdbs_own_account_name_check():
    """A direct port of duckdb-azure's own check, not just a plausible-looking string.

    From `ConnectionStringMatchStorageAccountName` in
    `azure_storage_account_client.cpp`: finds "AccountName=" and compares the
    following `len(account)` characters against the account name it parsed from the
    `abfss://` URL. If this test passes, DuckDB's own pre-check passes; whether the
    real Azure SDK then authenticates successfully is a separate question this test
    does not answer.
    """
    ref = table(
        client=FakeClient(
            info={
                **TABLE_INFO,
                "storage_location": (
                    "abfss://mycontainer@mystorageacct.dfs.core.windows.net/orders"
                ),
            }
        )
    )
    _, _, params = ref._duckdb_secret_for(
        {"azure_user_delegation_sas": {"sas_token": "sv=2024"}}
    )
    connection_string = params[0]

    pos = connection_string.find("AccountName=")
    assert pos != -1, "duckdb-azure raises here if AccountName= is missing at all"
    account = "mystorageacct"
    start = pos + len("AccountName=")
    assert connection_string[start : start + len(account)] == account


def test_azure_sas_with_no_parseable_account_builds_no_secret():
    ref = table()  # default storage_location is a local-looking path, no account
    ref.storage_location = "not-a-url"
    secret = ref._duckdb_secret_for(
        {"azure_user_delegation_sas": {"sas_token": "sv=2024"}}
    )
    assert secret is None


def test_aws_credentials_build_the_s3_secret():
    ref = table()
    secret = ref._duckdb_secret_for(
        {
            "aws_temp_credentials": {
                "access_key_id": "AKIA",
                "secret_access_key": "secret",
                "session_token": "token",
            }
        }
    )
    assert secret is not None
    extension, sql, params = secret
    assert extension == "httpfs"
    assert "TYPE s3" in sql
    assert params[:3] == ["AKIA", "secret", "token"]


def test_gcp_and_azure_aad_credentials_build_no_secret():
    ref = table()
    assert ref._duckdb_secret_for({"gcp_oauth_token": {"oauth_token": "x"}}) is None
    assert ref._duckdb_secret_for({"azure_aad": {"aad_token": "x"}}) is None


def test_other_protocol_errors_get_the_generic_explanation(monkeypatch):
    import metaflow_extensions.spark.plugins.catalog.unity as unity_mod

    class FakeProtocolError(Exception):
        pass

    monkeypatch.setattr(
        unity_mod, "_delta_protocol_error_types", lambda: (FakeProtocolError,)
    )

    ref = table()

    class ExplodingDelta:
        def version(self):
            return 0

        def to_pyarrow_dataset(self):
            raise FakeProtocolError("some other unsupported reader feature")

    ref._delta_table = lambda: ExplodingDelta()

    with pytest.raises(UnityCatalogError) as exc:
        ref.to_arrow()
    assert "some other unsupported reader feature" in str(exc.value)
    assert "query(" in str(exc.value)


def test_unrelated_exceptions_are_not_swallowed_by_the_protocol_translator():
    ref = table()

    class ExplodingDelta:
        def to_pyarrow_dataset(self):
            raise RuntimeError("network blip, nothing to do with the protocol")

    ref._delta_table = lambda: ExplodingDelta()

    with pytest.raises(RuntimeError, match="network blip"):
        ref.to_arrow()
