"""Tests for SQL warehouse statement execution.

The behaviour worth protecting: parameters are bound, never string-formatted into the
statement; state mapping matches the shared PENDING/RUNNING/SUCCESS/FAILED/CANCELLED
vocabulary; and the shared wait/cancel driver in backends/__init__.py works against this
backend with no special-casing.
"""

import decimal
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa
import pytest
from databricks.sdk.service.sql import (
    ResultData,
    ResultManifest,
    ResultSchema,
    ServiceError,
    ServiceErrorCode,
    StatementResponse,
    StatementState,
    StatementStatus,
)

from metaflow_extensions.spark.plugins.backends.databricks.sql import (
    DatabricksSqlBackend,
    _bind_parameters,
    _manifest_schema,
    _sql_type_and_value,
)
from metaflow_extensions.spark.plugins.context import (
    JobHandle,
    JobState,
    TaskContext,
)
from metaflow_extensions.spark.plugins.exceptions import (
    SparkConfigError,
    SparkException,
)


def make_ctx(tags=None):
    return TaskContext(
        step_name="analyze",
        pathspec="RetailFlow/42/analyze/7",
        flow_name="RetailFlow",
        run_id="42",
        task_id="7",
        attempt=0,
        user="tester",
        tags=tags or {},
    )


class FakeStatementExecution:
    """Stands in for `client.sdk.statement_execution`."""

    def __init__(self, responses=None):
        # queue of StatementResponse to return from get_statement(), in order.
        self.responses = list(responses or [])
        self.executed = []
        self.cancelled = []
        self.chunks_requested = []

    def execute_statement(
        self,
        statement,
        warehouse_id,
        *,
        disposition=None,
        format=None,
        wait_timeout=None,
        parameters=None,
        query_tags=None,
        catalog=None,
        schema=None,
        row_limit=None,
        byte_limit=None,
    ):
        self.executed.append(
            {
                "statement": statement,
                "warehouse_id": warehouse_id,
                "disposition": disposition,
                "format": format,
                "wait_timeout": wait_timeout,
                "parameters": parameters,
                "query_tags": query_tags,
                "catalog": catalog,
                "schema": schema,
                "row_limit": row_limit,
                "byte_limit": byte_limit,
            }
        )
        if self.responses:
            return self.responses.pop(0)
        return StatementResponse(
            statement_id="stmt-1", status=StatementStatus(state=StatementState.PENDING)
        )

    def get_statement(self, statement_id):
        if self.responses:
            return self.responses.pop(0)
        return StatementResponse(
            statement_id=statement_id,
            status=StatementStatus(state=StatementState.SUCCEEDED),
        )

    def cancel_execution(self, statement_id):
        self.cancelled.append(statement_id)

    def get_statement_result_chunk_n(self, statement_id, chunk_index):
        self.chunks_requested.append((statement_id, chunk_index))
        return ResultData(chunk_index=chunk_index)


class FakeClient:
    def __init__(self, service=None):
        self.sdk = _Sdk(service or FakeStatementExecution())

    def warehouse_url(self, warehouse_id):
        return "https://workspace/sql/warehouses/%s" % warehouse_id


class _Sdk:
    def __init__(self, service):
        self.statement_execution = service


def make_backend(config=None, service=None, ctx=None):
    backend = DatabricksSqlBackend(
        config or {"warehouse_id": "wh-1"}, ctx or make_ctx()
    )
    backend.client = FakeClient(service)
    return backend


# ----------------------------------------------------------------------
# parameter binding: the point of this backend, not an afterthought
# ----------------------------------------------------------------------
def test_string_parameter_is_never_interpolated_into_the_statement():
    items = _bind_parameters({"name": "'; DROP TABLE orders; --"})
    assert len(items) == 1
    assert items[0].name == "name"
    assert items[0].value == "'; DROP TABLE orders; --"
    assert items[0].type == "STRING"


def test_bool_is_bound_as_boolean_not_int():
    assert _sql_type_and_value(True) == ("BOOLEAN", "true")
    assert _sql_type_and_value(False) == ("BOOLEAN", "false")


def test_small_int_is_bound_as_int_large_as_long():
    assert _sql_type_and_value(5)[0] == "INT"
    assert _sql_type_and_value(2**40)[0] == "LONG"


def test_date_and_datetime_are_distinguished():
    assert _sql_type_and_value(date(2026, 1, 1)) == ("DATE", "2026-01-01")
    sql_type, value = _sql_type_and_value(datetime(2026, 1, 1, 12, 30, 0))
    assert sql_type == "TIMESTAMP"
    assert value.startswith("2026-01-01 12:30:00")


def test_an_aware_datetime_keeps_its_offset():
    plus_five = timezone(timedelta(hours=5))
    value = datetime(2026, 1, 1, 12, 0, tzinfo=plus_five)
    assert _sql_type_and_value(value) == (
        "TIMESTAMP",
        "2026-01-01 12:00:00.000000+05:00",
    )


def test_decimal_carries_precision_and_scale():
    sql_type, value = _sql_type_and_value(decimal.Decimal("12.50"))
    assert sql_type == "DECIMAL(4,2)"
    assert value == "12.50"


@pytest.mark.parametrize(
    "value,expected",
    [
        ("1E+3", ("DECIMAL(4,0)", "1000")),
        ("0.05", ("DECIMAL(3,2)", "0.05")),
        ("-1.5E-3", ("DECIMAL(5,4)", "-0.0015")),
    ],
)
def test_decimal_exponents_bind_in_fixed_notation(value, expected):
    assert _sql_type_and_value(decimal.Decimal(value)) == expected


@pytest.mark.parametrize("value", ["NaN", "Infinity", "1E+40"])
def test_decimals_sql_cannot_hold_are_rejected(value):
    with pytest.raises(SparkConfigError, match="DECIMAL"):
        _sql_type_and_value(decimal.Decimal(value))


def test_bytes_are_rejected_with_the_hex_workaround():
    with pytest.raises(SparkConfigError, match="unhex"):
        _sql_type_and_value(b"ab")


def test_none_binds_as_null():
    assert _sql_type_and_value(None) == ("STRING", None)


def test_no_parameters_is_none_not_an_empty_list():
    assert _bind_parameters({}) is None
    assert _bind_parameters(None) is None


# ----------------------------------------------------------------------
# submit
# ----------------------------------------------------------------------
def test_submit_sends_the_statement_and_named_parameters():
    service = FakeStatementExecution()
    backend = make_backend(
        config={
            "warehouse_id": "wh-1",
            "statement": "SELECT * FROM t WHERE id = :id",
            "parameters": {"id": 5},
        },
        service=service,
    )
    handle = backend.submit(backend.ctx)
    assert handle.job_id == "stmt-1"
    assert handle.ui_url == "https://workspace/sql/warehouses/wh-1"
    kwargs = service.executed[0]
    assert kwargs["statement"] == "SELECT * FROM t WHERE id = :id"
    assert kwargs["warehouse_id"] == "wh-1"
    assert kwargs["wait_timeout"] == "0s"
    assert kwargs["parameters"][0].name == "id"


def test_submit_without_a_warehouse_id_fails_before_any_request():
    backend = make_backend(config={"statement": "SELECT 1"})
    with pytest.raises(SparkConfigError, match="warehouse_id"):
        backend.submit(backend.ctx)


def test_submit_without_a_statement_fails_before_any_request():
    backend = make_backend(config={"warehouse_id": "wh-1"})
    with pytest.raises(SparkConfigError, match="statement"):
        backend.submit(backend.ctx)


def test_query_tags_carry_the_pathspec():
    service = FakeStatementExecution()
    ctx = make_ctx(tags={"metaflow_flow": "RetailFlow", "metaflow_run_id": "42"})
    backend = make_backend(
        config={"warehouse_id": "wh-1", "statement": "SELECT 1"},
        service=service,
        ctx=ctx,
    )
    backend.submit(ctx)
    tags = {t.key: t.value for t in service.executed[0]["query_tags"]}
    assert tags["metaflow_flow"] == "RetailFlow"


def test_query_tags_are_skipped_against_an_older_sdk_that_does_not_accept_them():
    class OldStyleService(FakeStatementExecution):
        """Mimics an sdk predating query_tags: no such named parameter at all."""

        def execute_statement(self, statement, warehouse_id, **kwargs):
            assert "query_tags" not in kwargs
            self.executed.append({"statement": statement, "warehouse_id": warehouse_id})
            return StatementResponse(
                statement_id="stmt-1",
                status=StatementStatus(state=StatementState.PENDING),
            )

    service = OldStyleService()
    ctx = make_ctx(tags={"metaflow_flow": "RetailFlow"})
    backend = make_backend(
        config={"warehouse_id": "wh-1", "statement": "SELECT 1"},
        service=service,
        ctx=ctx,
    )
    # Must not raise, and must not attempt to send a tag the SDK cannot accept.
    handle = backend.submit(ctx)
    assert handle.job_id == "stmt-1"


def test_a_request_level_rejection_is_reported_clearly():
    class Rejecting(FakeStatementExecution):
        def execute_statement(self, **kwargs):
            raise RuntimeError("PERMISSION_DENIED: no USE on this warehouse")

    backend = make_backend(
        config={"warehouse_id": "wh-1", "statement": "SELECT 1"},
        service=Rejecting(),
    )
    with pytest.raises(SparkException, match="PERMISSION_DENIED"):
        backend.submit(backend.ctx)


# ----------------------------------------------------------------------
# state mapping
# ----------------------------------------------------------------------
@pytest.mark.parametrize(
    "state,expected",
    [
        (StatementState.PENDING, JobState.PENDING),
        (StatementState.RUNNING, JobState.RUNNING),
        (StatementState.SUCCEEDED, JobState.SUCCESS),
        (StatementState.CLOSED, JobState.SUCCESS),
        (StatementState.FAILED, JobState.FAILED),
        (StatementState.CANCELED, JobState.CANCELLED),
    ],
)
def test_state_mapping(state, expected):
    backend = make_backend()
    handle = JobHandle(backend="databricks-sql", job_id="stmt-1")
    response = StatementResponse(
        statement_id="stmt-1", status=StatementStatus(state=state)
    )
    status = backend._status(response, handle)
    assert status.state == expected


def test_failure_carries_the_error_class_and_message():
    backend = make_backend()
    handle = JobHandle(backend="databricks-sql", job_id="stmt-1")
    response = StatementResponse(
        statement_id="stmt-1",
        status=StatementStatus(
            state=StatementState.FAILED,
            error=ServiceError(
                error_code=ServiceErrorCode.BAD_REQUEST,
                message="[PARSE_SYNTAX_ERROR] ...",
            ),
        ),
    )
    status = backend._status(response, handle)
    assert status.error_class == "BAD_REQUEST"
    assert "PARSE_SYNTAX_ERROR" in status.message


# ----------------------------------------------------------------------
# cancellation, via the shared driver in backends/__init__.py
# ----------------------------------------------------------------------
def test_cancel_calls_cancel_execution():
    service = FakeStatementExecution()
    backend = make_backend(service=service)
    backend.cancel(JobHandle(backend="databricks-sql", job_id="stmt-1"))
    assert service.cancelled == ["stmt-1"]


# ----------------------------------------------------------------------
# result collection
# ----------------------------------------------------------------------
def test_manifest_schema_maps_known_types_and_falls_back_for_unknown_ones():
    manifest = ResultManifest(
        schema=ResultSchema(
            columns=[
                _column("id", "LONG"),
                _column("tags", "ARRAY"),
            ]
        )
    )
    schema = _manifest_schema(manifest)
    assert schema.field("id").type == pa.int64()
    assert schema.field("tags").type == pa.string()


def test_manifest_schema_is_empty_when_there_are_no_columns():
    assert _manifest_schema(None) == pa.schema([])


def test_empty_result_builds_a_table_from_the_manifest_schema():
    manifest = ResultManifest(
        schema=ResultSchema(
            columns=[
                _column("id", "LONG"),
                _column("name", "STRING"),
            ]
        )
    )
    response = StatementResponse(
        statement_id="stmt-1",
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=manifest,
        result=ResultData(external_links=[]),
    )
    backend = make_backend()
    table = backend._collect_arrow("stmt-1", response)
    assert table.num_rows == 0
    assert table.schema.names == ["id", "name"]
    assert table.schema.field("id").type == pa.int64()


def test_chunk_pagination_follows_next_chunk_index():
    first_link = _external_link(0, next_chunk_index=1)
    service = FakeStatementExecution()
    backend = make_backend(service=service)
    response = StatementResponse(
        statement_id="stmt-1",
        status=StatementStatus(state=StatementState.SUCCEEDED),
        manifest=ResultManifest(),
        result=ResultData(external_links=[first_link]),
    )

    calls = []

    def fake_read(link):
        calls.append(link.chunk_index)
        return []

    import metaflow_extensions.spark.plugins.backends.databricks.sql as sql_mod

    original = sql_mod._read_arrow_stream
    sql_mod._read_arrow_stream = fake_read
    try:
        table = backend._collect_arrow("stmt-1", response)
    finally:
        sql_mod._read_arrow_stream = original

    assert calls == [0]
    assert service.chunks_requested == [("stmt-1", 1)]
    assert table.num_rows == 0


def _column(name, type_name):
    from databricks.sdk.service.sql import ColumnInfo, ColumnInfoTypeName

    return ColumnInfo(name=name, type_name=ColumnInfoTypeName[type_name])


def _external_link(chunk_index, next_chunk_index=None):
    from databricks.sdk.service.sql import ExternalLink

    return ExternalLink(
        chunk_index=chunk_index,
        next_chunk_index=next_chunk_index,
        external_link="https://example.com/chunk-%d" % chunk_index,
    )
