"""
Tests for management-ui/backend/main.py's archive search endpoints (Athena/
Glue query layer over the archive's Parquet index).

Redis, Athena, and S3 are all faked with small in-memory stand-ins rather
than MagicMocks, since these endpoints are read-modify-write against
records/state a single static mock return value can't reflect across a
POST -> poll -> GET/DELETE sequence within the same test.

main.py is loaded directly by file path (same workaround test_main.py
uses) rather than via a normal package import, since the hyphen in
"management-ui" isn't a valid Python identifier.
"""

from __future__ import annotations

import gzip
import importlib.util
import json
import os
import re
import sys
from contextlib import contextmanager
from datetime import date, datetime, timezone
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

_BACKEND_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_spec = importlib.util.spec_from_file_location(
    "management_ui_main", os.path.join(_BACKEND_DIR, "main.py")
)
ui_main = importlib.util.module_from_spec(_spec)
sys.modules["management_ui_main"] = ui_main
_spec.loader.exec_module(ui_main)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeRedis:
    """Minimal in-memory stand-in supporting exactly what archive search
    needs (get/set with ex/xx/keepttl, delete, sadd/srem/smembers for the
    archive_search:index set) plus no-op stubs for script_load/evalsha,
    since lifespan() unconditionally loads the rules/areas Lua scripts
    regardless of which endpoints a given test actually exercises."""

    def __init__(self):
        self.store: dict[str, str] = {}
        self.sets: dict[str, set[str]] = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value, ex=None, xx=False, nx=False, keepttl=False):
        if xx and key not in self.store:
            return None
        if nx and key in self.store:
            return None
        self.store[key] = value
        return True

    def delete(self, key):
        self.store.pop(key, None)

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(members)

    def srem(self, key, *members):
        self.sets.get(key, set()).difference_update(members)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def script_load(self, script: str) -> str:
        return "fake-sha"

    def evalsha(self, sha: str, numkeys: int, *args):
        return None

    def ft(self, index: str) -> "_FakeFt":
        # lifespan() unconditionally ensures all three RediSearch indices
        # exist at startup (see #934) regardless of which endpoints a given
        # test in this file actually exercises -- archive search doesn't
        # use search indices at all, so this just reports every index as
        # already present and never needs create_index() to do anything.
        return _FakeFt()


class _FakeFt:
    def info(self):
        return {}

    def create_index(self, fields, definition):
        raise AssertionError("create_index() should not be called -- info() always reports the index as present")


class FakeAthenaClient:
    """Each start_query_execution call gets its own incrementing
    QueryExecutionId. Defaults the new execution straight to SUCCEEDED
    (not Athena's real initial RUNNING state) so tests using the
    synchronous-thread patch don't spin the real poll loop against a
    perpetually-running fake for up to two real wall-clock minutes --
    tests that specifically need RUNNING/FAILED/timeout behavior set
    .executions[qid]["State"] (and ["Reason"]) themselves right after
    start_query_execution returns, before the synchronous thread's first
    poll ever runs."""

    def __init__(self, bucket: str = "test-bucket"):
        self._bucket = bucket
        self.executions: dict[str, dict] = {}
        self.started_queries: list[dict] = []
        self.stopped: list[str] = []
        self._next_id = 1
        # QueryExecutionId -> list of data-row tuples (column order matching
        # whatever SELECT that execution's query actually used) -- what
        # get_query_results serves back for that id. Never touches fake_s3;
        # this models Athena's own result-rows API, independent of the CSV
        # file Athena separately writes to S3 for the same execution.
        self.results: dict[str, list[tuple]] = {}
        # Set by a test to simulate an AWS-side failure (e.g. a permissions
        # mismatch) on the next call -- covers the 502 paths in main.py that
        # a plain state-transition can't exercise.
        self.raise_on_start: Optional[Exception] = None
        self.raise_on_get_query_execution: Optional[Exception] = None
        self.raise_on_get_query_results: Optional[Exception] = None

    def start_query_execution(self, QueryString, QueryExecutionContext=None, WorkGroup=None):
        if self.raise_on_start is not None:
            raise self.raise_on_start
        qid = f"exec-{self._next_id}"
        self._next_id += 1
        self.started_queries.append({
            "QueryString": QueryString,
            "QueryExecutionContext": QueryExecutionContext,
            "WorkGroup": WorkGroup,
        })
        self.executions[qid] = {
            "State": "SUCCEEDED",
            "OutputLocation": f"s3://{self._bucket}/athena-results/{qid}.csv",
            "Reason": "",
        }
        return {"QueryExecutionId": qid}

    def get_query_execution(self, QueryExecutionId):
        if self.raise_on_get_query_execution is not None:
            raise self.raise_on_get_query_execution
        info = self.executions[QueryExecutionId]
        status = {"State": info["State"]}
        if info["State"] in ("FAILED", "CANCELLED"):
            status["StateChangeReason"] = info["Reason"]
        return {
            "QueryExecution": {
                "Status": status,
                "ResultConfiguration": {"OutputLocation": info["OutputLocation"]},
            }
        }

    def get_query_results(self, QueryExecutionId, MaxResults=1000, NextToken=None):
        """Row 0 is always the column header (real Athena behavior, which
        main.py's _fetch_and_cache_results skips) -- unlike real Athena,
        MaxResults here bounds DATA rows only (not the header), matching how
        main.py requests _RESULT_ROW_CAP + 1 to learn whether a
        (_RESULT_ROW_CAP + 1)th row exists in a single call."""
        if self.raise_on_get_query_results is not None:
            raise self.raise_on_get_query_results
        all_rows = self.results.get(QueryExecutionId, [])
        sliced = all_rows[:MaxResults] if MaxResults is not None else all_rows
        header = {"Data": [{"VarCharValue": "column_header"}]}
        data = [{"Data": [{"VarCharValue": "" if v is None else str(v)} for v in row]} for row in sliced]
        return {"ResultSet": {"Rows": [header, *data]}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)
        self.executions[QueryExecutionId]["State"] = "CANCELLED"


class FakeS3Client:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []
        self.presigned_calls: list[dict] = []
        self.raise_on_presign: Optional[Exception] = None

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(f"no such key: {Key}")
        body = MagicMock()
        body.read.return_value = self.objects[Key]
        return {"Body": body}

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.objects.pop(Key, None)

    def generate_presigned_url(self, ClientMethod, Params, ExpiresIn=3600):
        if self.raise_on_presign is not None:
            raise self.raise_on_presign
        self.presigned_calls.append({"Params": Params, "ExpiresIn": ExpiresIn})
        return f"https://{Params['Bucket']}.s3.amazonaws.com/{Params['Key']}?presigned=1&expires={ExpiresIn}"


def _csv_body(rows: list[tuple]) -> bytes:
    header = "icao_hex,registration,type_designator,military,operator_designator,ident,first_message,last_message,s3_key\n"
    body = "\n".join(",".join(str(v) for v in row) for row in rows)
    return (header + body + "\n").encode("utf-8")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@contextmanager
def _synchronous_thread():
    """Patch threading.Thread so the polling thread runs synchronously in
    the caller's thread instead of racing the test's own assertions
    against a real background thread, and patch time.sleep to a no-op for
    the same scope only -- ui_main.time is the real stdlib time module (a
    process-wide singleton), so leaving this patch active any longer than
    this one synchronous call risks starving anyio/ASGI internals that
    also rely on real sleep behavior."""
    class _ImmediateThread:
        def __init__(self, target=None, args=(), daemon=None, name=None):
            self._target = target
            self._args = args

        def start(self):
            if self._target:
                self._target(*self._args)

    with patch("management_ui_main.threading.Thread", _ImmediateThread), \
         patch.object(ui_main.time, "sleep", lambda *_a, **_k: None):
        yield


@contextmanager
def _frozen_today(today: date):
    """Pins ui_main's `datetime.now(timezone.utc)` to noon UTC on `today` --
    create_archive_search's "tomorrow UTC" default and _resolve_search_range
    both read the clock through this, so a test asserting an exact resolved
    range needs it pinned rather than racing the real clock."""
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(today.year, today.month, today.day, 12, 0, 0, tzinfo=tz)

    with patch.object(ui_main, "datetime", _Frozen):
        yield


def _configure_env(monkeypatch, tmp_path) -> None:
    for name, value in {
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "REDIS_PASSWORD": "test-password",
        "S3_BUCKET": "test-bucket",
        "AWS_DEFAULT_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "x",
        "AWS_SECRET_ACCESS_KEY": "x",
        "ATHENA_WORKGROUP": "skyfollower",
        "ATHENA_DATABASE": "skyfollower",
        "ATHENA_TABLE": "archive_flights",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("DATA_DIR", str(tmp_path / "data"))


@pytest.fixture
def fake_redis():
    return FakeRedis()


@pytest.fixture
def fake_athena():
    return FakeAthenaClient()


@pytest.fixture
def fake_s3():
    return FakeS3Client()


@pytest.fixture
def client(tmp_path, monkeypatch, fake_redis, fake_athena, fake_s3):
    _configure_env(monkeypatch, tmp_path)

    class FakeSession:
        def __init__(self, *a, **k):
            pass

        def client(self, name):
            return {"s3": fake_s3, "athena": fake_athena}[name]

    with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis), \
         patch.object(ui_main, "boto3", MagicMock(Session=FakeSession)):
        with TestClient(ui_main.app) as c:
            yield c


def _create_search(
    client, name="test search", where_clause="icao_hex = 'A8AE7F'",
    start_date=None, end_date=None,
):
    body = {"name": name, "where_clause": where_clause}
    if start_date is not None:
        body["start_date"] = start_date
    if end_date is not None:
        body["end_date"] = end_date
    with _synchronous_thread():
        resp = client.post("/api/archive/search", json=body)
    return resp


# ---------------------------------------------------------------------------
# Query validation
# ---------------------------------------------------------------------------

class TestWhereClauseValidation:
    def test_empty_where_clause_rejected(self, client):
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "   "})
        assert resp.status_code == 400

    def test_semicolon_rejected(self, client):
        resp = client.post(
            "/api/archive/search", json={"name": "x", "where_clause": "icao_hex = 'A'; DROP TABLE x"}
        )
        assert resp.status_code == 400

    @pytest.mark.parametrize("keyword", ["DROP", "CREATE", "ALTER", "INSERT", "DELETE", "UPDATE", "GRANT"])
    def test_forbidden_keywords_rejected(self, client, keyword):
        resp = client.post(
            "/api/archive/search", json={"name": "x", "where_clause": f"1=1 {keyword} something"}
        )
        assert resp.status_code == 400

    def test_keyword_substring_not_false_positive(self, client, fake_athena):
        """ident = 'INSERT1' must not trip the forbidden-keyword guard --
        word-boundary matching, not a plain substring search."""
        with _synchronous_thread():
            resp = client.post(
                "/api/archive/search", json={"name": "x", "where_clause": "ident = 'INSERT1'"}
            )
        assert resp.status_code == 202

    def test_double_quoted_string_literal_rejected(self, client):
        """operator_designator = "DAL" is the classic mistake: Athena parses
        the double-quoted token as a column reference, not a string
        literal. Must be caught before ever reaching Athena."""
        resp = client.post(
            "/api/archive/search",
            json={"name": "x", "where_clause": 'operator_designator = "DAL"'},
        )
        assert resp.status_code == 400
        assert "DAL" in resp.json()["detail"]

    def test_legitimately_quoted_column_name_accepted(self, client, fake_athena):
        """"operator_designator" = 'DAL' double-quotes an actual column
        name, which is valid ANSI SQL -- must not be flagged."""
        with _synchronous_thread():
            resp = client.post(
                "/api/archive/search",
                json={"name": "x", "where_clause": '"operator_designator" = \'DAL\''},
            )
        assert resp.status_code == 202

    def test_no_double_quotes_unaffected(self, client, fake_athena):
        with _synchronous_thread():
            resp = client.post(
                "/api/archive/search", json={"name": "x", "where_clause": "ident = 'DAL123'"}
            )
        assert resp.status_code == 202


class TestQueryConstruction:
    def test_select_list_and_table_are_backend_controlled(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            _create_search(client, where_clause="icao_hex = 'A8AE7F'")
        query = fake_athena.started_queries[0]["QueryString"]
        # No derivable timestamp predicate -> full default range
        # (_ARCHIVE_EPOCH .. tomorrow UTC of the frozen clock).
        assert query.startswith(
            "SELECT icao_hex, registration, type_designator, military, "
            "operator_designator, ident, first_message, last_message, s3_key "
            "FROM skyfollower.archive_flights WHERE "
            "((year='2022') OR (year='2023') OR (year='2024') OR (year='2025') "
            "OR (year='2026' AND month BETWEEN '01' AND '08') "
            "OR (year='2026' AND month='09' AND day BETWEEN '01' AND '04')) "
            "AND (icao_hex = 'A8AE7F')"
        )

    def test_where_clause_is_parenthesized_after_the_partition_predicate(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            _create_search(client, where_clause="a = 1 OR b = 2")
        query = fake_athena.started_queries[0]["QueryString"]
        assert query.endswith("AND (a = 1 OR b = 2)")

    def test_explicit_range_narrows_the_partition_predicate(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            _create_search(
                client, where_clause="icao_hex = 'A8AE7F'",
                start_date="2026-09-01", end_date="2026-09-01",
            )
        query = fake_athena.started_queries[0]["QueryString"]
        assert "WHERE ((year='2026' AND month='09' AND day BETWEEN '01' AND '01')) AND (icao_hex = 'A8AE7F')" in query


# ---------------------------------------------------------------------------
# Partition predicate generator -- coarsest-clause-per-span, from the
# worked-examples table.
# ---------------------------------------------------------------------------

class TestPartitionPredicate:
    def test_single_day(self):
        got = ui_main._partition_predicate(date(2026, 9, 1), date(2026, 9, 1))
        assert got == "(year='2026' AND month='09' AND day BETWEEN '01' AND '01')"

    def test_day_range_within_a_month(self):
        got = ui_main._partition_predicate(date(2026, 9, 3), date(2026, 9, 10))
        assert got == "(year='2026' AND month='09' AND day BETWEEN '03' AND '10')"

    def test_range_spanning_two_months(self):
        got = ui_main._partition_predicate(date(2026, 8, 3), date(2026, 9, 2))
        assert got == (
            "(year='2026' AND month='08' AND day BETWEEN '03' AND '31') "
            "OR (year='2026' AND month='09' AND day BETWEEN '01' AND '02')"
        )

    def test_whole_month_collapses_even_on_a_28_day_february(self):
        got = ui_main._partition_predicate(date(2026, 2, 1), date(2026, 2, 28))
        assert got == "(year='2026' AND month='02')"

    def test_whole_month_collapses_on_a_29_day_leap_february(self):
        got = ui_main._partition_predicate(date(2024, 2, 1), date(2024, 2, 29))
        assert got == "(year='2024' AND month='02')"

    def test_contiguous_whole_months_collapse_to_a_month_range(self):
        got = ui_main._partition_predicate(date(2026, 1, 1), date(2026, 8, 31))
        assert got == "(year='2026' AND month BETWEEN '01' AND '08')"

    def test_multi_year_range_mixes_whole_years_months_and_days(self):
        got = ui_main._partition_predicate(date(2022, 1, 1), date(2026, 9, 3))
        assert got == (
            "(year='2022') OR (year='2023') OR (year='2024') OR (year='2025') "
            "OR (year='2026' AND month BETWEEN '01' AND '08') "
            "OR (year='2026' AND month='09' AND day BETWEEN '01' AND '03')"
        )

    def test_start_equals_end_is_valid(self):
        got = ui_main._partition_predicate(date(2026, 1, 1), date(2026, 1, 1))
        assert got == "(year='2026' AND month='01' AND day BETWEEN '01' AND '01')"

    def test_whole_multi_year_span_collapses_to_bare_years(self):
        got = ui_main._partition_predicate(date(2022, 1, 1), date(2023, 12, 31))
        assert got == "(year='2022') OR (year='2023')"


# ---------------------------------------------------------------------------
# _ARCHIVE_EPOCH must stay coupled to the Glue table's own partition
# projection lower bound -- a range wider than the projection can never
# match anything.
# ---------------------------------------------------------------------------

class TestArchiveEpochCoupling:
    def test_archive_epoch_matches_glue_projection_year_range_lower_bound(self):
        from shared.glue_projection import YEAR_RANGE
        assert ui_main._ARCHIVE_EPOCH == date(YEAR_RANGE[0], 1, 1)

    def test_archive_epoch_not_earlier_than_glue_projection_year_range(self):
        from shared.glue_projection import YEAR_RANGE
        assert ui_main._ARCHIVE_EPOCH.year >= YEAR_RANGE[0]


# ---------------------------------------------------------------------------
# Partition-range derivation from the WHERE clause's own timestamp
# predicates (see _derive_bounds) -- the highest-risk part of this change.
# Vectors mirror the issue's own "Deriving the partition range" table.
# ---------------------------------------------------------------------------

class TestDeriveBounds:
    def test_between_gives_both_bounds(self):
        lo, hi = ui_main._derive_bounds(
            "first_message BETWEEN timestamp '2026-09-01 11:00:00' AND timestamp '2026-09-01 13:00:00'"
        )
        assert (lo, hi) == (date(2026, 9, 1), date(2026, 9, 1))

    def test_gte_gives_a_lower_bound_only(self):
        lo, hi = ui_main._derive_bounds(
            "icao_hex='A445B0' AND first_message >= timestamp '2026-09-01 00:00:00'"
        )
        assert (lo, hi) == (date(2026, 9, 1), None)

    def test_flipped_orientation_still_derives(self):
        lo, hi = ui_main._derive_bounds("timestamp '2026-09-01' <= first_message")
        assert (lo, hi) == (date(2026, 9, 1), None)

    def test_last_message_lte_gives_upper_bound_only(self):
        lo, hi = ui_main._derive_bounds("last_message <= timestamp '2026-09-02 00:00:00'")
        assert (lo, hi) == (None, date(2026, 9, 2))

    def test_last_message_gte_gives_no_bound(self):
        """A flight can start long before it ends -- last_message >= T says
        nothing about how early first_message could be. Must not be
        (mis)treated as a lower bound."""
        lo, hi = ui_main._derive_bounds("last_message >= timestamp '2026-09-02 00:00:00'")
        assert (lo, hi) == (None, None)

    def test_or_bails_to_all_time(self):
        lo, hi = ui_main._derive_bounds("first_message > timestamp '2026-09-01' OR icao_hex='ABC'")
        assert (lo, hi) == (None, None)

    def test_not_bails_to_all_time(self):
        lo, hi = ui_main._derive_bounds("NOT (first_message < timestamp '2026-09-01')")
        assert (lo, hi) == (None, None)

    def test_timestamp_shaped_string_literal_is_not_a_column(self):
        """A regex would match this inside the string literal and silently
        narrow -- sqlglot must see 'ident' as the only real column here."""
        lo, hi = ui_main._derive_bounds("ident = 'first_message > 2020-01-01'")
        assert (lo, hi) == (None, None)

    def test_no_timestamp_predicate_bails_to_all_time(self):
        lo, hi = ui_main._derive_bounds("operator_designator = 'DAL'")
        assert (lo, hi) == (None, None)

    def test_parse_failure_bails_to_all_time(self):
        lo, hi = ui_main._derive_bounds("this is not valid sql at all ((")
        assert (lo, hi) == (None, None)

    def test_equals_gives_both_bounds_for_first_message(self):
        lo, hi = ui_main._derive_bounds("first_message = timestamp '2026-09-01 00:00:00'")
        assert (lo, hi) == (date(2026, 9, 1), date(2026, 9, 1))

    def test_equals_gives_upper_bound_only_for_last_message(self):
        lo, hi = ui_main._derive_bounds("last_message = timestamp '2026-09-01 00:00:00'")
        assert (lo, hi) == (None, date(2026, 9, 1))

    def test_multiple_predicates_take_the_tightest_bound(self):
        lo, hi = ui_main._derive_bounds(
            "first_message >= timestamp '2026-09-01' AND first_message >= timestamp '2026-09-05'"
        )
        assert lo == date(2026, 9, 5)


# ---------------------------------------------------------------------------
# Timestamp literal coercion (#1439) -- a bare string literal compared
# against first_message/last_message is rewritten into a proper
# TIMESTAMP '...' literal so Athena doesn't reject it with TYPE_MISMATCH.
# ---------------------------------------------------------------------------

class TestCoerceTimestampLiterals:
    def test_iso_with_z_suffix_between(self):
        result = ui_main._coerce_timestamp_literals(
            "last_message between '2026-09-05T13:55:00Z' and '2026-09-05T14:20:00Z'"
        )
        assert result == (
            "last_message between TIMESTAMP '2026-09-05 13:55:00' "
            "and TIMESTAMP '2026-09-05 14:20:00'"
        )

    def test_bare_date_gets_midnight(self):
        result = ui_main._coerce_timestamp_literals("first_message > '2026-09-05'")
        assert result == "first_message > TIMESTAMP '2026-09-05 00:00:00'"

    def test_date_time_without_seconds_gets_seconds(self):
        result = ui_main._coerce_timestamp_literals("last_message <= '2026-09-05 14:20'")
        assert result == "last_message <= TIMESTAMP '2026-09-05 14:20:00'"

    def test_fractional_seconds_truncated_to_millis(self):
        result = ui_main._coerce_timestamp_literals(
            "first_message = '2026-09-05T13:55:00.123456Z'"
        )
        assert result == "first_message = TIMESTAMP '2026-09-05 13:55:00.123'"

    def test_offset_other_than_z_converted_to_utc(self):
        result = ui_main._coerce_timestamp_literals("first_message = '2026-09-05T13:55:00+02:00'")
        assert result == "first_message = TIMESTAMP '2026-09-05 11:55:00'"

    def test_already_timestamp_typed_is_byte_for_byte_untouched(self):
        clause = "last_message between TIMESTAMP '2026-09-05 13:55:00' and TIMESTAMP '2026-09-05 14:20:00'"
        assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_unrelated_column_untouched(self):
        for clause in ["ident = 'DAL2'", "registration = 'N145AN'", "operator_designator = 'DAL'"]:
            assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_unparseable_literal_left_alone_for_part_b(self):
        clause = "first_message > 'not-a-date'"
        assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_timestamp_shaped_text_inside_unrelated_literal_untouched(self):
        """A regex-only approach would match this inside the string literal
        -- sqlglot must see 'icao_hex' as the only real column here, same
        risk _derive_bounds already guards against."""
        clause = "icao_hex = 'first_message > 2026-01-01'"
        assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_in_list_coerces_every_element(self):
        result = ui_main._coerce_timestamp_literals(
            "first_message in ('2026-09-05T13:55:00Z', '2026-09-06')"
        )
        assert result == (
            "first_message in (TIMESTAMP '2026-09-05 13:55:00', TIMESTAMP '2026-09-06 00:00:00')"
        )

    def test_flipped_comparison_literal_on_left(self):
        result = ui_main._coerce_timestamp_literals("'2026-09-05T13:55:00Z' <= last_message")
        assert result == "TIMESTAMP '2026-09-05 13:55:00' <= last_message"

    def test_mixed_clause_only_touches_the_qualifying_literal(self):
        """A literal that happens to share its exact text with an unrelated
        column's literal must not be touched just because the text
        matches -- coercion is scoped by AST position, not by value."""
        result = ui_main._coerce_timestamp_literals(
            "first_message > '2026-09-05' and ident = '2026-09-05'"
        )
        assert result == "first_message > TIMESTAMP '2026-09-05 00:00:00' and ident = '2026-09-05'"

    def test_parse_failure_returns_original_unchanged(self):
        clause = "this is not valid sql at all (("
        assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_no_timestamp_predicate_returns_original_unchanged(self):
        clause = "operator_designator = 'DAL'"
        assert ui_main._coerce_timestamp_literals(clause) == clause

    def test_formatting_of_untouched_parts_is_preserved(self):
        """Coercion splices into the original text at the literal's own
        offsets rather than re-serializing the whole tree, so casing/
        spacing anywhere else in the clause survives exactly."""
        result = ui_main._coerce_timestamp_literals(
            "icao_hex='A8AE7F' AND first_message > '2026-09-05'"
        )
        assert result == "icao_hex='A8AE7F' AND first_message > TIMESTAMP '2026-09-05 00:00:00'"


# ---------------------------------------------------------------------------
# Friendlier Athena TYPE_MISMATCH error (#1439 Part B)
# ---------------------------------------------------------------------------

class TestFriendlyAthenaError:
    def test_timestamp_varchar_mismatch_gets_hint_and_drops_line_offset(self):
        raw = (
            "TYPE_MISMATCH: line 1:241: Cannot check if timestamp(3) is "
            "BETWEEN varchar(20) and varchar(20)"
        )
        result = ui_main._friendly_athena_error(raw)
        assert "line 1:241" not in result
        assert "TIMESTAMP '2026-09-05 13:55:00'" in result
        assert "Cannot check if timestamp(3) is BETWEEN varchar(20) and varchar(20)" in result

    def test_unrelated_reason_unchanged(self):
        assert ui_main._friendly_athena_error("TABLE_NOT_FOUND") == "TABLE_NOT_FOUND"

    def test_type_mismatch_not_involving_timestamp_unchanged(self):
        raw = "TYPE_MISMATCH: line 1:10: Cannot check if integer is BETWEEN varchar(5) and varchar(5)"
        assert ui_main._friendly_athena_error(raw) == raw

    def test_empty_reason_unchanged(self):
        assert ui_main._friendly_athena_error("") == ""


# ---------------------------------------------------------------------------
# Range resolution -- intersecting the archive epoch/tomorrow defaults, the
# WHERE clause's own derived bounds (widened +/-1 day), and any explicit
# UI-supplied range.
# ---------------------------------------------------------------------------

class TestResolveSearchRange:
    def test_all_time_default_is_epoch_to_tomorrow_utc(self):
        with _frozen_today(date(2026, 9, 3)):
            start, end = ui_main._resolve_search_range(
                "operator_designator = 'DAL'", ui_main._ARCHIVE_EPOCH, date(2026, 9, 4)
            )
        assert (start, end) == (ui_main._ARCHIVE_EPOCH, date(2026, 9, 4))

    def test_derived_range_widened_by_a_day_each_side(self):
        with _frozen_today(date(2026, 9, 3)):
            start, end = ui_main._resolve_search_range(
                "first_message BETWEEN timestamp '2026-09-01 11:00:00' AND timestamp '2026-09-01 13:00:00'",
                ui_main._ARCHIVE_EPOCH, date(2026, 9, 4),
            )
        assert (start, end) == (date(2026, 8, 31), date(2026, 9, 2))

    def test_explicit_range_honored_verbatim_when_clause_has_no_timestamp(self):
        start, end = ui_main._resolve_search_range(
            "icao_hex='A445B0'", date(2026, 9, 1), date(2026, 9, 1)
        )
        assert (start, end) == (date(2026, 9, 1), date(2026, 9, 1))

    def test_empty_intersection_returns_none_none(self):
        """A first_message clause bounded to <= Sep 2, intersected with an
        explicit start of Sep 5 -- no possible overlap."""
        start, end = ui_main._resolve_search_range(
            "first_message BETWEEN timestamp '2026-09-01' AND timestamp '2026-09-02'",
            date(2026, 9, 5), date(2026, 9, 10),
        )
        assert (start, end) == (None, None)


# ---------------------------------------------------------------------------
# start_date/end_date validation and defaulting at the API boundary.
# ---------------------------------------------------------------------------

class TestDateRangeValidation:
    def test_explicit_start_after_end_returns_400(self, client):
        resp = client.post(
            "/api/archive/search",
            json={
                "name": "x", "where_clause": "1=1",
                "start_date": "2026-09-10", "end_date": "2026-09-01",
            },
        )
        assert resp.status_code == 400

    def test_start_after_default_end_returns_400(self, client):
        """No end_date given -> defaults to tomorrow UTC; an explicit
        start_date past that default is still an operator-supplied
        contradiction, not a derivation-only emptiness."""
        with _frozen_today(date(2026, 9, 3)):
            resp = client.post(
                "/api/archive/search",
                json={"name": "x", "where_clause": "1=1", "start_date": "2026-09-10"},
            )
        assert resp.status_code == 400

    def test_start_equal_to_end_is_valid(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            resp = _create_search(
                client, where_clause="1=1", start_date="2026-09-01", end_date="2026-09-01"
            )
        assert resp.status_code == 202

    def test_resolved_dates_persisted_and_visible_on_detail(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            resp = _create_search(
                client, where_clause="icao_hex='A445B0'",
                start_date="2026-09-01", end_date="2026-09-01",
            )
        uuid = resp.json()["uuid"]
        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["start_date"] == "2026-09-01"
        assert detail["end_date"] == "2026-09-01"

    def test_legacy_record_with_no_date_fields_still_loads(self, client, fake_redis):
        fake_redis.store["archive_search:legacy-uuid"] = json.dumps({
            "name": "old search", "where_clause": "1=1", "status": "COMPLETE",
            "submitted_at": "2026-01-01T00:00:00+00:00", "query_execution_id": "exec-old",
        })
        fake_redis.sadd("archive_search:index", "legacy-uuid")
        resp = client.get("/api/archive/search/legacy-uuid")
        assert resp.status_code == 200
        body = resp.json()
        assert body["start_date"] is None
        assert body["end_date"] is None

    def test_resubmit_with_persisted_dates_reproduces_identical_query(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            first = _create_search(
                client, where_clause="icao_hex='A445B0'",
                start_date="2026-08-01", end_date="2026-09-01",
            )
        uuid = first.json()["uuid"]
        detail = client.get(f"/api/archive/search/{uuid}").json()
        first_query = fake_athena.started_queries[0]["QueryString"]

        # Resubmit with a later frozen clock -- the persisted, already-
        # resolved dates must be reused verbatim rather than re-resolving
        # "tomorrow" against the new clock.
        with _frozen_today(date(2026, 12, 25)):
            second = _create_search(
                client, name="resubmitted", where_clause="icao_hex='A445B0'",
                start_date=detail["start_date"], end_date=detail["end_date"],
            )
        second_query = fake_athena.started_queries[1]["QueryString"]
        assert second_query == first_query
        assert second.status_code == 202


# ---------------------------------------------------------------------------
# Empty-intersection short-circuit: a contradiction only visible after
# derivation must resolve to a real, zero-row COMPLETE search without ever
# calling Athena -- distinct from the 400 above on the operator's own
# explicit start > end.
# ---------------------------------------------------------------------------

class TestEmptyIntersectionShortCircuit:
    def test_no_athena_call_is_made(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            resp = _create_search(
                client,
                where_clause="first_message BETWEEN timestamp '2026-09-01 00:00:00' AND timestamp '2026-09-30 00:00:00'",
                start_date="2026-09-05", end_date="2026-09-06",
            )
        assert resp.status_code == 202
        assert fake_athena.started_queries == []

    def test_search_is_immediately_complete_with_zero_rows(self, client, fake_athena):
        with _frozen_today(date(2026, 9, 3)):
            resp = _create_search(
                client,
                where_clause="first_message BETWEEN timestamp '2026-09-01 00:00:00' AND timestamp '2026-09-30 00:00:00'",
                start_date="2026-09-05", end_date="2026-09-06",
            )
        uuid = resp.json()["uuid"]
        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["status"] == "COMPLETE"

        results = client.get(f"/api/archive/search/{uuid}/results").json()
        assert results == {"rows": [], "total_rows": 0, "truncated": False}

    def test_delete_of_an_empty_intersection_search_does_not_touch_s3_or_athena(
        self, client, fake_athena, fake_s3
    ):
        with _frozen_today(date(2026, 9, 3)):
            resp = _create_search(
                client,
                where_clause="first_message BETWEEN timestamp '2026-09-01 00:00:00' AND timestamp '2026-09-30 00:00:00'",
                start_date="2026-09-05", end_date="2026-09-06",
            )
        uuid = resp.json()["uuid"]
        delete_resp = client.delete(f"/api/archive/search/{uuid}")
        assert delete_resp.status_code == 204
        assert fake_s3.deleted == []


# ---------------------------------------------------------------------------
# Property test: partition-predicate derivation is an optimisation only --
# it must never change which rows a query matches. Executes the ACTUAL
# generated SQL (via sqlglot's own pure-Python executor, against an
# in-memory table) with derivation enabled (the real, possibly-narrowed
# partition predicate) vs. forced off (a partition predicate spanning the
# full archive range, i.e. every partition), and asserts the two produce
# identical row sets for every vector -- not just the worked examples, a
# real end-to-end evaluation of the generated WHERE clause.
# ---------------------------------------------------------------------------

class TestDerivationSupersetProperty:
    # icao_hex doubles as the row's identity for comparing result sets.
    # year/month/day mirror what the real S3 key layout/Parquet index would
    # carry for each row's first_message -- exactly what the partition
    # predicate is written to filter on.
    _TABLE_ROWS = [
        {"icao_hex": "A00001", "operator_designator": "DAL", "first_message": "2022-01-01 00:00:00.000",
         "last_message": "2022-01-01 01:00:00.000", "year": "2022", "month": "01", "day": "01"},
        {"icao_hex": "A00002", "operator_designator": "UAL", "first_message": "2026-07-31 12:00:00.000",
         "last_message": "2026-07-31 13:00:00.000", "year": "2026", "month": "07", "day": "31"},
        {"icao_hex": "A00003", "operator_designator": "AAL", "first_message": "2026-08-01 00:00:00.000",
         "last_message": "2026-08-01 01:00:00.000", "year": "2026", "month": "08", "day": "01"},
        {"icao_hex": "A00004", "operator_designator": "DAL", "first_message": "2026-08-15 09:00:00.000",
         "last_message": "2026-08-15 10:00:00.000", "year": "2026", "month": "08", "day": "15"},
        {"icao_hex": "A00005", "operator_designator": "SWA", "first_message": "2026-09-03 00:00:00.000",
         "last_message": "2026-09-03 01:00:00.000", "year": "2026", "month": "09", "day": "03"},
    ]

    def _matching_icao_hexes(self, partition_predicate: str, where_clause: str) -> set[str]:
        from sqlglot.executor import execute
        query = f"SELECT icao_hex FROM skyfollower.archive_flights WHERE ({partition_predicate}) AND ({where_clause})"
        table = execute(query, dialect="trino", tables={"skyfollower": {"archive_flights": self._TABLE_ROWS}})
        return {row[0] for row in table.rows}

    @pytest.mark.parametrize("where_clause", [
        "first_message BETWEEN timestamp '2026-07-31 00:00:00' AND timestamp '2026-07-31 23:59:59'",
        "first_message >= timestamp '2026-08-01 00:00:00'",
        "last_message <= timestamp '2026-08-01 01:00:00'",
        "icao_hex = 'A00001'",
        "first_message > timestamp '2026-07-01' OR icao_hex = 'A00001'",
        "NOT (first_message < timestamp '2026-08-01')",
        "operator_designator = 'DAL'",
    ])
    def test_derivation_enabled_matches_derivation_forced_off(self, where_clause):
        with _frozen_today(date(2026, 9, 3)):
            today = ui_main.datetime.now(ui_main.timezone.utc).date()
            tomorrow = today + ui_main.timedelta(days=1)
            start, end = ui_main._resolve_search_range(where_clause, ui_main._ARCHIVE_EPOCH, tomorrow)
            assert start is not None, "none of this test's vectors should derive an empty intersection"
            derived_predicate = ui_main._partition_predicate(start, end)
            full_range_predicate = ui_main._partition_predicate(ui_main._ARCHIVE_EPOCH, tomorrow)

        enabled = self._matching_icao_hexes(derived_predicate, where_clause)
        forced_off = self._matching_icao_hexes(full_range_predicate, where_clause)
        assert enabled == forced_off


# ---------------------------------------------------------------------------
# Create / list / get
# ---------------------------------------------------------------------------

class TestCreateAndListSearches:
    def test_create_returns_202_and_uuid(self, client):
        resp = _create_search(client)
        assert resp.status_code == 202
        assert "uuid" in resp.json()

    def test_created_search_appears_in_list(self, client):
        create_resp = _create_search(client, name="my search")
        uuid = create_resp.json()["uuid"]

        list_resp = client.get("/api/archive/search")
        assert list_resp.status_code == 200
        entries = list_resp.json()
        assert any(e["uuid"] == uuid and e["name"] == "my search" for e in entries)

    def test_list_includes_expires_at_seven_days_out(self, client):
        create_resp = _create_search(client)
        uuid = create_resp.json()["uuid"]
        entry = next(e for e in client.get("/api/archive/search").json() if e["uuid"] == uuid)

        from datetime import datetime
        submitted = datetime.fromisoformat(entry["submitted_at"])
        expires = datetime.fromisoformat(entry["expires_at"])
        assert (expires - submitted).days == 7

    def test_get_one_includes_where_clause(self, client):
        create_resp = _create_search(client, where_clause="ident = 'DAL123'")
        uuid = create_resp.json()["uuid"]

        resp = client.get(f"/api/archive/search/{uuid}")
        assert resp.status_code == 200
        assert resp.json()["where_clause"] == "ident = 'DAL123'"

    def test_get_one_persists_the_coerced_timestamp_clause(self, client, fake_athena):
        """#1439: the normalized clause -- not the raw operator input -- is
        what's persisted/displayed, so History's "WHERE clause submitted"
        block, Duplicate, and Edit & Resubmit all show what actually ran."""
        create_resp = _create_search(client, where_clause="first_message > '2026-09-05'")
        uuid = create_resp.json()["uuid"]

        resp = client.get(f"/api/archive/search/{uuid}")
        assert resp.status_code == 200
        assert resp.json()["where_clause"] == "first_message > TIMESTAMP '2026-09-05 00:00:00'"

        query = fake_athena.started_queries[0]["QueryString"]
        assert "TIMESTAMP '2026-09-05 00:00:00'" in query

    def test_get_nonexistent_search_404s(self, client):
        resp = client.get("/api/archive/search/does-not-exist")
        assert resp.status_code == 404

    def test_create_adds_uuid_to_archive_search_index(self, client, fake_redis):
        """Listing goes through archive_search:index (SMEMBERS), not a
        keyspace SCAN, specifically to stay cheap on a production Redis
        with hundreds of thousands of unrelated keys -- create must keep
        that index in sync or every search becomes invisible to list."""
        create_resp = _create_search(client)
        uuid = create_resp.json()["uuid"]
        assert uuid in fake_redis.smembers("archive_search:index")

    def test_delete_removes_uuid_from_archive_search_index(self, client, fake_redis):
        create_resp = _create_search(client)
        uuid = create_resp.json()["uuid"]
        assert client.delete(f"/api/archive/search/{uuid}").status_code == 204
        assert uuid not in fake_redis.smembers("archive_search:index")

    def test_list_prunes_a_stale_index_entry_for_an_already_expired_record(self, client, fake_redis):
        """A uuid whose backing archive_search:{uuid} key has already
        expired (7-day TTL) has no way to notify the index set directly --
        list must self-heal by pruning it from the index the next time
        anyone asks, not just silently omit it from the response forever."""
        fake_redis.sadd("archive_search:index", "long-gone-uuid")
        resp = client.get("/api/archive/search")
        assert resp.status_code == 200
        assert all(e["uuid"] != "long-gone-uuid" for e in resp.json())
        assert "long-gone-uuid" not in fake_redis.smembers("archive_search:index")

    def test_newly_created_search_is_running(self, client, fake_athena):
        """Without the synchronous-thread patch, the poll never runs, so
        the record should stay exactly as POST left it: RUNNING."""
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "1=1"})
        uuid = resp.json()["uuid"]
        assert client.get(f"/api/archive/search/{uuid}").json()["status"] == "RUNNING"

    def test_athena_start_failure_returns_502_not_500(self, client, fake_athena):
        fake_athena.raise_on_start = Exception("AccessDeniedException: not authorized")
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "1=1"})
        assert resp.status_code == 502


# ---------------------------------------------------------------------------
# Background polling
# ---------------------------------------------------------------------------

class TestBackgroundPolling:
    def test_succeeded_query_marks_search_complete(self, client, fake_athena):
        # The synchronous thread runs to completion before start_query_execution's
        # caller (the POST handler) even returns, so seed the eventual SUCCEEDED
        # state via a side effect: patch get_query_execution to flip state after
        # the first call, simulating "still running on attempt 1, done by attempt 2".
        # Simpler here: the fake starts RUNNING: flip it before the thread body
        # even gets a chance to poll, using a wrapping Thread that mutates state
        # first.
        real_start = fake_athena.start_query_execution

        def start_and_complete(*a, **k):
            result = real_start(*a, **k)
            fake_athena.executions[result["QueryExecutionId"]]["State"] = "SUCCEEDED"
            return result

        fake_athena.start_query_execution = start_and_complete
        resp = _create_search(client)
        uuid = resp.json()["uuid"]

        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["status"] == "COMPLETE"
        assert detail["error"] is None

    def test_failed_query_marks_search_failed_with_reason(self, client, fake_athena):
        real_start = fake_athena.start_query_execution

        def start_and_fail(*a, **k):
            result = real_start(*a, **k)
            fake_athena.executions[result["QueryExecutionId"]]["State"] = "FAILED"
            fake_athena.executions[result["QueryExecutionId"]]["Reason"] = "TABLE_NOT_FOUND"
            return result

        fake_athena.start_query_execution = start_and_fail
        resp = _create_search(client)
        uuid = resp.json()["uuid"]

        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["status"] == "FAILED"
        assert detail["error"] == "TABLE_NOT_FOUND"

    def test_timestamp_type_mismatch_gets_the_friendly_hint(self, client, fake_athena):
        """#1439 Part B: a raw TYPE_MISMATCH between a timestamp column and
        a string literal is rewritten into an actionable hint before it
        reaches the operator, with the misleading line 1:NNN offset (which
        points into the generated query, not their input) stripped."""
        real_start = fake_athena.start_query_execution

        def start_and_fail(*a, **k):
            result = real_start(*a, **k)
            fake_athena.executions[result["QueryExecutionId"]]["State"] = "FAILED"
            fake_athena.executions[result["QueryExecutionId"]]["Reason"] = (
                "TYPE_MISMATCH: line 1:241: Cannot check if timestamp(3) is "
                "BETWEEN varchar(20) and varchar(20)"
            )
            return result

        fake_athena.start_query_execution = start_and_fail
        resp = _create_search(client)
        uuid = resp.json()["uuid"]

        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["status"] == "FAILED"
        assert "line 1:241" not in detail["error"]
        assert "TIMESTAMP '2026-09-05 13:55:00'" in detail["error"]

    def test_deadline_exceeded_aborts_and_calls_stop_query_execution(self, client, fake_athena):
        # Never reaches a terminal state; force the deadline to have
        # already elapsed so the poll loop's `while` body never executes,
        # going straight to the give-up path.
        with patch.object(ui_main, "ATHENA_POLL_DEADLINE_SECONDS", -1):
            resp = _create_search(client)
        uuid = resp.json()["uuid"]
        qid = fake_athena.started_queries and list(fake_athena.executions.keys())[0]

        detail = client.get(f"/api/archive/search/{uuid}").json()
        assert detail["status"] == "ABORTED"
        assert detail["error"] == "Deadline exceeded (2 minutes)"
        assert qid in fake_athena.stopped


# ---------------------------------------------------------------------------
# Results retrieval and pagination
# ---------------------------------------------------------------------------

class TestResultsRetrieval:
    def _complete_search(self, client, fake_athena, fake_s3, rows):
        """Registers `rows` (9-field tuples, s3_key last -- same shape
        _SEARCH_SELECT_COLUMNS produces) as what get_query_results will hand
        back for the search's own query execution. No fake_s3 interaction at
        all -- the paged view never reads S3."""
        real_start = fake_athena.start_query_execution

        def start_and_register(*a, **k):
            result = real_start(*a, **k)
            fake_athena.results[result["QueryExecutionId"]] = rows
            return result

        fake_athena.start_query_execution = start_and_register
        resp = _create_search(client)
        return resp.json()["uuid"]

    def test_not_complete_returns_400(self, client):
        # Deliberately not using _synchronous_thread() -- the real
        # background thread hasn't run yet by the time the next line
        # executes, so the record is still exactly as POST left it: RUNNING.
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "1=1"})
        uuid = resp.json()["uuid"]
        results_resp = client.get(f"/api/archive/search/{uuid}/results")
        assert results_resp.status_code == 400

    def test_get_query_results_failure_returns_502_not_500(self, client, fake_athena, fake_s3):
        """A permissions mismatch or other AWS-side failure fetching the
        cached page window must surface as a clean 502, not an unhandled
        500."""
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        fake_athena.raise_on_get_query_results = Exception("AccessDeniedException: not authorized")
        resp = client.get(f"/api/archive/search/{uuid}/results")
        assert resp.status_code == 502

    def test_results_omit_s3_key_and_include_uuid_and_token(self, client, fake_athena, fake_s3):
        row = ("A8AE7F", "N659DL", "B738", "false", "DAL", "DAL123",
               "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
               "flights/2026/07/31/0198abcd-1234-7abc-8def-1234567890ab.json.gz")
        uuid = self._complete_search(client, fake_athena, fake_s3, [row])

        resp = client.get(f"/api/archive/search/{uuid}/results")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total_rows"] == 1
        rows = body["rows"]
        assert len(rows) == 1
        result_row = rows[0]
        assert "s3_key" not in result_row
        assert result_row["uuid"] == "0198abcd-1234-7abc-8def-1234567890ab"
        assert result_row["icao_hex"] == "A8AE7F"
        assert result_row["military"] is False
        assert "token" in result_row and result_row["token"]

    def test_token_decrypts_back_to_the_real_s3_key(self, client, fake_athena, fake_s3):
        s3_key = "flights/2026/07/31/0198abcd-1234-7abc-8def-1234567890ab.json.gz"
        row = ("A8AE7F", "N659DL", "B738", "true", "DAL", "DAL123",
               "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000", s3_key)
        uuid = self._complete_search(client, fake_athena, fake_s3, [row])

        token = client.get(f"/api/archive/search/{uuid}/results").json()["rows"][0]["token"]
        assert ui_main._decrypt_token(token) == s3_key

    def test_pagination_returns_100_rows_per_page(self, client, fake_athena, fake_s3):
        rows = [
            (f"A{i:05X}", "N1", "B738", "false", "DAL", "DAL1",
             "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
             f"flights/2026/07/31/uuid-{i}.json.gz")
            for i in range(150)
        ]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        page1 = client.get(f"/api/archive/search/{uuid}/results?page=1").json()
        page2 = client.get(f"/api/archive/search/{uuid}/results?page=2").json()
        assert len(page1["rows"]) == 100
        assert len(page2["rows"]) == 50
        assert page1["total_rows"] == 150
        assert page2["total_rows"] == 150

    def test_second_page_request_reuses_cache_no_second_athena_call(self, client, fake_athena, fake_s3):
        rows = [("A1", "N1", "B738", "false", "DAL", "DAL1",
                  "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
                  "flights/2026/07/31/uuid-0.json.gz")]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        client.get(f"/api/archive/search/{uuid}/results?page=1")
        original_get_query_results = fake_athena.get_query_results
        fake_athena.get_query_results = MagicMock(side_effect=AssertionError("should not refetch from Athena"))
        try:
            client.get(f"/api/archive/search/{uuid}/results?page=1")
        finally:
            fake_athena.get_query_results = original_get_query_results

    def test_page_size_param_controls_slice_size(self, client, fake_athena, fake_s3):
        rows = [
            (f"A{i:05X}", "N1", "B738", "false", "DAL", "DAL1",
             "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
             f"flights/2026/07/31/uuid-{i}.json.gz")
            for i in range(60)
        ]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        page1 = client.get(f"/api/archive/search/{uuid}/results?page=1&page_size=25").json()
        page2 = client.get(f"/api/archive/search/{uuid}/results?page=2&page_size=25").json()
        page3 = client.get(f"/api/archive/search/{uuid}/results?page=3&page_size=25").json()
        assert len(page1["rows"]) == 25
        assert len(page2["rows"]) == 25
        assert len(page3["rows"]) == 10
        assert page1["total_rows"] == 60

    def test_page_size_out_of_bounds_returns_422(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        too_small = client.get(f"/api/archive/search/{uuid}/results?page_size=24")
        too_large = client.get(f"/api/archive/search/{uuid}/results?page_size=501")
        assert too_small.status_code == 422
        assert too_large.status_code == 422


class TestResultsSorting:
    def _complete_search(self, client, fake_athena, fake_s3, rows):
        return TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, rows)

    _ROWS = [
        ("A00003", "N3", "B738", "false", "DAL", "DAL3",
         "2026-07-31 12:00:00.000", "2026-07-31 15:00:00.000",
         "flights/2026/07/31/uuid-3.json.gz"),
        ("A00001", "N1", "A320", "true", "AAL", "AAL1",
         "2026-07-31 14:00:00.000", "2026-07-31 13:00:00.000",
         "flights/2026/07/31/uuid-1.json.gz"),
        ("A00002", "N2", "C172", "false", "UAL", "UAL2",
         "2026-07-31 10:00:00.000", "2026-07-31 20:00:00.000",
         "flights/2026/07/31/uuid-2.json.gz"),
    ]

    def test_sort_by_icao_hex_ascending_default(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex")
        assert [r["icao_hex"] for r in resp.json()["rows"]] == ["A00001", "A00002", "A00003"]

    def test_sort_dir_desc_reverses_order(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=desc")
        assert [r["icao_hex"] for r in resp.json()["rows"]] == ["A00003", "A00002", "A00001"]

    def test_sort_by_military_boolean(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=military")
        assert [r["military"] for r in resp.json()["rows"]] == [False, False, True]

    def test_sort_by_first_message_timestamp(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=first_message")
        assert [r["icao_hex"] for r in resp.json()["rows"]] == ["A00002", "A00003", "A00001"]

    def test_sort_by_last_message_timestamp_desc(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=last_message&sort_dir=desc")
        assert [r["icao_hex"] for r in resp.json()["rows"]] == ["A00002", "A00003", "A00001"]

    def test_sort_is_applied_before_pagination(self, client, fake_athena, fake_s3):
        rows = [
            (f"A{i:05X}", "N1", "B738", "false", "DAL", "DAL1",
             "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
             f"flights/2026/07/31/uuid-{i}.json.gz")
            for i in range(150)
        ]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        page1 = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=desc&page=1").json()
        page2 = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=desc&page=2").json()
        # Descending across the WHOLE 150-row set: page 1 holds the top 100
        # hex values, page 2 the bottom 50 -- not a within-page reorder.
        assert page1["rows"][0]["icao_hex"] == f"A{149:05X}"
        assert page1["rows"][-1]["icao_hex"] == f"A{50:05X}"
        assert page2["rows"][0]["icao_hex"] == f"A{49:05X}"
        assert page2["rows"][-1]["icao_hex"] == f"A{0:05X}"

    def test_sort_does_not_trigger_a_second_athena_call(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        client.get(f"/api/archive/search/{uuid}/results")
        original_get_query_results = fake_athena.get_query_results
        fake_athena.get_query_results = MagicMock(side_effect=AssertionError("should not refetch just to sort"))
        try:
            resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=desc")
            assert resp.status_code == 200
        finally:
            fake_athena.get_query_results = original_get_query_results

    def test_invalid_sort_by_returns_422(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=token")
        assert resp.status_code == 422

    def test_invalid_sort_dir_returns_422(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=sideways")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# The 500/501 boundary -- the core memory-bound fix. Off-by-one is the easy
# thing to get wrong here, so both edges (exactly the cap, one past it) get
# their own test rather than relying on a single "big" number.
# ---------------------------------------------------------------------------

def _row(i: int) -> tuple:
    return (
        f"A{i:05X}", "N1", "B738", "false", "DAL", "DAL1",
        "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
        f"flights/2026/07/31/uuid-{i}.json.gz",
    )


class TestResultCap:
    def _complete_search(self, client, fake_athena, fake_s3, rows):
        return TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, rows)

    def test_exactly_500_rows_is_not_truncated_and_total_rows_is_exact(self, client, fake_athena, fake_s3):
        rows = [_row(i) for i in range(500)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        resp = client.get(f"/api/archive/search/{uuid}/results?page_size=500")
        assert resp.status_code == 200
        body = resp.json()
        assert body["truncated"] is False
        assert body["total_rows"] == 500
        assert len(body["rows"]) == 500

    def test_501_rows_caches_exactly_500_and_marks_truncated(self, client, fake_athena, fake_s3):
        rows = [_row(i) for i in range(501)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        resp = client.get(f"/api/archive/search/{uuid}/results?page_size=500")
        assert resp.status_code == 200
        body = resp.json()
        assert body["truncated"] is True
        assert body["total_rows"] == 500
        assert len(body["rows"]) == 500
        # The discarded 501st row (the highest icao_hex) must never surface.
        assert f"A{500:05X}" not in [r["icao_hex"] for r in body["rows"]]

    def test_only_501_rows_ever_requested_from_athena_regardless_of_true_match_count(
        self, client, fake_athena, fake_s3
    ):
        """The fake models a search that "really" matched far more than 500
        rows (Athena would never hand all of those back in one call in
        production either) -- get_query_results must still only ever be
        asked for _RESULT_ROW_CAP + 1."""
        rows = [_row(i) for i in range(5000)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        original_get_query_results = fake_athena.get_query_results
        calls = []

        def spy(QueryExecutionId, MaxResults=1000, NextToken=None):
            calls.append(MaxResults)
            return original_get_query_results(QueryExecutionId, MaxResults=MaxResults, NextToken=NextToken)

        fake_athena.get_query_results = spy
        client.get(f"/api/archive/search/{uuid}/results")
        assert calls == [ui_main._RESULT_ROW_CAP + 1]

    def test_page_beyond_cached_range_returns_a_clear_error(self, client, fake_athena, fake_s3):
        rows = [_row(i) for i in range(501)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        # 500 cached rows / 100 per page = 5 valid pages.
        last_valid = client.get(f"/api/archive/search/{uuid}/results?page=5&page_size=100")
        assert last_valid.status_code == 200
        assert len(last_valid.json()["rows"]) == 100

        beyond = client.get(f"/api/archive/search/{uuid}/results?page=6&page_size=100")
        assert beyond.status_code == 400
        assert "page" in beyond.json()["detail"].lower()

    def test_unaffected_small_result_is_byte_identical_in_shape(self, client, fake_athena, fake_s3):
        """A 12-row match -- today's common case -- behaves exactly as
        before, plus the new (always-False-here) `truncated` field."""
        rows = [_row(i) for i in range(12)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        resp = client.get(f"/api/archive/search/{uuid}/results")
        assert resp.status_code == 200
        body = resp.json()
        assert body["truncated"] is False
        assert body["total_rows"] == 12
        assert len(body["rows"]) == 12

    def test_encrypt_s3_key_called_at_most_page_size_times_never_per_matching_row(
        self, client, fake_athena, fake_s3
    ):
        """Simulates far more real matches (5000) than could ever be cached
        (500) -- a single results request must mint at most page_size
        tokens (the whole cached window, when page_size is the 500 max),
        never one per matching row."""
        rows = [_row(i) for i in range(5000)]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        with patch.object(ui_main, "_encrypt_s3_key", wraps=ui_main._encrypt_s3_key) as spy:
            resp = client.get(f"/api/archive/search/{uuid}/results?page_size=500")
        assert resp.status_code == 200
        assert spy.call_count <= 500
        assert spy.call_count != 5000


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

class TestDeleteSearch:
    def test_delete_nonexistent_404s(self, client):
        assert client.delete("/api/archive/search/nope").status_code == 404

    def test_delete_running_search_stops_query_and_removes_record(self, client, fake_athena):
        # Deliberately not using _synchronous_thread() -- see
        # test_not_complete_returns_400 for why this keeps the record RUNNING.
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "1=1"})
        uuid = resp.json()["uuid"]

        delete_resp = client.delete(f"/api/archive/search/{uuid}")
        assert delete_resp.status_code == 204
        assert client.get(f"/api/archive/search/{uuid}").status_code == 404
        assert fake_athena.stopped  # best-effort stop was attempted

    def test_delete_complete_search_removes_s3_result_file(self, client, fake_athena, fake_s3):
        real_start = fake_athena.start_query_execution

        def start_and_complete(*a, **k):
            result = real_start(*a, **k)
            qid = result["QueryExecutionId"]
            fake_athena.executions[qid]["State"] = "SUCCEEDED"
            key = fake_athena.executions[qid]["OutputLocation"].removeprefix("s3://test-bucket/")
            fake_s3.objects[key] = _csv_body([])
            return result

        fake_athena.start_query_execution = start_and_complete
        resp = _create_search(client)
        uuid = resp.json()["uuid"]

        client.delete(f"/api/archive/search/{uuid}")
        # The result object itself, plus its .metadata sidecar.
        assert len(fake_s3.deleted) == 2
        assert fake_s3.deleted[1] == f"{fake_s3.deleted[0]}.metadata"

    def test_delete_removes_download_query_result_and_metadata_sidecars(
        self, client, fake_athena, fake_s3
    ):
        """A search that's had "Download CSV" clicked at least once has a
        second, separate Athena query execution (download_query_execution_id)
        with its own result file -- delete must clean that up too, not just
        the paged-view query's result."""
        uuid = TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, rows=[])
        download_resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert download_resp.status_code == 307

        client.delete(f"/api/archive/search/{uuid}")

        # Two Athena executions (paged-view + download), two objects each
        # (result + .metadata) = four deletes total.
        assert len(fake_s3.deleted) == 4
        result_keys = [k for k in fake_s3.deleted if not k.endswith(".metadata")]
        metadata_keys = [k for k in fake_s3.deleted if k.endswith(".metadata")]
        assert len(result_keys) == 2
        assert len(set(result_keys)) == 2  # the two underlying result keys are distinct
        assert sorted(metadata_keys) == sorted(f"{k}.metadata" for k in result_keys)

    def test_delete_without_download_never_queried_only_cleans_main_result(
        self, client, fake_athena, fake_s3
    ):
        """No "Download CSV" click ever happened -- download_query_execution_id
        is unset, so delete must not attempt to clean up a download result
        that never existed."""
        uuid = TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, rows=[])
        client.delete(f"/api/archive/search/{uuid}")
        assert len(fake_s3.deleted) == 2  # main result + its .metadata only

    def test_failed_s3_cleanup_still_deletes_redis_record(self, client, fake_athena, fake_s3):
        """A failed S3/Athena cleanup is best-effort -- it must not block
        removing the Redis record, for either the main result or the
        download result."""
        uuid = TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, rows=[])
        client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        fake_athena.raise_on_get_query_execution = RuntimeError("boom")

        delete_resp = client.delete(f"/api/archive/search/{uuid}")
        assert delete_resp.status_code == 204
        assert fake_s3.deleted == []  # every cleanup attempt failed before any delete_object call
        assert client.get(f"/api/archive/search/{uuid}").status_code == 404

    def test_thread_resurrection_guard_xx_prevents_late_write_after_delete(self, client, fake_redis):
        """A background poll write landing after DELETE already removed
        the key must be a silent no-op, not a resurrection -- exercised
        directly against _update_search_record since the real race is
        timing-dependent and not reliably reproducible via HTTP alone."""
        resp = _create_search(client)
        uuid = resp.json()["uuid"]
        client.delete(f"/api/archive/search/{uuid}")
        assert fake_redis.get(f"archive_search:{uuid}") is None

        ui_main._update_search_record(uuid, status="COMPLETE")

        assert fake_redis.get(f"archive_search:{uuid}") is None


# ---------------------------------------------------------------------------
# Download -- always S3-direct via a presigned URL, for every result size,
# backed by a second, sanitized query that never selects s3_key.
# ---------------------------------------------------------------------------

class TestDownload:
    def _complete_search(self, client, fake_athena, fake_s3, rows=()):
        return TestResultsRetrieval()._complete_search(client, fake_athena, fake_s3, list(rows))

    def test_not_complete_returns_400(self, client):
        resp = client.post("/api/archive/search", json={"name": "x", "where_clause": "1=1"})
        uuid = resp.json()["uuid"]
        resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert resp.status_code == 400

    def test_nonexistent_search_404s(self, client):
        resp = client.get("/api/archive/search/nope/download", follow_redirects=False)
        assert resp.status_code == 404

    def test_download_query_excludes_s3_key_column_and_reuses_partition_and_where(
        self, client, fake_athena, fake_s3
    ):
        with _frozen_today(date(2026, 9, 3)):
            uuid = self._complete_search(client, fake_athena, fake_s3, rows=[_row(0)])
        client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)

        assert len(fake_athena.started_queries) == 2
        search_query = fake_athena.started_queries[0]["QueryString"]
        download_query = fake_athena.started_queries[1]["QueryString"]

        # Same partition predicate + where_clause as the original search --
        # everything from WHERE onward is byte-identical between the two.
        assert search_query.split(" WHERE ", 1)[1] == download_query.split(" WHERE ", 1)[1]

        select_clause = download_query.split(" FROM ", 1)[0]
        assert select_clause == (
            f"SELECT regexp_extract(s3_key, '{ui_main._UUID_FROM_S3_KEY_PATTERN}', 1) AS uuid, "
            "icao_hex, registration, type_designator, military, operator_designator, ident, "
            "first_message, last_message"
        )
        # Today's nine columns, in today's order, uuid first -- and s3_key
        # is never a standalone selected column (only an argument to
        # regexp_extract, which must reference it to derive uuid at all).
        assert select_clause.count("s3_key") == 1

    def test_download_returns_a_307_redirect_to_a_presigned_s3_url(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[_row(0)])
        resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert resp.status_code == 307
        assert resp.headers["location"].startswith("https://test-bucket.s3.amazonaws.com/")
        assert len(fake_s3.presigned_calls) == 1
        call = fake_s3.presigned_calls[0]
        assert call["ExpiresIn"] == ui_main._DOWNLOAD_PRESIGN_TTL_SECONDS
        # The friendly download filename must never leak the real S3 key
        # layout either -- it's derived from the search's own name + uuid.
        content_disposition = call["Params"]["ResponseContentDisposition"]
        assert "flights/" not in content_disposition
        assert "test-bucket" not in content_disposition
        assert content_disposition.startswith("attachment; filename=")

    def test_download_query_submitted_at_most_once_per_search(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3)
        queries_before = len(fake_athena.started_queries)

        client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert len(fake_athena.started_queries) == queries_before + 1

        client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert len(fake_athena.started_queries) == queries_before + 1  # no second query, ever

    def test_downloaded_object_contains_no_s3_path_and_a_bare_uuid_first_column(
        self, client, fake_athena, fake_s3
    ):
        """Models what Athena would actually write for the download query
        (uuid first, no s3_key/bucket/date-folder path anywhere) and
        confirms the presigned redirect points at exactly that object --
        the backend never transforms the object in transit, so what's
        written is what a browser following the redirect would receive."""
        uuid = self._complete_search(client, fake_athena, fake_s3)

        real_start = fake_athena.start_query_execution
        written_csv = (
            "uuid,icao_hex,registration,type_designator,military,operator_designator,"
            "ident,first_message,last_message\n"
            "0198abcd-1234-7abc-8def-1234567890ab,A8AE7F,N659DL,B738,false,DAL,DAL123,"
            "2026-07-31 12:00:00.000,2026-07-31 13:00:00.000\n"
        )

        def start_and_write(*a, **k):
            result = real_start(*a, **k)
            qid = result["QueryExecutionId"]
            key = fake_athena.executions[qid]["OutputLocation"].removeprefix("s3://test-bucket/")
            fake_s3.objects[key] = written_csv.encode("utf-8")
            return result

        fake_athena.start_query_execution = start_and_write
        resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert resp.status_code == 307

        presigned_key = fake_s3.presigned_calls[-1]["Params"]["Key"]
        content = fake_s3.objects[presigned_key].decode("utf-8")
        assert "flights/" not in content
        assert ".json.gz" not in content
        assert "test-bucket" not in content

        header, first_row, *_ = content.strip().splitlines()
        assert header == (
            "uuid,icao_hex,registration,type_designator,military,operator_designator,"
            "ident,first_message,last_message"
        )
        first_uuid = first_row.split(",")[0]
        assert re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", first_uuid
        )

    def test_download_query_failure_returns_502_not_500(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3)
        fake_athena.raise_on_start = Exception("AccessDeniedException: not authorized")
        resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert resp.status_code == 502

    def test_failed_download_query_returns_502(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3)
        real_start = fake_athena.start_query_execution

        def start_and_fail(*a, **k):
            result = real_start(*a, **k)
            qid = result["QueryExecutionId"]
            fake_athena.executions[qid]["State"] = "FAILED"
            fake_athena.executions[qid]["Reason"] = "SYNTAX_ERROR"
            return result

        fake_athena.start_query_execution = start_and_fail
        resp = client.get(f"/api/archive/search/{uuid}/download", follow_redirects=False)
        assert resp.status_code == 502

    def test_legacy_record_missing_date_fields_still_downloads(self, client, fake_athena, fake_s3, fake_redis):
        """A search record written before start_date/end_date existed (see
        ArchiveSearchDetail's Optional fields) must still be downloadable --
        falls back to the full archive range, same as create_archive_search's
        own default for an omitted explicit bound."""
        fake_redis.store["archive_search:legacy-uuid"] = json.dumps({
            "name": "legacy", "where_clause": "icao_hex = 'A8AE7F'", "status": "COMPLETE",
            "submitted_at": "2026-01-01T00:00:00+00:00", "query_execution_id": "exec-old",
        })
        fake_redis.sadd("archive_search:index", "legacy-uuid")
        fake_athena.executions["exec-old"] = {
            "State": "SUCCEEDED", "OutputLocation": "s3://test-bucket/athena-results/exec-old.csv", "Reason": "",
        }

        resp = client.get("/api/archive/search/legacy-uuid/download", follow_redirects=False)
        assert resp.status_code == 307


class TestDownloadFilename:
    def test_slugifies_the_search_name(self):
        assert ui_main._download_filename("My Search!", "abc-123") == "my-search-abc-123.csv"

    def test_falls_back_to_archive_search_for_an_all_punctuation_name(self):
        assert ui_main._download_filename("***", "abc-123") == "archive-search-abc-123.csv"


# ---------------------------------------------------------------------------
# Flight fetch (encrypted token)
# ---------------------------------------------------------------------------

class TestFlightFetch:
    def test_invalid_token_returns_400(self, client):
        resp = client.get("/api/archive/flights/not-a-real-token")
        assert resp.status_code == 400

    def test_missing_flight_object_returns_502_not_500(self, client, fake_s3):
        """A valid token whose flight object is gone (404/NoSuchKey) or
        unreachable (e.g. a permissions mismatch, 403) must surface as a
        clean 502 -- the same AWS-error contract as every other archive
        endpoint -- not an unhandled 500."""
        s3_key = "flights/2026/07/31/uuid.json.gz"
        token = ui_main._encrypt_s3_key(s3_key)  # never written to fake_s3.objects

        resp = client.get(f"/api/archive/flights/{token}")
        assert resp.status_code == 502

    def test_valid_token_downloads_the_flight_object(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid.json.gz"
        fake_s3.objects[s3_key] = b"gzipped-flight-bytes"
        token = ui_main._encrypt_s3_key(s3_key)

        resp = client.get(f"/api/archive/flights/{token}")
        assert resp.status_code == 200
        assert resp.content == b"gzipped-flight-bytes"
        assert "attachment" in resp.headers["content-disposition"]

    def test_token_never_exposes_the_raw_s3_key_in_the_url(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid.json.gz"
        fake_s3.objects[s3_key] = b"x"
        token = ui_main._encrypt_s3_key(s3_key)
        assert "flights/2026" not in token


# ---------------------------------------------------------------------------
# Flight detail view (History's "View" modal)
# ---------------------------------------------------------------------------

def _put_flight_record(fake_s3, s3_key: str, record: dict) -> str:
    fake_s3.objects[s3_key] = gzip.compress(json.dumps(record).encode("utf-8"))
    return ui_main._encrypt_s3_key(s3_key)


# A real archived flight's `aircraft` field is merge_aircraft.lua's
# unflattened output: type/category/manufacturer/powerplant/etc. live one
# level deeper, under aircraft.aircraft, alongside the flat
# registration/registrant/icao_hex at the top.
_NESTED_AIRCRAFT = {
    "icao_hex": "A471E0",
    "registration": "N386DA",
    "type_designator": "B738",
    "manufacturer_model": "BOEING 737-800",
    "aircraft": {
        "type": "Airplane",
        "category": "Land",
        "model": "737-832",
        "serial_number": "30373",
        "seats": 189,
        "powerplant": {"type": "Turbo-fan", "count": 2, "manufacturer": "CFM INTL.", "model": "CFM56 SERIES"},
    },
}


class TestFlightView:
    def test_aircraft_fields_are_flattened_from_the_nested_shape(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": _NESTED_AIRCRAFT,
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 42,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        body = resp.json()

        # Regression: get_archive_flight_view previously read these two
        # straight off the unflattened envelope and always got None.
        assert body["type_designator"] == "B738"
        assert body["manufacturer_model"] == "BOEING 737-800"

        assert body["category"] == "Land"
        assert body["aircraft_type"] == "Airplane"
        assert body["model"] == "737-832"
        assert body["serial_number"] == "30373"
        assert body["seats"] == 189
        assert body["powerplant"] == {
            "type": "Turbo-fan", "count": 2, "manufacturer": "CFM INTL.", "model": "CFM56 SERIES",
        }

    def test_aircraft_detail_fields_absent_when_not_enriched(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid2.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 3,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        body = resp.json()
        for field in ("category", "aircraft_type", "model", "serial_number", "seats", "powerplant", "registrant"):
            assert body[field] is None

    def test_registrant_is_read_from_flight_not_aircraft(self, client, fake_s3):
        """registrant is a sibling of aircraft/operator on CompletedFlight
        (an entity, the aircraft's legal owner -- not a property of the
        airframe), not nested inside aircraft."""
        s3_key = "flights/2026/07/31/uuid3.json.gz"
        registrant = {"names": ["Delta Air Lines Inc"], "city": "Atlanta", "country": "US"}
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "registrant": registrant,
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 3,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        assert resp.json()["registrant"] == registrant

    def test_registrant_nested_inside_aircraft_is_not_leaked(self, client, fake_s3):
        """A stale/legacy-shaped record with registrant still nested inside
        aircraft (the pre-#1416 shape) must not surface it -- get_archive_flight_view
        reads registrant off the flight record itself, not the aircraft dict."""
        s3_key = "flights/2026/07/31/uuid4.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F", "registrant": {"names": ["Stale Nested Corp"]}},
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 3,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        assert resp.json()["registrant"] is None

    def test_receiver_sources_present_on_the_flight_record(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid5.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "receiver_sources": ["1090", "EXTERNAL"],
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 3,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        assert resp.json()["receiver_sources"] == ["1090", "EXTERNAL"]

    def test_receiver_sources_defaults_to_empty_for_legacy_flights(self, client, fake_s3):
        """Legacy-migrated flights never had receiver_sources -- must default
        to an empty list, not error, and the frontend treats [] as absent."""
        s3_key = "flights/2026/07/31/uuid6.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:05:00Z",
            "total_messages": 3,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        assert resp.json()["receiver_sources"] == []

    def test_flight_path_carries_coord_times_and_speeds(self, client, fake_s3):
        """#1441: get_archive_flight_view must pass velocities into
        build_flight_path, not just positions -- coordSpeeds only appears
        when velocities is explicitly passed."""
        s3_key = "flights/2026/07/31/uuid7.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "positions": [
                {"latitude": 33.0, "longitude": -84.0, "altitude": 1000, "timestamp": "2026-07-31T12:00:00+00:00"},
                {"latitude": 34.0, "longitude": -85.0, "altitude": 2000, "timestamp": "2026-07-31T12:00:10+00:00"},
            ],
            "velocities": [
                {"timestamp": "2026-07-31T12:00:00+00:00", "velocity": 400.0},
            ],
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:00:10Z",
            "total_messages": 2,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        flight_path = resp.json()["flight_path"]
        assert flight_path["properties"]["coordTimes"] == [1785499200, 1785499210]
        # Nearest-match extrapolation: only one velocity sample, before both positions.
        assert flight_path["properties"]["coordSpeeds"] == [400, 400]

    def test_flight_path_absent_when_fewer_than_two_positions(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid8.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "positions": [{"latitude": 33.0, "longitude": -84.0, "timestamp": "2026-07-31T12:00:00+00:00"}],
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:00:00Z",
            "total_messages": 1,
        })

        resp = client.get(f"/api/archive/flights/{token}/view")
        assert resp.status_code == 200
        assert resp.json()["flight_path"] is None


class TestFlightPathDownload:
    def test_geojson_export_carries_coord_times_and_speeds(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid9.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "positions": [
                {"latitude": 33.0, "longitude": -84.0, "altitude": 1000, "timestamp": "2026-07-31T12:00:00+00:00"},
                {"latitude": 34.0, "longitude": -85.0, "altitude": 2000, "timestamp": "2026-07-31T12:00:10+00:00"},
            ],
            "velocities": [
                {"timestamp": "2026-07-31T12:00:00+00:00", "velocity": 400.0},
                {"timestamp": "2026-07-31T12:00:10+00:00", "velocity": 420.0},
            ],
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:00:10Z",
            "total_messages": 2,
        })

        resp = client.get(f"/api/archive/flights/{token}/flight-path")
        assert resp.status_code == 200
        feature = json.loads(resp.content)
        assert feature["properties"]["coordTimes"] == [1785499200, 1785499210]
        assert feature["properties"]["coordSpeeds"] == [400, 420]

    def test_404_when_fewer_than_two_positions(self, client, fake_s3):
        s3_key = "flights/2026/07/31/uuid10.json.gz"
        token = _put_flight_record(fake_s3, s3_key, {
            "aircraft": {"icao_hex": "A8AE7F"},
            "positions": [{"latitude": 33.0, "longitude": -84.0, "timestamp": "2026-07-31T12:00:00+00:00"}],
            "first_message": "2026-07-31T12:00:00Z",
            "last_message": "2026-07-31T12:00:00Z",
            "total_messages": 1,
        })

        resp = client.get(f"/api/archive/flights/{token}/flight-path")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Startup reconciliation
# ---------------------------------------------------------------------------

class TestStartupReconciliation:
    def test_running_record_at_startup_becomes_aborted(self, tmp_path, monkeypatch, fake_redis, fake_athena, fake_s3):
        _configure_env(monkeypatch, tmp_path)

        fake_redis.store["archive_search:stuck-uuid"] = json.dumps({
            "name": "old search", "where_clause": "1=1", "status": "RUNNING",
            "submitted_at": "2026-01-01T00:00:00+00:00", "query_execution_id": "exec-old",
        })
        # Mirrors what create_archive_search itself keeps in sync -- the
        # reconciliation sweep now enumerates archive_search:index rather
        # than scanning the whole keyspace, so a record injected directly
        # into .store without also being indexed here would (correctly)
        # never be found by it.
        fake_redis.sadd("archive_search:index", "stuck-uuid")

        class FakeSession:
            def __init__(self, *a, **k):
                pass

            def client(self, name):
                return {"s3": fake_s3, "athena": fake_athena}[name]

        with patch.object(ui_main.redis_lib, "Redis", return_value=fake_redis), \
             patch.object(ui_main, "boto3", MagicMock(Session=FakeSession)):
            with TestClient(ui_main.app) as c:
                resp = c.get("/api/archive/search/stuck-uuid")
                assert resp.json()["status"] == "ABORTED"


# ---------------------------------------------------------------------------
# LRU result cache (direct unit tests, no HTTP round trip needed)
# ---------------------------------------------------------------------------

class TestBoundedResultCache:
    def test_evicts_least_recently_used_past_cap(self):
        cache = ui_main._BoundedResultCache(max_entries=2)
        cache.put("a", [{"x": 1}])
        cache.put("b", [{"x": 2}])
        cache.put("c", [{"x": 3}])
        assert cache.get("a") is None
        assert cache.get("b") is not None
        assert cache.get("c") is not None

    def test_get_refreshes_recency(self):
        cache = ui_main._BoundedResultCache(max_entries=2)
        cache.put("a", [{"x": 1}])
        cache.put("b", [{"x": 2}])
        cache.get("a")  # a is now most-recently-used
        cache.put("c", [{"x": 3}])  # should evict b, not a
        assert cache.get("a") is not None
        assert cache.get("b") is None

    def test_discard_removes_entry(self):
        cache = ui_main._BoundedResultCache(max_entries=2)
        cache.put("a", [{"x": 1}])
        cache.discard("a")
        assert cache.get("a") is None


# ---------------------------------------------------------------------------
# UUID extraction from s3_key
# ---------------------------------------------------------------------------

class TestUuidFromS3Key:
    def test_extracts_uuid(self):
        key = "flights/2026/07/31/0198abcd-1234-7abc-8def-1234567890ab.json.gz"
        assert ui_main._uuid_from_s3_key(key) == "0198abcd-1234-7abc-8def-1234567890ab"

    _UUID_RE = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"

    def test_extracts_well_formed_uuid_from_current_simplified_key_shape(self):
        """Current key layout: flights/{Y}/{M}/{D}/{uuid}.json.gz -- no
        icao_hex/ident prefix."""
        key = "flights/2026/07/31/0198abcd-1234-7abc-8def-1234567890ab.json.gz"
        extracted = ui_main._uuid_from_s3_key(key)
        assert extracted
        assert re.fullmatch(self._UUID_RE, extracted)

    def test_extracts_well_formed_uuid_from_legacy_key_shape(self):
        """Legacy key layout: flights/{Y}/{M}/{D}/{icao_hex}_{ident}_{uuid}.json.gz
        -- extraction must anchor on the uuid immediately before ".json.gz",
        not just strip the suffix off the whole filename (which would wrongly
        include the icao_hex/ident prefix)."""
        key = "flights/2026/07/31/A8AE7F_DAL123_0198abcd-1234-7abc-8def-1234567890ab.json.gz"
        extracted = ui_main._uuid_from_s3_key(key)
        assert extracted
        assert re.fullmatch(self._UUID_RE, extracted)
        assert extracted == "0198abcd-1234-7abc-8def-1234567890ab"

    def test_unrecognized_shape_returns_empty_string_not_garbage(self):
        assert ui_main._uuid_from_s3_key("flights/2026/07/31/not-a-uuid.json.gz") == ""
