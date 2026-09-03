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

import importlib.util
import json
import os
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
        # Set by a test to simulate an AWS-side failure (e.g. a permissions
        # mismatch) on the next call -- covers the 502 paths in main.py that
        # a plain state-transition can't exercise.
        self.raise_on_start: Optional[Exception] = None
        self.raise_on_get_query_execution: Optional[Exception] = None

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

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)
        self.executions[QueryExecutionId]["State"] = "CANCELLED"


class FakeS3Client:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.deleted: list[str] = []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(f"no such key: {Key}")
        body = MagicMock()
        body.read.return_value = self.objects[Key]
        return {"Body": body}

    def delete_object(self, Bucket, Key):
        self.deleted.append(Key)
        self.objects.pop(Key, None)


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
        assert results == {"rows": [], "total_rows": 0}

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
        real_start = fake_athena.start_query_execution

        def start_and_complete(*a, **k):
            result = real_start(*a, **k)
            qid = result["QueryExecutionId"]
            fake_athena.executions[qid]["State"] = "SUCCEEDED"
            output = fake_athena.executions[qid]["OutputLocation"]
            key = output.removeprefix("s3://test-bucket/")
            fake_s3.objects[key] = _csv_body(rows)
            return result

        fake_athena.start_query_execution = start_and_complete
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

    def test_athena_failure_locating_results_returns_502_not_500(self, client, fake_athena, fake_s3):
        """_result_output_location's get_query_execution call must be
        covered by the same try/except as the S3 download -- a permissions
        mismatch or other AWS-side failure there must not surface as an
        unhandled 500."""
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        fake_athena.raise_on_get_query_execution = Exception("AccessDeniedException: not authorized")
        resp = client.get(f"/api/archive/search/{uuid}/results")
        assert resp.status_code == 502

    def test_missing_result_file_returns_502_not_500(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        # Simulate the CSV having been deleted/never written -- fake_s3
        # raises KeyError for a missing key, same as a real 404/NoSuchKey.
        fake_s3.objects.clear()
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

    def test_second_page_request_reuses_cache_no_second_s3_fetch(self, client, fake_athena, fake_s3):
        rows = [("A1", "N1", "B738", "false", "DAL", "DAL1",
                  "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
                  "flights/2026/07/31/uuid-0.json.gz")]
        uuid = self._complete_search(client, fake_athena, fake_s3, rows)

        client.get(f"/api/archive/search/{uuid}/results?page=1")
        original_get_object = fake_s3.get_object
        fake_s3.get_object = MagicMock(side_effect=AssertionError("should not refetch from S3"))
        try:
            client.get(f"/api/archive/search/{uuid}/results?page=1")
        finally:
            fake_s3.get_object = original_get_object

    def test_page_size_param_controls_slice_size(self, client, fake_athena, fake_s3):
        rows = [
            (f"A{i:05X}", "N1", "B738", "false", "DAL", "DAL1",
             "2026-07-31 12:00:00.000", "2026-07-31 13:00:00.000",
             f"flights/2026/07/31/A{i:05X}_DAL1_uuid-{i}.json.gz")
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

    def test_sort_does_not_trigger_a_second_s3_fetch(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, self._ROWS)
        client.get(f"/api/archive/search/{uuid}/results")
        original_get_object = fake_s3.get_object
        fake_s3.get_object = MagicMock(side_effect=AssertionError("should not refetch from S3 just to sort"))
        try:
            resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=desc")
            assert resp.status_code == 200
        finally:
            fake_s3.get_object = original_get_object

    def test_invalid_sort_by_returns_422(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=token")
        assert resp.status_code == 422

    def test_invalid_sort_dir_returns_422(self, client, fake_athena, fake_s3):
        uuid = self._complete_search(client, fake_athena, fake_s3, rows=[])
        resp = client.get(f"/api/archive/search/{uuid}/results?sort_by=icao_hex&sort_dir=sideways")
        assert resp.status_code == 422


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
        assert len(fake_s3.deleted) == 1

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
