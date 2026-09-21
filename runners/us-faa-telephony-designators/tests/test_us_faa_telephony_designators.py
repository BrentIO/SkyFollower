"""Tests for the US FAA telephony designators data runner."""

from __future__ import annotations

import importlib.util
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Module import
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "us_faa_telephony_designators_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["us_faa_telephony_designators_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

download_section_3 = _mod.download_section_3
download_section_4 = _mod.download_section_4
is_expired = _mod.is_expired
build_record = _mod.build_record
write_to_redis = _mod.write_to_redis
ENRICHMENT_TTL_SECONDS = _mod.ENRICHMENT_TTL_SECONDS


# ---------------------------------------------------------------------------
# Fixtures: HTML samples
# ---------------------------------------------------------------------------

def _section_3_table(rows: list[tuple[str, str, str, str]]) -> str:
    trs = "<tr><td>3‐Ltr</td><td>Company</td><td>Country</td><td>Telephony</td></tr>"
    for designator, company, country, callsign in rows:
        trs += f"<tr><td>{designator}</td><td>{company}</td><td>{country}</td><td>{callsign}</td></tr>"
    return f"<table>{trs}</table>"


def _section_3_html(tables: list[str]) -> str:
    return "<html><body>" + "".join(tables) + "</body></html>"


def _section_4_html(rows: list[tuple[str, str, str, str]]) -> str:
    trs = (
        "<tr><td>Telephony/Call­Sign</td><td>Identifier</td>"
        "<td>Company or Operating Agency</td><td>Expiration­Date</td></tr>"
    )
    for callsign, identifier, company, expiration in rows:
        trs += f"<tr><td>{callsign}</td><td>{identifier}</td><td>{company}</td><td>{expiration}</td></tr>"
    return f"<html><body><table>{trs}</table></body></html>"


def _mock_session(html: str, status_code: int = 200) -> MagicMock:
    session = MagicMock()
    response = MagicMock()
    response.status_code = status_code
    response.text = html
    session.get.return_value = response
    return session


# ---------------------------------------------------------------------------
# Tests: download_section_3
# ---------------------------------------------------------------------------

class TestDownloadSection3:
    def test_parses_a_single_table(self):
        html = _section_3_html([
            _section_3_table([("AAL", "AMERICAN AIRLINES INC.", "UNITED STATES", "AMERICAN")]),
        ])
        rows = download_section_3(_mock_session(html))
        assert rows == [{
            "designator": "AAL",
            "company": "AMERICAN AIRLINES INC.",
            "country": "UNITED STATES",
            "callsign": "AMERICAN",
        }]

    def test_combines_multiple_paginated_tables(self):
        """The real page splits the logical table across ~26 <table>
        elements sharing the same header row -- every one must be parsed,
        not just the first."""
        html = _section_3_html([
            _section_3_table([("AAL", "AMERICAN AIRLINES INC.", "UNITED STATES", "AMERICAN")]),
            _section_3_table([("KMM", "KM MALTA AIRLINES PLC.", "MALTA", "SKY KNIGHT")]),
            _section_3_table([("TKJ", "AJET HAVA TASIMACILIGI ANONIM SIRKETI", "TURKEY", "ANATOLIA")]),
        ])
        rows = download_section_3(_mock_session(html))
        designators = [r["designator"] for r in rows]
        assert designators == ["AAL", "KMM", "TKJ"]

    def test_row_with_empty_callsign_still_parsed(self):
        html = _section_3_html([
            _section_3_table([("AAA", "AVICON AVIATION CONSULTANTS & AGENTS", "PAKISTAN", "")]),
        ])
        rows = download_section_3(_mock_session(html))
        assert rows[0]["callsign"] is None

    def test_ignores_unrelated_tables_on_the_page(self):
        html = "<html><body><table><tr><td>Nav</td><td>Menu</td></tr></table>" + \
            _section_3_table([("AAL", "AMERICAN AIRLINES INC.", "UNITED STATES", "AMERICAN")]) + \
            "</body></html>"
        rows = download_section_3(_mock_session(html))
        assert len(rows) == 1

    def test_raises_on_non_200(self):
        with pytest.raises(RuntimeError, match="HTTP 404"):
            download_section_3(_mock_session("", status_code=404))

    def test_raises_when_no_matching_table_found(self):
        html = "<html><body><table><tr><td>Nothing relevant</td></tr></table></body></html>"
        with pytest.raises(RuntimeError, match="Section 3 designator table"):
            download_section_3(_mock_session(html))


# ---------------------------------------------------------------------------
# Tests: download_section_4
# ---------------------------------------------------------------------------

class TestDownloadSection4:
    def test_parses_rows(self):
        html = _section_4_html([
            ("AIR SIX", "ARSIX", "NYC Environmental Protection (New Windsor, NY)", "24-Feb-2027"),
            ("NASA", "NASA", "National Aeronautics and Space Administration", "N/A"),
        ])
        rows = download_section_4(_mock_session(html))
        assert rows == [
            {"designator": "ARSIX", "company": "NYC Environmental Protection (New Windsor, NY)",
             "callsign": "AIR SIX", "expiration": "24-Feb-2027"},
            {"designator": "NASA", "company": "National Aeronautics and Space Administration",
             "callsign": "NASA", "expiration": "N/A"},
        ]

    def test_variable_length_identifier_not_rejected(self):
        """Identifier is not always 3 letters -- ARSIX (6), FEMA (4)."""
        html = _section_4_html([
            ("FEMA", "FEMA", "Federal Emergency Management Agency", "N/A"),
        ])
        rows = download_section_4(_mock_session(html))
        assert rows[0]["designator"] == "FEMA"

    def test_raises_on_non_200(self):
        with pytest.raises(RuntimeError, match="HTTP 500"):
            download_section_4(_mock_session("", status_code=500))

    def test_raises_when_no_matching_table_found(self):
        html = "<html><body><table><tr><td>Nothing relevant</td></tr></table></body></html>"
        with pytest.raises(RuntimeError, match="Section 4"):
            download_section_4(_mock_session(html))


# ---------------------------------------------------------------------------
# Tests: is_expired
# ---------------------------------------------------------------------------

class TestIsExpired:
    _TODAY = datetime(2026, 9, 20, tzinfo=timezone.utc)

    def test_na_is_never_expired(self):
        assert is_expired("N/A", self._TODAY) is False

    def test_empty_is_never_expired(self):
        assert is_expired("", self._TODAY) is False

    def test_future_date_not_expired(self):
        assert is_expired("24-Feb-2027", self._TODAY) is False

    def test_past_date_is_expired(self):
        assert is_expired("1-Jan-2026", self._TODAY) is True

    def test_todays_date_is_not_expired(self):
        assert is_expired("20-Sep-2026", self._TODAY) is False

    def test_unparseable_date_treated_as_not_expired(self):
        assert is_expired("not a date", self._TODAY) is False


# ---------------------------------------------------------------------------
# Tests: build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_section_3_row(self):
        row = {"designator": "kmm", "company": "KM MALTA AIRLINES PLC.", "country": "MALTA", "callsign": "SKY KNIGHT"}
        record = build_record(row)
        assert record == {
            "airline_designator": "KMM",
            "name": "KM MALTA AIRLINES PLC.",
            "country": "MALTA",
            "callsign": "SKY KNIGHT",
        }

    def test_missing_callsign_omitted_not_none(self):
        row = {"designator": "AAA", "company": "AVICON", "country": "PAKISTAN", "callsign": None}
        record = build_record(row)
        assert "callsign" not in record

    def test_section_4_row_defaults_country(self):
        row = {"designator": "NASA", "company": "National Aeronautics and Space Administration", "callsign": "NASA"}
        record = build_record(row, default_country="United States")
        assert record["country"] == "United States"

    def test_row_country_wins_over_default(self):
        row = {"designator": "KMM", "company": "KM MALTA AIRLINES", "country": "MALTA", "callsign": "SKY KNIGHT"}
        record = build_record(row, default_country="United States")
        assert record["country"] == "MALTA"


# ---------------------------------------------------------------------------
# Tests: write_to_redis (additive-only)
# ---------------------------------------------------------------------------

class _FakeRedisJson:
    """Minimal fake reproducing real JSON.SET NX semantics: writes only
    when the key is absent, returns None (no-op) when NX blocks the write,
    matching what shared.redis_json.set_json() returns from the real
    redis-py client. Also tracks EXPIRE calls (key -> most recent TTL
    seconds), so tests can assert the TTL-refresh behavior independently
    of whether the content write itself happened."""

    def __init__(self, existing: dict[str, dict] | None = None):
        self._store: dict[str, dict] = dict(existing or {})
        self.expired: dict[str, int] = {}

    def json(self, encoder=None):
        return self

    def set(self, key, path, obj, nx=False, **kwargs):
        if nx and key in self._store:
            return None
        self._store[key] = obj
        return "OK"

    def expire(self, key, seconds):
        self.expired[key] = seconds
        return True


class TestWriteToRedis:
    def test_writes_new_designator(self):
        r = _FakeRedisJson()
        rows = [{"airline_designator": "KMM", "name": "KM MALTA AIRLINES", "country": "MALTA", "callsign": "SKY KNIGHT"}]
        count = write_to_redis(rows, r)
        assert count == 1
        assert r._store["operator:KMM"]["name"] == "KM MALTA AIRLINES"

    def test_never_overwrites_an_existing_entry(self):
        """The core additive-only guarantee: pre-seed a conflicting value,
        run the writer, assert it's unchanged."""
        r = _FakeRedisJson(existing={"operator:AAL": {"airline_designator": "AAL", "name": "Pre-existing Mictronics value"}})
        rows = [{"airline_designator": "AAL", "name": "FAA all-caps value that must not win", "country": "UNITED STATES"}]
        count = write_to_redis(rows, r)
        assert count == 0
        assert r._store["operator:AAL"]["name"] == "Pre-existing Mictronics value"

    def test_mixed_batch_only_counts_new_writes(self):
        r = _FakeRedisJson(existing={"operator:AAL": {"airline_designator": "AAL", "name": "Existing"}})
        rows = [
            {"airline_designator": "AAL", "name": "Should not overwrite"},
            {"airline_designator": "KMM", "name": "New entry"},
        ]
        count = write_to_redis(rows, r)
        assert count == 1
        assert r._store["operator:AAL"]["name"] == "Existing"
        assert r._store["operator:KMM"]["name"] == "New entry"

    def test_second_run_is_a_no_op(self):
        """Idempotency: running the writer twice with the same rows only
        writes once."""
        r = _FakeRedisJson()
        rows = [{"airline_designator": "KMM", "name": "KM MALTA AIRLINES"}]
        first = write_to_redis(rows, r)
        second = write_to_redis(rows, r)
        assert first == 1
        assert second == 0

    def test_empty_list_returns_zero(self):
        r = _FakeRedisJson()
        assert write_to_redis([], r) == 0

    def test_key_is_uppercased_designator(self):
        r = _FakeRedisJson()
        rows = [{"airline_designator": "NASA", "name": "National Aeronautics and Space Administration"}]
        write_to_redis(rows, r)
        assert "operator:NASA" in r._store

    def test_ttl_refreshed_on_a_newly_written_designator(self):
        r = _FakeRedisJson()
        rows = [{"airline_designator": "KMM", "name": "KM MALTA AIRLINES"}]
        write_to_redis(rows, r)
        assert r.expired["operator:KMM"] == ENRICHMENT_TTL_SECONDS

    def test_ttl_refreshed_on_an_already_existing_entry_this_runner_never_wrote(self):
        """The whole point of refreshing independently of the content write:
        an existing mictronics-owned entry gets its TTL kept alive by this
        runner too, even though its content is never touched."""
        r = _FakeRedisJson(existing={"operator:AAL": {"airline_designator": "AAL", "name": "Pre-existing Mictronics value"}})
        rows = [{"airline_designator": "AAL", "name": "Should not overwrite"}]
        count = write_to_redis(rows, r)
        assert count == 0
        assert r._store["operator:AAL"]["name"] == "Pre-existing Mictronics value"
        assert r.expired["operator:AAL"] == ENRICHMENT_TTL_SECONDS

    def test_ttl_refreshed_for_every_row_in_a_mixed_batch(self):
        r = _FakeRedisJson(existing={"operator:AAL": {"airline_designator": "AAL", "name": "Existing"}})
        rows = [
            {"airline_designator": "AAL", "name": "Should not overwrite"},
            {"airline_designator": "KMM", "name": "New entry"},
        ]
        write_to_redis(rows, r)
        assert r.expired["operator:AAL"] == ENRICHMENT_TTL_SECONDS
        assert r.expired["operator:KMM"] == ENRICHMENT_TTL_SECONDS
