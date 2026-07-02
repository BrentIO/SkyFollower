"""Tests for the Georgia GCAA data runner."""

from __future__ import annotations

import importlib.util
import os
import sys
from unittest.mock import MagicMock, patch

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
        "ge_gcaa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ge_gcaa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_parse_table = _mod._parse_table
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_PAGE_URL = _mod._PAGE_URL


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

def _table_html(rows: list[list[str]], table_id: str, header: list[str] | None = None) -> str:
    if header is None:
        header = ["Party", "Aircraft", "Registration", "Date", "Serial", "Year"]
    header_html = "<tr>" + "".join(f"<th>{h}</th>" for h in header) + "</tr>"
    rows_html = ""
    for row in rows:
        rows_html += "<tr>" + "".join(f"<td>{c}</td>" for c in row) + "</tr>"
    return f'<table id="{table_id}">{header_html}{rows_html}</table>'


def _page_html(table1_rows: list[list[str]], table2_rows: list[list[str]]) -> str:
    t1 = _table_html(table1_rows, "table_1")
    t2 = _table_html(table2_rows, "table_2")
    return f"<html><body>{t1}{t2}</body></html>"


def _data_row(
    party="Georgian Airways",
    model="Boeing 737-800",
    registration="4L-GAA",
    date="27.07.2007",
    serial="12345",
    year="2005",
) -> list[str]:
    return [party, model, registration, date, serial, year]


def _make_response(html: str = "", status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = html
    return resp


def _make_merged_row(
    registration="4L-GAA",
    model="Boeing 737-800",
    serial="12345",
    year="2005",
    operator="Georgian Airways",
    owner="",
) -> dict:
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "year": year,
        "operator": operator,
        "owner": owner,
    }


def _make_redis_with_search(icao_hex="4A1234", registration="4L-GAA"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:simple:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    return r


def _make_redis_no_match():
    r = MagicMock()
    results = MagicMock()
    results.docs = []
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _parse_table
# ---------------------------------------------------------------------------

class TestParseTable:
    def _soup_table(self, rows, table_id="table_1"):
        from bs4 import BeautifulSoup
        html = _table_html(rows, table_id)
        return BeautifulSoup(html, "lxml").find(id=table_id)

    def test_parses_basic_row(self):
        table = self._soup_table([_data_row()])
        records = _parse_table(table)
        assert len(records) == 1
        assert records[0]["registration"] == "4L-GAA"
        assert records[0]["model"] == "Boeing 737-800"
        assert records[0]["serial"] == "12345"
        assert records[0]["year"] == "2005"
        assert records[0]["party"] == "Georgian Airways"

    def test_skips_non_4l_rows(self):
        table = self._soup_table([_data_row(registration="G-ABCD")])
        assert _parse_table(table) == []

    def test_skips_header_row(self):
        table = self._soup_table([_data_row()])
        records = _parse_table(table)
        assert len(records) == 1  # only data row, not header

    def test_skips_short_rows(self):
        from bs4 import BeautifulSoup
        html = f'<table id="t"><tr><th>h</th></tr><tr><td>4L-GAA</td><td>x</td></tr></table>'
        table = BeautifulSoup(html, "lxml").find("table")
        assert _parse_table(table) == []

    def test_whitespace_normalised_in_cells(self):
        table = self._soup_table([_data_row(party="  Air  Georgia  ")])
        records = _parse_table(table)
        assert records[0]["party"] == "Air Georgia"

    def test_georgian_script_preserved(self):
        table = self._soup_table([_data_row(party="შპს ჯორჯიან ეარვეისი (Georgian Airways)")])
        records = _parse_table(table)
        assert records[0]["party"] == "შპს ჯორჯიან ეარვეისი (Georgian Airways)"


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            download_and_parse(session)

    def test_raises_when_table_1_missing(self):
        html = '<html><body><table id="table_2"><tr><th>h</th></tr></table></body></html>'
        session = MagicMock()
        session.get.return_value = _make_response(html)
        with pytest.raises(RuntimeError, match="table_1 not found"):
            download_and_parse(session)

    def test_raises_when_table_2_missing(self):
        html = '<html><body><table id="table_1"><tr><th>h</th></tr></table></body></html>'
        session = MagicMock()
        session.get.return_value = _make_response(html)
        with pytest.raises(RuntimeError, match="table_2 not found"):
            download_and_parse(session)

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        import logging
        with patch.object(logging.getLogger("ge-gcaa"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _PAGE_URL in logged

    def test_merges_operator_and_owner_by_registration(self):
        html = _page_html(
            [_data_row(party="Op Co", registration="4L-AAA")],
            [_data_row(party="Own Co", registration="4L-AAA")],
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        r = records[0]
        assert r["operator"] == "Op Co"
        assert r["owner"] == "Own Co"
        assert r["registration"] == "4L-AAA"

    def test_operator_only_aircraft_included(self):
        html = _page_html(
            [_data_row(registration="4L-AAA")],
            [],
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["owner"] == ""

    def test_owner_only_aircraft_included(self):
        html = _page_html(
            [],
            [_data_row(party="Own Co", registration="4L-BBB")],
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["operator"] == ""
        assert records[0]["owner"] == "Own Co"

    def test_model_from_table2_when_table1_empty_model(self):
        html = _page_html(
            [_data_row(model="", registration="4L-AAA")],
            [_data_row(model="Boeing 737", registration="4L-AAA")],
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        assert records[0]["model"] == "Boeing 737"

    def test_unique_registrations_across_tables(self):
        html = _page_html(
            [_data_row(registration="4L-AAA"), _data_row(registration="4L-BBB")],
            [_data_row(registration="4L-BBB"), _data_row(registration="4L-CCC")],
        )
        session = MagicMock()
        session.get.return_value = _make_response(html)
        records = download_and_parse(session)
        regs = {r["registration"] for r in records}
        assert regs == {"4L-AAA", "4L-BBB", "4L-CCC"}


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record_with_operator_and_owner(self):
        row = _make_merged_row(owner="Own Co")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert record["icao_hex"] == "4A1234"
        assert record["registration"] == "4L-GAA"
        assert record["source"] == "ge-gcaa"
        assert record["aircraft"]["model"] == "Boeing 737-800"
        assert record["aircraft"]["serial_number"] == "12345"
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"
        assert record["registrant"]["names"] == ["Georgian Airways", "Own Co"]

    def test_owner_omitted_when_same_as_operator(self):
        row = _make_merged_row(operator="Same Co", owner="Same Co")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert record["registrant"]["names"] == ["Same Co"]

    def test_only_owner_when_no_operator(self):
        row = _make_merged_row(operator="", owner="Own Co")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert record["registrant"]["names"] == ["Own Co"]

    def test_no_registrant_when_no_parties(self):
        row = _make_merged_row(operator="", owner="")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "registrant" not in record

    def test_year_stored_as_date(self):
        row = _make_merged_row(year="1998")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert record["aircraft"]["manufactured_date"] == "1998-01-01"

    def test_invalid_year_omitted(self):
        row = _make_merged_row(year="abcd")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_out_of_range_year_omitted(self):
        row = _make_merged_row(year="1800")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_merged_row(model="")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_merged_row(serial="")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_no_aircraft_key_when_all_fields_empty(self):
        row = _make_merged_row(model="", serial="", year="")
        record = _build_record(row, "4A1234", "4L-GAA")
        assert "aircraft" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("4L-GAA")

    def test_plain_value_unchanged(self):
        assert _escape_tag("ABCD") == "ABCD"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found(self):
        rows = [_make_merged_row()]
        r = _make_redis_with_search(icao_hex="4A1234", registration="4L-GAA")
        assert write_to_redis(rows, r, REDIS_TTL) == 1

    def test_record_not_written_when_not_found(self):
        rows = [_make_merged_row()]
        r = _make_redis_no_match()
        assert write_to_redis(rows, r, REDIS_TTL) == 0

    def test_empty_registration_skipped(self):
        rows = [_make_merged_row(registration="")]
        r = _make_redis_with_search()
        assert write_to_redis(rows, r, REDIS_TTL) == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_merged_row()]
        r = _make_redis_with_search(icao_hex="4A1234", registration="4L-GAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:detail:4A1234"

    def test_source_field_in_written_record(self):
        rows = [_make_merged_row()]
        r = _make_redis_with_search(icao_hex="4A1234", registration="4L-GAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "ge-gcaa"

    def test_ttl_applied(self):
        rows = [_make_merged_row()]
        r = _make_redis_with_search(icao_hex="4A1234", registration="4L-GAA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:detail:4A1234", REDIS_TTL)

    def test_empty_list_returns_zero(self):
        assert write_to_redis([], _make_redis_no_match(), REDIS_TTL) == 0


# ---------------------------------------------------------------------------
# Tests: publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mc = MagicMock()

        def fake_connect(host, port, keepalive):
            mc.on_connect(mc, None, None, 0, None)

        mc.connect.side_effect = fake_connect
        return mc

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("ge_gcaa_main.mqtt.Client") as mock_cls:
            mock_cls.return_value = MagicMock()
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ge_gcaa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 63, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ge_gcaa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 63, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "63"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ge_gcaa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/ge-gcaa"
