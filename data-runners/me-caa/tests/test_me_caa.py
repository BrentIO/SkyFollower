"""Tests for the Montenegro CAA data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, call, patch

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
        "me_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["me_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
_fetch_list_page = _mod._fetch_list_page
_fetch_detail_page = _mod._fetch_detail_page
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_DEREGISTERED_MARKER = _mod._DEREGISTERED_MARKER
_LIST_URL = _mod._LIST_URL


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

def _list_html(rows: list[dict]) -> str:
    header = "<tr><th>Registarska oznaka</th><th>Redni broj u registru</th><th>Ime</th><th>Tip</th></tr>"
    body = ""
    for r in rows:
        body += (
            f"<tr>"
            f"<td>{r.get('Registarska oznaka','')}</td>"
            f"<td>{r.get('Redni broj u registru','1')}</td>"
            f"<td>{r.get('Ime','')}</td>"
            f"<td>{r.get('Tip','')}</td>"
            f"</tr>"
        )
    return f"<html><body><table>{header}{body}</table></body></html>"


def _detail_html(manufacturer="Cessna", year="2003", dereg="") -> str:
    return (
        f"<html><body><table>"
        f"<tr><th>Manufacturer</th><td>{manufacturer}</td></tr>"
        f"<tr><th>Year Built</th><td>{year}</td></tr>"
        f"<tr><th>MTOM</th><td>1111</td></tr>"
        f"<tr><th>Dereg</th><td>{dereg}</td></tr>"
        f"</table></body></html>"
    )


def _make_response(html: str, status_code: int = 200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = html
    return resp


def _make_row(
    registration="4O-JAZ",
    ime="Owner Name",
    tip="C172S",
) -> dict:
    return {
        "registration": registration,
        "model": tip,
        "owner_name": ime,
        "manufacturer": "Cessna",
        "year_built": "2003",
    }


def _make_redis_with_search(icao_hex="4C0100", registration="4O-JAZ"):
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
# Tests: _fetch_list_page
# ---------------------------------------------------------------------------

class TestFetchListPage:
    def test_returns_rows(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([
            {"Registarska oznaka": "4O-JAZ", "Ime": "Owner", "Tip": "C172S"},
        ]))
        rows = _fetch_list_page(session, 0)
        assert len(rows) == 1
        assert rows[0]["Registarska oznaka"] == "4O-JAZ"

    def test_returns_empty_when_no_table(self):
        session = MagicMock()
        session.get.return_value = _make_response("<html><body><p>Nothing</p></body></html>")
        rows = _fetch_list_page(session, 0)
        assert rows == []

    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 500)
        with pytest.raises(RuntimeError, match="HTTP 500"):
            _fetch_list_page(session, 0)

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([]))
        import logging
        with patch.object(logging.getLogger("me-caa"), "info") as mock_log:
            _fetch_list_page(session, 2)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "page=2" in logged

    def test_page_number_in_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_list_html([]))
        _fetch_list_page(session, 3)
        called_url = session.get.call_args[0][0]
        assert "page=3" in called_url


# ---------------------------------------------------------------------------
# Tests: _fetch_detail_page
# ---------------------------------------------------------------------------

class TestFetchDetailPage:
    def test_extracts_manufacturer(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(manufacturer="Piper"))
        detail = _fetch_detail_page(session, "4O-JAZ")
        assert detail["Manufacturer"] == "Piper"

    def test_extracts_year_built(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(year="1998"))
        detail = _fetch_detail_page(session, "4O-JAZ")
        assert detail["Year Built"] == "1998"

    def test_extracts_dereg_when_set(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(dereg="2023-01-01"))
        detail = _fetch_detail_page(session, "4O-JAZ")
        assert detail["Dereg"] == "2023-01-01"

    def test_dereg_empty_when_active(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html(dereg=""))
        detail = _fetch_detail_page(session, "4O-JAZ")
        assert detail.get("Dereg", "") == ""

    def test_returns_empty_dict_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response("", 404)
        detail = _fetch_detail_page(session, "4O-JAZ")
        assert detail == {}

    def test_url_uses_lowercase_registration(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html())
        _fetch_detail_page(session, "4O-JAZ")
        called_url = session.get.call_args[0][0]
        assert "4o-jaz" in called_url

    def test_logs_url(self):
        session = MagicMock()
        session.get.return_value = _make_response(_detail_html())
        import logging
        with patch.object(logging.getLogger("me-caa"), "info") as mock_log:
            _fetch_detail_page(session, "4O-JAZ")
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "4o-jaz" in logged


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _make_session(self, list_pages: list[list[dict]], detail_responses: list[str]):
        session = MagicMock()
        responses = []
        for page_rows in list_pages:
            responses.append(_make_response(_list_html(page_rows)))
        # Empty page to terminate pagination
        responses.append(_make_response(_list_html([])))
        for html in detail_responses:
            responses.append(_make_response(html))
        session.get.side_effect = responses
        return session

    def test_skips_deregistered_on_list(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "4O-AAA", "Ime": _DEREGISTERED_MARKER, "Tip": "C172"},
                {"Registarska oznaka": "4O-BBB", "Ime": "Owner", "Tip": "PA28"},
            ]],
            detail_responses=[_detail_html()],
        )
        records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "4O-BBB"

    def test_skips_dereg_on_detail_page(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "4O-AAA", "Ime": "Owner", "Tip": "C172"},
            ]],
            detail_responses=[_detail_html(dereg="2022-06-01")],
        )
        records = download_and_parse(session)
        assert records == []

    def test_skips_non_4o_prefix(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "N12345", "Ime": "Owner", "Tip": "C172"},
            ]],
            detail_responses=[],
        )
        records = download_and_parse(session)
        assert records == []

    def test_paginates_until_empty(self):
        session = self._make_session(
            list_pages=[
                [{"Registarska oznaka": "4O-AAA", "Ime": "Owner A", "Tip": "C172"}],
                [{"Registarska oznaka": "4O-BBB", "Ime": "Owner B", "Tip": "PA28"}],
            ],
            detail_responses=[_detail_html(), _detail_html()],
        )
        records = download_and_parse(session)
        assert len(records) == 2

    def test_maps_fields_correctly(self):
        session = self._make_session(
            list_pages=[[
                {"Registarska oznaka": "4O-JAZ", "Ime": "Air Montenegro", "Tip": "B737"},
            ]],
            detail_responses=[_detail_html(manufacturer="Boeing", year="2010")],
        )
        records = download_and_parse(session)
        assert records[0]["registration"] == "4O-JAZ"
        assert records[0]["model"] == "B737"
        assert records[0]["owner_name"] == "Air Montenegro"
        assert records[0]["manufacturer"] == "Boeing"
        assert records[0]["year_built"] == "2010"


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert record["icao_hex"] == "4C0100"
        assert record["registration"] == "4O-JAZ"
        assert record["source"] == "me-caa"
        assert record["aircraft"]["model"] == "C172S"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["manufactured_date"] == "2003-01-01"
        assert record["registrant"]["names"] == ["Owner Name"]

    def test_year_stored_as_date(self):
        row = _make_row()
        row["year_built"] = "1999"
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert record["aircraft"]["manufactured_date"] == "1999-01-01"

    def test_non_digit_year_omitted(self):
        row = _make_row()
        row["year_built"] = "unknown"
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row()
        row["model"] = ""
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert "model" not in record.get("aircraft", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row()
        row["manufacturer"] = ""
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_owner_omits_registrant(self):
        row = _make_row()
        row["owner_name"] = ""
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row()
        row["model"] = ""
        row["manufacturer"] = ""
        row["year_built"] = ""
        record = _build_record(row, "4C0100", "4O-JAZ")
        assert "aircraft" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("4OJAZ") == "4OJAZ"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("4O-JAZ")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-JAZ")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_empty_registration_skipped(self):
        rows = [_make_row(registration="")]
        r = _make_redis_with_search()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_writes_to_detail_key(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-JAZ")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:detail:4C0100"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-JAZ")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "me-caa"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4C0100", registration="4O-JAZ")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:detail:4C0100", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(registration="4O-JAZ"), _make_row(registration="4O-ABC")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:simple:4C0100"
        doc_a.registration = "4O-JAZ"
        doc_b = MagicMock()
        doc_b.id = "aircraft:simple:4C0101"
        doc_b.registration = "4O-ABC"
        results = MagicMock()
        results.docs = [doc_a, doc_b]
        r.ft.return_value.search.return_value = results
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


# ---------------------------------------------------------------------------
# Tests: publish_completion_stats
# ---------------------------------------------------------------------------

class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("me_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 33, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 33, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "33"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("me_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/me-caa"
