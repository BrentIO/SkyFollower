"""Tests for the Suriname CASAS data runner."""

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
        "sr_casas_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["sr_casas_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


_discover_xlsx_url = _mod._discover_xlsx_url
download_and_parse = _mod.download_and_parse
_build_record = _mod._build_record
_clean = _mod._clean
_escape_tag = _mod._escape_tag
_build_registration_map = _mod._build_registration_map
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
_INDEX_URL = _mod._INDEX_URL
MQTT_ROOT = _mod.MQTT_ROOT

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_HEADERS = [
    "#", "MAKE", "MODEL", "SERIES", "MANUFACTURER", "SERIAL_NUMBER",
    "NATIONALITY MARK OR COMMON MARK", "REGISTRATION MARK", "OWNER_NAME", "OPERATOR (*)",
]


def _make_xlsx_row(
    num=1,
    make="GRUMMAN",
    model="G164",
    series="B",
    manufacturer="SCHWEIZER",
    serial="185B",
    nationality="PZ",
    registration="UBD",
    owner="SURINAM SKY FARMERS",
    operator="SURINAM SKY FARMERS (AG)",
):
    return [num, make, model, series, manufacturer, serial, nationality, registration, owner, operator]


def _make_session(content=b"", status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.content = content
    resp.text = content.decode("utf-8", errors="replace") if isinstance(content, bytes) else content
    session = MagicMock()
    session.get.return_value = resp
    return session


def _xlsx_bytes(rows: list[list]) -> bytes:
    """Build a real xlsx file in memory from a list of rows (first = header)."""
    import io as _io
    import openpyxl as _openpyxl

    wb = _openpyxl.Workbook()
    ws = wb.active
    for row in rows:
        ws.append(row)
    buf = _io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


_INDEX_HTML = b"""
<html><body>
<a href="https://www.casas.sr/wp-content/uploads/2026/06/REGISTER-05-2026.xlsx">Civil Aircraft Register</a>
<a href="https://www.casas.sr/wp-content/uploads/2026/02/CASAS-UAS-REGISTRY-FEB-2026.xlsx">UAS Registry</a>
</body></html>
"""


# ---------------------------------------------------------------------------
# _clean
# ---------------------------------------------------------------------------


class TestClean:
    def test_strips_whitespace(self):
        assert _clean("  hello  ") == "hello"

    def test_collapses_internal_whitespace(self):
        assert _clean("hello   world") == "hello world"

    def test_none_returns_empty(self):
        assert _clean(None) == ""

    def test_empty_returns_empty(self):
        assert _clean("") == ""

    def test_int_coerced_to_string(self):
        """Source has mixed int/str serial numbers -- must not raise."""
        assert _clean(711) == "711"

    def test_float_coerced_to_string(self):
        assert _clean(185.0) == "185.0"


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("PZABC") == "PZABC"

    def test_hyphen_escaped(self):
        assert _escape_tag("PZ-ABC") == r"PZ\-ABC"


# ---------------------------------------------------------------------------
# _discover_xlsx_url
# ---------------------------------------------------------------------------


class TestDiscoverXlsxUrl:
    def test_finds_civil_register_not_uas(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_xlsx_url(session)
        assert url == "https://www.casas.sr/wp-content/uploads/2026/06/REGISTER-05-2026.xlsx"

    def test_ignores_uas_registry_link(self):
        session = _make_session(_INDEX_HTML)
        url = _discover_xlsx_url(session)
        assert "UAS" not in url

    def test_logs_index_url(self):
        session = _make_session(_INDEX_HTML)
        with patch.object(_mod, "logger") as mock_logger:
            _discover_xlsx_url(session)
        mock_logger.info.assert_any_call(
            "Downloading Suriname CASAS registry page from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = _make_session(status_code=404)
        with pytest.raises(RuntimeError, match="Registry page request failed with HTTP 404"):
            _discover_xlsx_url(session)

    def test_no_link_found_raises(self):
        session = _make_session(b"<html><body>nothing here</body></html>")
        with pytest.raises(RuntimeError, match="No Civil Aircraft Register xlsx link found"):
            _discover_xlsx_url(session)

    def test_only_uas_link_present_raises(self):
        """A page with only the UAS link (no civil register) must not match."""
        html = b'<a href="https://www.casas.sr/wp-content/uploads/2026/02/CASAS-UAS-REGISTRY-FEB-2026.xlsx">UAS</a>'
        session = _make_session(html)
        with pytest.raises(RuntimeError, match="No Civil Aircraft Register xlsx link found"):
            _discover_xlsx_url(session)


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def _make_two_call_session(self, index_html, xlsx_bytes, xlsx_status=200):
        index_resp = MagicMock()
        index_resp.ok = True
        index_resp.status_code = 200
        index_resp.content = index_html
        index_resp.text = index_html.decode()

        xlsx_resp = MagicMock()
        xlsx_resp.ok = xlsx_status < 400
        xlsx_resp.status_code = xlsx_status
        xlsx_resp.content = xlsx_bytes

        session = MagicMock()
        session.get.side_effect = [index_resp, xlsx_resp]
        return session

    def test_xlsx_http_error_raises(self):
        session = self._make_two_call_session(_INDEX_HTML, b"", xlsx_status=503)
        with pytest.raises(RuntimeError, match="Xlsx request failed with HTTP 503"):
            download_and_parse(session)

    def test_parses_valid_rows(self):
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(registration="UBD"), _make_xlsx_row(num=2, registration="UBI")])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "PZ-UBD"
        assert rows[1]["registration"] == "PZ-UBI"

    def test_concatenates_nationality_and_registration(self):
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(nationality="PZ", registration="ABC")])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert rows[0]["registration"] == "PZ-ABC"

    def test_skips_blank_rows(self):
        xlsx = _xlsx_bytes([_HEADERS, [None] * 10, _make_xlsx_row()])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_row_missing_registration(self):
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(registration="")])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert len(rows) == 0

    def test_skips_row_missing_nationality(self):
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(nationality="")])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert len(rows) == 0

    def test_mixed_type_serial_number_handled(self):
        """Real source has both string ('185B') and int (711) serial numbers."""
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(serial=711)])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert rows[0]["serial"] == "711"

    def test_none_series_handled(self):
        """~14% of real rows have no SERIES value at all."""
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row(series=None)])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        rows = download_and_parse(session)
        assert rows[0]["series"] == ""

    def test_logs_parsed_count(self):
        xlsx = _xlsx_bytes([_HEADERS, _make_xlsx_row()])
        session = self._make_two_call_session(_INDEX_HTML, xlsx)
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d records.", 1)


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def _row(self, **overrides):
        row = {
            "make": "GRUMMAN",
            "model": "G164",
            "series": "B",
            "serial": "185B",
            "owner": "SURINAM SKY FARMERS",
        }
        row.update(overrides)
        return row

    def test_basic_fields(self):
        record = _build_record(self._row(), "3A1234", "PZ-UBD")
        assert record["icao_hex"] == "3A1234"
        assert record["registration"] == "PZ-UBD"
        assert record["source"] == "sr-casas-registry"
        assert record["military"] is False

    def test_manufacturer_from_make(self):
        record = _build_record(self._row(make="GRUMMAN"), "3A1234", "PZ-UBD")
        assert record["aircraft"]["manufacturer"] == "GRUMMAN"

    def test_model_with_series_appended(self):
        record = _build_record(self._row(model="G164", series="B"), "3A1234", "PZ-UBD")
        assert record["aircraft"]["model"] == "G164B"

    def test_model_without_series(self):
        record = _build_record(self._row(model="R44", series=""), "3A1234", "PZ-UBD")
        assert record["aircraft"]["model"] == "R44"

    def test_trailing_asterisk_stripped_from_model(self):
        """Real source has one row with 'G164*' -- the asterisk is a stray
        marker, not meaningful model data."""
        record = _build_record(self._row(model="G164*", series=""), "3A1234", "PZ-UBD")
        assert record["aircraft"]["model"] == "G164"

    def test_serial_number(self):
        record = _build_record(self._row(serial="185B"), "3A1234", "PZ-UBD")
        assert record["aircraft"]["serial_number"] == "185B"

    def test_registrant_names_from_owner(self):
        record = _build_record(self._row(owner="SURINAM SKY FARMERS"), "3A1234", "PZ-UBD")
        assert record["registrant"]["names"] == ["SURINAM SKY FARMERS"]

    def test_operator_not_stored(self):
        """Matches the established repo-wide convention: when both an owner
        and operator column exist, only owner is captured."""
        row = self._row()
        row["operator"] = "SURINAM SKY FARMERS (AG)"
        record = _build_record(row, "3A1234", "PZ-UBD")
        assert "operator" not in record

    def test_empty_owner_omits_registrant(self):
        record = _build_record(self._row(owner=""), "3A1234", "PZ-UBD")
        assert "registrant" not in record

    def test_empty_fields_omit_aircraft(self):
        record = _build_record(self._row(make="", model="", serial=""), "3A1234", "PZ-UBD")
        assert "aircraft" not in record

    def test_source_is_sr_casas(self):
        record = _build_record(self._row(), "3A1234", "PZ-UBD")
        assert record["source"] == "sr-casas-registry"


# ---------------------------------------------------------------------------
# _build_registration_map
# ---------------------------------------------------------------------------


class TestBuildRegistrationMap:
    def _make_redis(self, docs):
        doc_mocks = []
        for icao_hex, registration in docs:
            doc = MagicMock()
            doc.id = f"aircraft:mictronics:{icao_hex}"
            doc.registration = registration
            doc_mocks.append(doc)
        result = MagicMock()
        result.docs = doc_mocks
        r = MagicMock()
        r.ft.return_value.search.return_value = result
        return r

    def test_returns_registration_to_hex_map(self):
        r = self._make_redis([("3A1234", "PZ-UBD"), ("3A1235", "PZ-UBI")])
        reg_map = _build_registration_map(["PZ-UBD", "PZ-UBI"], r)
        assert reg_map["PZ-UBD"] == "3A1234"
        assert reg_map["PZ-UBI"] == "3A1235"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["PZ-UBD"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_registrations_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"3A{i:04d}", f"PZ-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"PZ-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def _row(self, registration="PZ-UBD"):
        return {
            "registration": registration,
            "make": "GRUMMAN",
            "model": "G164",
            "series": "B",
            "serial": "185B",
            "owner": "SURINAM SKY FARMERS",
        }

    def test_writes_found_registrations(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"PZ-UBD": "3A1234"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [self._row()]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_skips_empty_registration(self):
        rows = [self._row(registration="")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"PZ-UBD": "3A1234"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:3A1234"

    def test_pipeline_error_logs_warning(self):
        rows = [self._row()]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"PZ-UBD": "3A1234"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()


# ---------------------------------------------------------------------------
# publish_completion_stats
# ---------------------------------------------------------------------------


class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_missing_mqtt_block_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_blank_host_skips_without_crashing(self):
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch.object(_mod.mqtt, "Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/sr-casas-registry"
