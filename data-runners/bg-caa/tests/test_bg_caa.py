"""Tests for the Bulgaria CAA data runner."""

from __future__ import annotations

import importlib.util
import io
import os
import sys
from unittest.mock import MagicMock, patch

import openpyxl
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
        "bg_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["bg_caa_main"] = mod
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
_INDEX_URL = _mod._INDEX_URL


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_row(
    registration="LZ-ABC",
    model="Cessna 172",
    serial="17280001",
    category="Small Aeroplane",
):
    return {
        "registration": registration,
        "model": model,
        "serial": serial,
        "category": category,
    }


def _make_xlsx_bytes(data_rows: list[tuple]) -> bytes:
    """Build an in-memory xlsx with info row, header row, then data rows."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(("Info text about the register",))
    ws.append(("Рег. №", "Дата", "Модел", "Сериен №", "Рег. знак", "Категория ВС", "Основание"))
    for row in data_rows:
        ws.append(row)
    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


_INDEX_HTML = b"""
<html><body>
<a href="/files/Aircraft_Register_20260619.xlsx">Download</a>
</body></html>
"""

_INDEX_HTML_FULL_URL = b"""
<html><body>
<a href="https://www.caa.bg/files/Aircraft_Register_20260619.xlsx">Download</a>
</body></html>
"""


def _make_session(index_content=_INDEX_HTML, xlsx_bytes=None, index_status=200, xlsx_status=200):
    index_resp = MagicMock()
    index_resp.ok = index_status < 400
    index_resp.status_code = index_status
    index_resp.content = index_content
    index_resp.text = index_content.decode("utf-8", errors="replace")

    xlsx_resp = MagicMock()
    xlsx_resp.ok = xlsx_status < 400
    xlsx_resp.status_code = xlsx_status
    xlsx_resp.content = xlsx_bytes or b""

    session = MagicMock()
    session.get.side_effect = [index_resp, xlsx_resp]
    return session


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

    def test_int_coerced_to_string(self):
        assert _clean(46192) == "46192"

    def test_float_coerced_to_string(self):
        assert _clean(46192.0) == "46192.0"


# ---------------------------------------------------------------------------
# _escape_tag
# ---------------------------------------------------------------------------


class TestEscapeTag:
    def test_plain_unchanged(self):
        assert _escape_tag("LZABC") == "LZABC"

    def test_hyphen_escaped(self):
        assert _escape_tag("LZ-ABC") == r"LZ\-ABC"


# ---------------------------------------------------------------------------
# _discover_xlsx_url
# ---------------------------------------------------------------------------


class TestDiscoverXlsxUrl:
    def _make_index_session(self, content, status=200):
        resp = MagicMock()
        resp.ok = status < 400
        resp.status_code = status
        resp.content = content
        resp.text = content.decode("utf-8", errors="replace")
        session = MagicMock()
        session.get.return_value = resp
        return session

    def test_returns_absolute_url_from_relative_href(self):
        session = self._make_index_session(_INDEX_HTML)
        url = _discover_xlsx_url(session)
        assert url == "https://www.caa.bg/files/Aircraft_Register_20260619.xlsx"

    def test_returns_absolute_url_unchanged(self):
        session = self._make_index_session(_INDEX_HTML_FULL_URL)
        url = _discover_xlsx_url(session)
        assert url == "https://www.caa.bg/files/Aircraft_Register_20260619.xlsx"

    def test_logs_index_url(self):
        session = self._make_index_session(_INDEX_HTML)
        with patch.object(_mod, "logger") as mock_logger:
            _discover_xlsx_url(session)
        mock_logger.info.assert_any_call(
            "Downloading Bulgaria CAA index page from %s", _INDEX_URL
        )

    def test_http_error_raises(self):
        session = self._make_index_session(b"", status=404)
        with pytest.raises(RuntimeError, match="Index page request failed with HTTP 404"):
            _discover_xlsx_url(session)

    def test_no_xlsx_link_raises(self):
        session = self._make_index_session(b"<html><body><a href='/other'>no xlsx</a></body></html>")
        with pytest.raises(RuntimeError, match="No Aircraft_Register xlsx link found"):
            _discover_xlsx_url(session)


# ---------------------------------------------------------------------------
# download_and_parse
# ---------------------------------------------------------------------------


class TestDownloadAndParse:
    def test_logs_xlsx_url(self):
        xlsx_bytes = _make_xlsx_bytes([])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call(
            "Downloading Bulgaria CAA aircraft register from %s",
            "https://www.caa.bg/files/Aircraft_Register_20260619.xlsx",
        )

    def test_xlsx_http_error_raises(self):
        session = _make_session(xlsx_status=503)
        with pytest.raises(RuntimeError, match="Xlsx request failed with HTTP 503"):
            download_and_parse(session)

    def test_parses_lz_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", "SN001", "LZ-ABC", "Cat", "Basis"),
            (2, 46193, "Piper PA-28", "SN002", "LZ-XYZ", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert len(rows) == 2
        assert rows[0]["registration"] == "LZ-ABC"
        assert rows[1]["registration"] == "LZ-XYZ"

    def test_skips_non_lz_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", "SN001", "LZ-ABC", "Cat", "Basis"),
            (2, 46193, "Piper PA-28", "SN002", "G-ABCD", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "LZ-ABC"

    def test_skips_header_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", "SN001", "LZ-ABC", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert len(rows) == 1

    def test_skips_short_rows(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172"),  # only 3 cols
            (2, 46193, "Piper", "SN002", "LZ-XYZ", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert len(rows) == 1
        assert rows[0]["registration"] == "LZ-XYZ"

    def test_maps_model_serial_and_category(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", "SN001", "LZ-ABC", "Small Aeroplane", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert rows[0]["model"] == "Cessna 172"
        assert rows[0]["serial"] == "SN001"
        assert rows[0]["category"] == "Small Aeroplane"

    def test_logs_parsed_count(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", "SN001", "LZ-ABC", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        with patch.object(_mod, "logger") as mock_logger:
            download_and_parse(session)
        mock_logger.info.assert_any_call("Parsed %d LZ- records.", 1)

    def test_numeric_serial_coerced_to_string(self):
        xlsx_bytes = _make_xlsx_bytes([
            (1, 46192, "Cessna 172", 12345, "LZ-ABC", "Cat", "Basis"),
        ])
        session = _make_session(xlsx_bytes=xlsx_bytes)
        rows = download_and_parse(session)
        assert rows[0]["serial"] == "12345"


# ---------------------------------------------------------------------------
# _build_record
# ---------------------------------------------------------------------------


class TestBuildRecord:
    def test_basic_fields(self):
        record = _build_record(_make_row(), "42A001", "LZ-ABC")
        assert record["icao_hex"] == "42A001"
        assert record["registration"] == "LZ-ABC"
        assert record["source"] == "bg-caa"

    def test_aircraft_model(self):
        record = _build_record(_make_row(model="Cessna 172"), "42A001", "LZ-ABC")
        assert record["aircraft"]["model"] == "Cessna 172"

    def test_aircraft_serial_number(self):
        record = _build_record(_make_row(serial="17280001"), "42A001", "LZ-ABC")
        assert record["aircraft"]["serial_number"] == "17280001"

    def test_aircraft_type_mapped_from_category(self):
        record = _build_record(_make_row(category="Small Aeroplane"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Airplane"

    def test_category_case_insensitive(self):
        record = _build_record(_make_row(category="small aeroplane"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Airplane"

    def test_sailplane_maps_to_glider(self):
        record = _build_record(_make_row(category="Sailplane"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Glider"

    def test_rotorcraft_maps_to_rotorcraft(self):
        record = _build_record(_make_row(category="Rotorcraft"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Rotorcraft"

    def test_hot_air_balloon_maps_to_balloon(self):
        record = _build_record(_make_row(category="Hot-air Balloon"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Balloon"

    def test_powered_sailplane_maps_to_powered_glider(self):
        record = _build_record(_make_row(category="Powered Sailplane"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Powered Glider"

    def test_motor_hanglider_maps_to_wsc(self):
        record = _build_record(_make_row(category="Motor-hanglider"), "42A001", "LZ-ABC")
        assert record["aircraft"]["type"] == "Weight-Shift-Control"

    def test_experimental_omits_type(self):
        record = _build_record(_make_row(category="Experimental"), "42A001", "LZ-ABC")
        assert "type" not in record.get("aircraft", {})

    def test_unknown_category_omits_type(self):
        record = _build_record(_make_row(category="Unknown Category"), "42A001", "LZ-ABC")
        assert "type" not in record.get("aircraft", {})

    def test_empty_category_omits_type(self):
        record = _build_record(_make_row(category=""), "42A001", "LZ-ABC")
        assert "type" not in record.get("aircraft", {})

    def test_empty_model_omits_aircraft(self):
        record = _build_record(_make_row(model="", serial="", category=""), "42A001", "LZ-ABC")
        assert "aircraft" not in record

    def test_empty_model_but_serial_present(self):
        record = _build_record(_make_row(model="", serial="SN001"), "42A001", "LZ-ABC")
        assert "model" not in record["aircraft"]
        assert record["aircraft"]["serial_number"] == "SN001"

    def test_source_is_bg_caa(self):
        record = _build_record(_make_row(), "42A001", "LZ-ABC")
        assert record["source"] == "bg-caa"

    def test_no_registrant_field(self):
        record = _build_record(_make_row(), "42A001", "LZ-ABC")
        assert "registrant" not in record


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
        r = self._make_redis([("42A001", "LZ-ABC"), ("42A002", "LZ-XYZ")])
        reg_map = _build_registration_map(["LZ-ABC", "LZ-XYZ"], r)
        assert reg_map["LZ-ABC"] == "42A001"
        assert reg_map["LZ-XYZ"] == "42A002"

    def test_redis_failure_logs_warning(self):
        r = MagicMock()
        r.ft.return_value.search.side_effect = Exception("connection refused")
        with patch.object(_mod, "logger") as mock_logger:
            result = _build_registration_map(["LZ-ABC"], r)
        assert result == {}
        mock_logger.warning.assert_called_once()

    def test_empty_list_returns_empty(self):
        r = MagicMock()
        result = _build_registration_map([], r)
        assert result == {}
        r.ft.return_value.search.assert_not_called()

    def test_batches_large_lists(self):
        docs = [(f"42{i:04d}", f"LZ-{i:03d}") for i in range(150)]
        r = self._make_redis(docs)
        regs = [f"LZ-{i:03d}" for i in range(150)]
        _build_registration_map(regs, r)
        assert r.ft.return_value.search.call_count == 2


# ---------------------------------------------------------------------------
# write_to_redis
# ---------------------------------------------------------------------------


class TestWriteToRedis:
    def test_writes_found_registrations(self):
        rows = [_make_row(registration="LZ-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"LZ-ABC": "42A001"}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 1
        pipe.json.return_value.set.assert_called_once()
        pipe.expire.assert_called_once()

    def test_skips_no_redis_match(self):
        rows = [_make_row(registration="LZ-ABC")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_skips_empty_registration(self):
        rows = [_make_row(registration="")]
        r = MagicMock()
        r.pipeline.return_value = MagicMock()
        with patch.object(_mod, "_build_registration_map", return_value={}):
            count = write_to_redis(rows, r, 1209600)
        assert count == 0

    def test_uses_aircraft_registry_key(self):
        rows = [_make_row(registration="LZ-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"LZ-ABC": "42A001"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:42A001"

    def test_pipeline_error_logs_warning(self):
        rows = [_make_row(registration="LZ-ABC")]
        r = MagicMock()
        pipe = MagicMock()
        pipe.execute.side_effect = Exception("Redis timeout")
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"LZ-ABC": "42A001"}):
            with patch.object(_mod, "logger") as mock_logger:
                write_to_redis(rows, r, 1209600)
        mock_logger.warning.assert_called()

    def test_null_fields_omitted_from_written_record(self):
        """Optional fields with no source data must be absent from the written record, not None."""
        rows = [_make_row(registration="LZ-ABC", model="", category="")]
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        with patch.object(_mod, "_build_registration_map", return_value={"LZ-ABC": "42A001"}):
            write_to_redis(rows, r, 1209600)
        set_call = pipe.json.return_value.set.call_args
        written = set_call[0][2]
        assert "model" not in written["aircraft"]
        assert "type" not in written["aircraft"]
