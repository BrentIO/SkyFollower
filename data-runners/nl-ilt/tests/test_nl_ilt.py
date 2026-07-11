"""Tests for the Netherlands ILT data runner."""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest
from odf.opendocument import OpenDocumentSpreadsheet
from odf.table import Table, TableCell, TableRow
from odf.text import P

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
        "nl_ilt_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["nl_ilt_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_cell_text = _mod._cell_text
_read_row = _mod._read_row
_clean_header = _mod._clean_header
_find_download_url = _mod._find_download_url
parse_ods = _mod.parse_ods
_build_record = _mod._build_record
write_to_redis = _mod.write_to_redis
download_registry = _mod.download_registry
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
INDEX_URL = _mod.INDEX_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_cell(text: str = "", repeated: int | None = None) -> TableCell:
    kwargs = {}
    if repeated is not None:
        kwargs["numbercolumnsrepeated"] = repeated
    tc = TableCell(**kwargs)
    if text:
        tc.addElement(P(text=text))
    return tc


def _make_row_element(cells: list[TableCell]) -> TableRow:
    tr = TableRow()
    for c in cells:
        tr.addElement(c)
    return tr


def _make_ods(rows: list[list[str]]) -> str:
    """Build a minimal ODS file from rows of plain string values and return its path."""
    doc = OpenDocumentSpreadsheet()
    table = Table(name="Sheet1")
    for values in rows:
        tr = TableRow()
        for v in values:
            tc = TableCell()
            if v:
                tc.addElement(P(text=v))
            tr.addElement(tc)
        table.addElement(tr)
    doc.spreadsheet.addElement(table)

    tmp = tempfile.NamedTemporaryFile(suffix=".ods", delete=False)
    tmp.close()
    doc.save(tmp.name)
    return tmp.name


_HEADERS = [
    "X-Ponder [details=1][kolom=1]",
    "Registration [details=1][kolom=2]",
    "Manufacturer [details=1][kolom=3]",
    "Model [details=1][kolom=4]",
    "Serial [details=1][kolom=5]",
    "Built [details=1][kolom=6]",
    "Group [details=1][kolom=7]",
    "ICAO-code [details=1][kolom=8]",
    "Engines [details=1][kolom=9]",
    "EngKind [details=1][kolom=10]",
    "EngManufacturer [details=1][kolom=11]",
    "EngModel [details=1][kolom=12]",
]


def _make_row(
    icao_hex="4B1234",
    registration="PH-ABC",
    manufacturer="Cessna",
    model="172S",
    serial="172S12345",
    built="2010",
    group="Small aeroplane",
    icao_code="C172",
    engines="1",
    eng_kind="Reciprocating piston-driven engine",
    eng_manufacturer="Lycoming",
    eng_model="IO-360",
) -> dict:
    return {
        "X-Ponder": icao_hex,
        "Registration": registration,
        "Manufacturer": manufacturer,
        "Model": model,
        "Serial": serial,
        "Built": built,
        "Group": group,
        "ICAO-code": icao_code,
        "Engines": engines,
        "EngKind": eng_kind,
        "EngManufacturer": eng_manufacturer,
        "EngModel": eng_model,
    }


def _make_redis():
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests: _cell_text
# ---------------------------------------------------------------------------

class TestCellText:
    def test_single_paragraph(self):
        tc = _make_cell("Hello")
        assert _cell_text(tc) == "Hello"

    def test_empty_cell_returns_empty_string(self):
        tc = _make_cell("")
        assert _cell_text(tc) == ""

    def test_multiple_paragraphs_joined_with_space(self):
        tc = TableCell()
        tc.addElement(P(text="Line1"))
        tc.addElement(P(text="Line2"))
        assert _cell_text(tc) == "Line1 Line2"


# ---------------------------------------------------------------------------
# Tests: _read_row
# ---------------------------------------------------------------------------

class TestReadRow:
    def test_plain_cells(self):
        row = _make_row_element([_make_cell("A"), _make_cell("B")])
        assert _read_row(row) == ["A", "B"]

    def test_repeated_cell_expanded(self):
        row = _make_row_element([_make_cell("A"), _make_cell("B", repeated=3)])
        assert _read_row(row) == ["A", "B", "B", "B"]

    def test_large_repeat_on_empty_trailing_cell_collapsed_to_one(self):
        """A guard against implausibly large repeat counts on trailing empty
        cells (e.g. a spreadsheet declaring 1000 blank columns)."""
        row = _make_row_element([_make_cell("A"), _make_cell("", repeated=1000)])
        assert _read_row(row) == ["A", ""]

    def test_small_repeat_on_empty_cell_still_expanded(self):
        row = _make_row_element([_make_cell("A"), _make_cell("", repeated=5)])
        assert _read_row(row) == ["A", "", "", "", "", ""]


# ---------------------------------------------------------------------------
# Tests: _clean_header
# ---------------------------------------------------------------------------

class TestCleanHeader:
    def test_strips_bracket_annotations(self):
        assert _clean_header("Registration [details=1][kolom=2]") == "Registration"

    def test_no_brackets_unchanged(self):
        assert _clean_header("Registration") == "Registration"

    def test_strips_trailing_whitespace(self):
        assert _clean_header("Registration   [details=1]") == "Registration"


# ---------------------------------------------------------------------------
# Tests: _find_download_url
# ---------------------------------------------------------------------------

class TestFindDownloadUrl:
    def test_finds_absolute_url(self):
        html = (
            '<a href="https://www.ilent.nl/binaries/x/'
            'luchtvaartuigregister-ilt-datas2-2026-07-01.ods">download</a>'
        )
        with patch("nl_ilt_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = html
            mock_get.return_value = resp
            url = _find_download_url()
        assert url.endswith("luchtvaartuigregister-ilt-datas2-2026-07-01.ods")
        assert url.startswith("https://")

    def test_finds_relative_url(self):
        html = '<a href="/binaries/luchtvaartuigregister-ilt-datas2-2026-07-01.ods">download</a>'
        with patch("nl_ilt_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = html
            mock_get.return_value = resp
            url = _find_download_url()
        assert url == "https://www.ilent.nl/binaries/luchtvaartuigregister-ilt-datas2-2026-07-01.ods"

    def test_raises_on_http_error(self):
        with patch("nl_ilt_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                _find_download_url()

    def test_raises_when_no_link_found(self):
        with patch("nl_ilt_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.text = "<html><body>nothing here</body></html>"
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="Could not find"):
                _find_download_url()


# ---------------------------------------------------------------------------
# Tests: parse_ods
# ---------------------------------------------------------------------------

class TestParseOds:
    def test_parses_data_rows(self):
        path = _make_ods([
            _HEADERS,
            ["informational text row"],
            ["4B1234", "PH-ABC", "Cessna", "172S", "172S12345", "2010",
             "Small aeroplane", "C172", "1", "Reciprocating piston-driven engine",
             "Lycoming", "IO-360"],
        ])
        rows = parse_ods(path)
        assert len(rows) == 1
        assert rows[0]["X-Ponder"] == "4B1234"
        assert rows[0]["Registration"] == "PH-ABC"

    def test_headers_have_brackets_stripped(self):
        path = _make_ods([
            _HEADERS,
            ["informational text row"],
            ["4B1234", "PH-ABC"] + [""] * 10,
        ])
        rows = parse_ods(path)
        assert "X-Ponder" in rows[0]
        assert "X-Ponder [details=1][kolom=1]" not in rows[0]

    def test_informational_row_skipped(self):
        path = _make_ods([
            _HEADERS,
            ["this row is informational text, not data"],
            ["4B1234", "PH-ABC"] + [""] * 10,
        ])
        rows = parse_ods(path)
        assert len(rows) == 1

    def test_blank_data_row_skipped(self):
        path = _make_ods([
            _HEADERS,
            ["info"],
            [""] * 12,
            ["4B1234", "PH-ABC"] + [""] * 10,
        ])
        rows = parse_ods(path)
        assert len(rows) == 1

    def test_short_row_padded_with_empty_strings(self):
        path = _make_ods([
            _HEADERS,
            ["info"],
            ["4B1234", "PH-ABC"],
        ])
        rows = parse_ods(path)
        assert rows[0]["Registration"] == "PH-ABC"
        assert rows[0]["Manufacturer"] == ""

    def test_raises_on_too_few_rows(self):
        path = _make_ods([_HEADERS])
        with pytest.raises(RuntimeError, match="at least 3 rows"):
            parse_ods(path)


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row)
        assert record["icao_hex"] == "4B1234"
        assert record["registration"] == "PH-ABC"
        assert record["military"] is False
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["aircraft"]["manufactured_date"] == "2010-01-01T00:00:00Z"
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["type_designator"] == "C172"
        assert record["aircraft"]["powerplant"]["count"] == 1
        assert record["aircraft"]["powerplant"]["type"] == "Piston"
        assert record["aircraft"]["powerplant"]["manufacturer"] == "Lycoming"
        assert record["aircraft"]["powerplant"]["model"] == "IO-360"

    def test_icao_hex_uppercased(self):
        row = _make_row(icao_hex="4b1234")
        record = _build_record(row)
        assert record["icao_hex"] == "4B1234"

    def test_invalid_icao_hex_returns_none(self):
        row = _make_row(icao_hex="NOTHEX")
        assert _build_record(row) is None

    def test_short_icao_hex_returns_none(self):
        row = _make_row(icao_hex="4B12")
        assert _build_record(row) is None

    def test_empty_icao_hex_returns_none(self):
        row = _make_row(icao_hex="")
        assert _build_record(row) is None

    def test_missing_registration_returns_none(self):
        row = _make_row(registration="")
        assert _build_record(row) is None

    def test_unmapped_group_passes_through(self):
        row = _make_row(group="Something Unknown")
        record = _build_record(row)
        assert record["aircraft"]["type"] == "Something Unknown"

    def test_invalid_built_year_omitted(self):
        row = _make_row(built="not a year")
        record = _build_record(row)
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_engine_not_defined_omits_powerplant_type(self):
        row = _make_row(eng_kind="Engine - not defined")
        record = _build_record(row)
        assert "type" not in record["aircraft"].get("powerplant", {})

    def test_unknown_eng_kind_passes_through(self):
        row = _make_row(eng_kind="Some Future Engine Type")
        record = _build_record(row)
        assert record["aircraft"]["powerplant"]["type"] == "Some Future Engine Type"

    def test_eng_manufacturer_unknown_filtered(self):
        row = _make_row(eng_manufacturer="Unknown")
        record = _build_record(row)
        assert "manufacturer" not in record["aircraft"].get("powerplant", {})

    def test_eng_manufacturer_unknown_case_insensitive(self):
        row = _make_row(eng_manufacturer="UNKNOWN")
        record = _build_record(row)
        assert "manufacturer" not in record["aircraft"].get("powerplant", {})

    def test_eng_model_not_further_defined_filtered(self):
        row = _make_row(eng_model="Not further defined")
        record = _build_record(row)
        assert "model" not in record["aircraft"].get("powerplant", {})

    def test_non_digit_engines_count_omitted(self):
        row = _make_row(engines="several")
        record = _build_record(row)
        assert "count" not in record["aircraft"].get("powerplant", {})

    def test_no_powerplant_fields_omits_powerplant_key(self):
        row = _make_row(engines="", eng_kind="", eng_manufacturer="", eng_model="")
        record = _build_record(row)
        assert "powerplant" not in record.get("aircraft", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row)
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_group_omits_type(self):
        row = _make_row(group="")
        record = _build_record(row)
        assert "type" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written(self):
        rows = [_make_row()]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_invalid_icao_hex_skipped(self):
        rows = [_make_row(icao_hex="NOTHEX")]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_writes_to_registry_key(self):
        rows = [_make_row(icao_hex="4B1234")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4B1234"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "nl-ilt"

    def test_ttl_applied(self):
        rows = [_make_row(icao_hex="4B1234")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4B1234", REDIS_TTL)

    def test_empty_rows_returns_zero(self):
        r = _make_redis()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records(self):
        rows = [_make_row(icao_hex="4B1234"), _make_row(icao_hex="4B5678")]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 2


# ---------------------------------------------------------------------------
# Tests: download_registry
# ---------------------------------------------------------------------------

class TestDownloadRegistry:
    def test_writes_temp_file(self):
        with patch("nl_ilt_main._find_download_url", return_value="https://www.ilent.nl/x.ods"):
            with patch("nl_ilt_main.requests.get") as mock_get:
                resp = MagicMock()
                resp.status_code = 200
                resp.content = b"fake ods bytes"
                mock_get.return_value = resp
                path = download_registry()
        try:
            assert os.path.exists(path)
            with open(path, "rb") as f:
                assert f.read() == b"fake ods bytes"
        finally:
            os.unlink(path)

    def test_raises_on_http_error(self):
        with patch("nl_ilt_main._find_download_url", return_value="https://www.ilent.nl/x.ods"):
            with patch("nl_ilt_main.requests.get") as mock_get:
                resp = MagicMock()
                resp.status_code = 503
                mock_get.return_value = resp
                with pytest.raises(RuntimeError, match="HTTP 503"):
                    download_registry()


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
        with patch("nl_ilt_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nl_ilt_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nl_ilt_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nl_ilt_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/nl-ilt"
