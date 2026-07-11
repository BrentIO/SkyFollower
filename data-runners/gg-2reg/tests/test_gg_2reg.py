"""Tests for the Guernsey 2-reg data runner."""

from __future__ import annotations

import importlib.util
import io
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
        "gg_2reg_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["gg_2reg_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_col_index = _mod._col_index
_words_to_cols = _mod._words_to_cols
_find_pdf_url = _mod._find_pdf_url
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_INDEX_URL = _mod._INDEX_URL
_SKIP_PREFIXES = _mod._SKIP_PREFIXES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_word(text: str, x0: float, top: float = 100.0) -> dict:
    return {"text": text, "x0": x0, "top": top}


def _make_response(text: str = "", status_code: int = 200, content: bytes = b""):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.text = text
    resp.content = content
    return resp


def _make_index_page(pdf_url: str) -> str:
    return f'<html><body><a href="{pdf_url}">Register PDF</a></body></html>'


def _make_row(
    registration="2-ABCD",
    manufacturer="Airbus S.A.S.",
    model="A320-214",
    serial="1234",
    owner="Test Owner Ltd.",
) -> dict:
    return {
        "registration": registration,
        "manufacturer": manufacturer,
        "model": model,
        "serial": serial,
        "owner": owner,
    }


def _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD"):
    r = MagicMock()
    doc = MagicMock()
    doc.id = f"aircraft:mictronics:{icao_hex}"
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
# Tests: _col_index
# ---------------------------------------------------------------------------

class TestColIndex:
    def test_registration_col(self):
        assert _col_index(27.8) == 0
        assert _col_index(100.0) == 0

    def test_manufacturer_col(self):
        assert _col_index(126.7) == 1
        assert _col_index(300.0) == 1

    def test_type_col(self):
        assert _col_index(387.1) == 2
        assert _col_index(500.0) == 2

    def test_msn_col(self):
        assert _col_index(566.8) == 3

    def test_owner_col(self):
        assert _col_index(648.1) == 4
        assert _col_index(900.0) == 4

    def test_date_col(self):
        assert _col_index(1015.1) == 5
        assert _col_index(2000.0) == 5


# ---------------------------------------------------------------------------
# Tests: _words_to_cols
# ---------------------------------------------------------------------------

class TestWordsToCols:
    def test_single_word_per_column(self):
        words = [
            _make_word("2-ABCD", 28.0),
            _make_word("Airbus", 127.0),
            _make_word("A320", 387.0),
            _make_word("1234", 567.0),
            _make_word("Owner", 648.0),
            _make_word("01/01/2020", 1015.0),
        ]
        cols = _words_to_cols(words)
        assert cols[0] == "2-ABCD"
        assert cols[1] == "Airbus"
        assert cols[2] == "A320"
        assert cols[3] == "1234"
        assert cols[4] == "Owner"
        assert cols[5] == "01/01/2020"

    def test_multi_word_manufacturer(self):
        words = [
            _make_word("2-ABCD", 28.0),
            _make_word("Eclipse", 127.0),
            _make_word("Aviation", 160.0),
            _make_word("Corporation", 210.0),
            _make_word("EA500", 387.0),
            _make_word("000267", 567.0),
            _make_word("TAK", 648.0),
            _make_word("Aviation", 675.0),
        ]
        cols = _words_to_cols(words)
        assert cols[0] == "2-ABCD"
        assert cols[1] == "Eclipse Aviation Corporation"
        assert cols[2] == "EA500"
        assert cols[3] == "000267"
        assert cols[4] == "TAK Aviation"

    def test_empty_column_is_empty_string(self):
        words = [_make_word("2-ABCD", 28.0)]
        cols = _words_to_cols(words)
        assert cols[0] == "2-ABCD"
        assert cols[1] == ""
        assert cols[4] == ""


# ---------------------------------------------------------------------------
# Tests: _find_pdf_url
# ---------------------------------------------------------------------------

class TestFindPdfUrl:
    def test_finds_absolute_href(self):
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.return_value = _make_response(text=html)
        url = _find_pdf_url(session)
        assert url == "https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf"

    def test_finds_relative_href_and_prepends_base(self):
        html = _make_index_page("/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.return_value = _make_response(text=html)
        url = _find_pdf_url(session)
        assert url == "https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf"

    def test_raises_when_no_pdf_link(self):
        session = MagicMock()
        session.get.return_value = _make_response(text="<html><body>No link</body></html>")
        with pytest.raises(RuntimeError, match="No register PDF link"):
            _find_pdf_url(session)

    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            _find_pdf_url(session)

    def test_logs_index_url(self):
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.return_value = _make_response(text=html)
        import logging
        with patch.object(logging.getLogger("gg-2reg"), "info") as mock_log:
            _find_pdf_url(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _INDEX_URL in logged


# ---------------------------------------------------------------------------
# Tests: download_and_parse (with mocked PDF)
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _mock_page(self, first_line: str, rows: list[list[dict]]) -> MagicMock:
        """Build a mock pdfplumber page with given first line and word rows."""
        page = MagicMock()
        page.extract_text.return_value = first_line + "\nsome content"
        all_words = []
        for top_idx, word_list in enumerate(rows):
            top = 80.0 + top_idx * 18
            for w in word_list:
                all_words.append({**w, "top": top})
        page.extract_words.return_value = all_words
        return page

    def _make_data_row_words(
        self,
        registration="2-ABCD",
        manufacturer="Airbus S.A.S.",
        model="A320-214",
        serial="1234",
        owner="Test Owner Ltd.",
        top=80.0,
    ) -> list[dict]:
        words = [{"text": registration, "x0": 28.0, "top": top}]
        for i, part in enumerate(manufacturer.split()):
            words.append({"text": part, "x0": 127.0 + i * 40, "top": top})
        for i, part in enumerate(model.split()):
            words.append({"text": part, "x0": 387.0 + i * 30, "top": top})
        words.append({"text": serial, "x0": 567.0, "top": top})
        for i, part in enumerate(owner.split()):
            words.append({"text": part, "x0": 648.0 + i * 40, "top": top})
        return words

    def test_raises_on_pdf_download_error(self):
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [
            _make_response(text=html),
            _make_response(status_code=503),
        ]
        with pytest.raises(RuntimeError, match="HTTP 503"):
            download_and_parse(session)

    def test_logs_pdf_url(self):
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [
            _make_response(text=html),
            _make_response(status_code=503),
        ]
        import logging
        with patch.object(logging.getLogger("gg-2reg"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert "https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf" in logged

    def test_skips_special_section_pages(self):
        for prefix in _SKIP_PREFIXES:
            words = self._make_data_row_words()
            page = self._mock_page(prefix + "JUN 2026", [words])
            html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
            session = MagicMock()
            pdf_bytes = b"%PDF fake"
            session.get.side_effect = [
                _make_response(text=html),
                _make_response(content=pdf_bytes),
            ]
            with patch("gg_2reg_main.pdfplumber.open") as mock_open:
                mock_pdf = MagicMock()
                mock_pdf.__enter__ = lambda s: mock_pdf
                mock_pdf.__exit__ = MagicMock(return_value=False)
                mock_pdf.pages = [page]
                mock_open.return_value = mock_pdf
                records = download_and_parse(session)
            assert records == [], f"Expected empty for prefix: {prefix!r}"

    def test_parses_main_register_page(self):
        words = self._make_data_row_words()
        page = self._mock_page("Registration Aircraft Manufacturer Type MSN Registered Owner", [words])
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [
            _make_response(text=html),
            _make_response(content=b"%PDF fake"),
        ]
        with patch("gg_2reg_main.pdfplumber.open") as mock_open:
            mock_pdf = MagicMock()
            mock_pdf.__enter__ = lambda s: mock_pdf
            mock_pdf.__exit__ = MagicMock(return_value=False)
            mock_pdf.pages = [page]
            mock_open.return_value = mock_pdf
            records = download_and_parse(session)
        assert len(records) == 1
        assert records[0]["registration"] == "2-ABCD"
        assert "Airbus" in records[0]["manufacturer"]
        assert records[0]["model"] == "A320-214"
        assert records[0]["serial"] == "1234"

    def test_skips_non_2prefix_rows(self):
        words = self._make_data_row_words(registration="G-ABCD")
        page = self._mock_page("Registration Aircraft Manufacturer", [words])
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [
            _make_response(text=html),
            _make_response(content=b"%PDF fake"),
        ]
        with patch("gg_2reg_main.pdfplumber.open") as mock_open:
            mock_pdf = MagicMock()
            mock_pdf.__enter__ = lambda s: mock_pdf
            mock_pdf.__exit__ = MagicMock(return_value=False)
            mock_pdf.pages = [page]
            mock_open.return_value = mock_pdf
            records = download_and_parse(session)
        assert records == []


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4CA123", "2-ABCD")
        assert record["icao_hex"] == "4CA123"
        assert record["registration"] == "2-ABCD"
        assert record["source"] == "gg-2reg"
        assert record["military"] is False
        assert record["aircraft"]["manufacturer"] == "Airbus S.A.S."
        assert record["aircraft"]["model"] == "A320-214"
        assert record["aircraft"]["serial_number"] == "1234"
        assert record["registrant"]["names"] == ["Test Owner Ltd."]

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_owner_omits_registrant(self):
        row = _make_row(owner="")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "registrant" not in record

    def test_private_owner_omitted(self):
        row = _make_row(owner="(private)")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "registrant" not in record

    def test_private_owner_omitted_case_insensitive(self):
        row = _make_row(owner="(PRIVATE)")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "registrant" not in record

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(manufacturer="", model="", serial="")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert "aircraft" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("2-ABCD")

    def test_plain_value_unchanged(self):
        assert _escape_tag("ABCD") == "ABCD"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_record_not_written_when_not_found(self):
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
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4CA123"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "gg-2reg"

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4CA123", REDIS_TTL)

    def test_empty_list_returns_zero(self):
        count = write_to_redis([], _make_redis_no_match(), REDIS_TTL)
        assert count == 0

    def test_null_fields_omitted_from_written_record(self):
        rows = [_make_row(manufacturer="")]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert "manufacturer" not in set_call[0][2]["aircraft"]

    def test_multiple_records(self):
        rows = [_make_row(registration="2-ABCD"), _make_row(registration="2-EFGH")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:mictronics:4CA123"
        doc_a.registration = "2-ABCD"
        doc_b = MagicMock()
        doc_b.id = "aircraft:mictronics:4CA456"
        doc_b.registration = "2-EFGH"
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
        mc = MagicMock()

        def fake_connect(host, port, keepalive):
            mc.on_connect(mc, None, None, 0, None)

        mc.connect.side_effect = fake_connect
        return mc

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("gg_2reg_main.mqtt.Client") as mock_cls:
            mock_cls.return_value = MagicMock()
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "42"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/gg-2reg"
