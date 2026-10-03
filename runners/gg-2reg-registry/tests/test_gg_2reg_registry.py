"""Tests for the Guernsey 2-reg data runner."""

from __future__ import annotations

import importlib.util
import io
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "gg_2reg_registry_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["gg_2reg_registry_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()


_col_index = _mod._col_index
_find_column_thresholds = _mod._find_column_thresholds
_words_to_cols = _mod._words_to_cols
_find_pdf_url = _mod._find_pdf_url
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.ENRICHMENT_TTL_SECONDS
MQTT_ROOT = _mod.MQTT_ROOT
_INDEX_URL = _mod._INDEX_URL
_SKIP_PREFIXES = _mod._SKIP_PREFIXES


# Column boundaries for the current register PDF geometry (header x0 minus margin)
THRESHOLDS = (79.0, 242.0, 364.0, 423.0, 670.0)


def _header_words(top: float = 40.0) -> list[dict]:
    spec = [
        ("Registration", 19), ("Aircraft", 81), ("Manufacturer", 111), ("Type", 244),
        ("MSN", 366), ("Registered", 425), ("Owner/", 467), ("Charterer", 498),
        ("by", 536), ("demise", 547), ("Date", 672), ("of", 692), ("registration", 701),
    ]
    return [{"text": t, "x0": float(x), "top": top} for t, x in spec]


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


class TestFindColumnThresholds:
    def test_derived_from_header_row(self):
        assert _find_column_thresholds(_header_words()) == THRESHOLDS

    def test_header_position_independent_of_page_layout(self):
        shifted = [{**w, "x0": w["x0"] + 100, "top": 96.0} for w in _header_words()]
        assert _find_column_thresholds(shifted) == tuple(t + 100 for t in THRESHOLDS)

    def test_title_line_alone_is_not_a_header(self):
        words = [_make_word("Aircraft", 19.0, 42.0), _make_word("register", 63.0, 42.0)]
        assert _find_column_thresholds(words) is None

    def test_no_header_returns_none(self):
        assert _find_column_thresholds([_make_word("2-ABCD", 19.0)]) is None

    def test_header_words_split_across_lines_not_matched(self):
        words = _header_words()
        words[10]["top"] = 60.0
        assert _find_column_thresholds(words) is None


class TestColIndex:
    def test_registration_col(self):
        assert _col_index(19.0, THRESHOLDS) == 0
        assert _col_index(78.9, THRESHOLDS) == 0

    def test_manufacturer_col(self):
        assert _col_index(81.0, THRESHOLDS) == 1
        assert _col_index(210.0, THRESHOLDS) == 1

    def test_type_col(self):
        assert _col_index(244.0, THRESHOLDS) == 2
        assert _col_index(322.0, THRESHOLDS) == 2

    def test_msn_col(self):
        assert _col_index(366.0, THRESHOLDS) == 3

    def test_owner_col(self):
        assert _col_index(425.0, THRESHOLDS) == 4
        assert _col_index(564.0, THRESHOLDS) == 4

    def test_date_col(self):
        assert _col_index(672.0, THRESHOLDS) == 5


class TestWordsToCols:
    def test_single_word_per_column(self):
        words = [
            _make_word("2-ABCD", 19.0),
            _make_word("The", 81.0),
            _make_word("737-8", 244.0),
            _make_word("43317", 366.0),
            _make_word("AerFin", 425.0),
            _make_word("04/09/2026", 672.0),
        ]
        cols = _words_to_cols(words, THRESHOLDS)
        assert cols == ["2-ABCD", "The", "737-8", "43317", "AerFin", "04/09/2026"]

    def test_wrapped_multi_word_cells_stay_in_column(self):
        words = [
            _make_word("2-CHOP", 19.0),
            _make_word("Costruzioni", 81.0),
            _make_word("Aeronautiche", 124.0),
            _make_word("Giovanni", 175.0),
            _make_word("Agusta", 210.0),
            _make_word("CL-600-2B16", 244.0),
            _make_word("(CL-604", 293.0),
            _make_word("Variant)", 322.0),
            _make_word("8185", 366.0),
            _make_word("A", 425.0),
            _make_word("T", 432.0),
            _make_word("Aviation", 438.0),
        ]
        cols = _words_to_cols(words, THRESHOLDS)
        assert cols[0] == "2-CHOP"
        assert cols[1] == "Costruzioni Aeronautiche Giovanni Agusta"
        assert cols[2] == "CL-600-2B16 (CL-604 Variant)"
        assert cols[3] == "8185"
        assert cols[4] == "A T Aviation"

    def test_empty_column_is_empty_string(self):
        cols = _words_to_cols([_make_word("2-ABCD", 19.0)], THRESHOLDS)
        assert cols[0] == "2-ABCD"
        assert cols[1] == ""
        assert cols[4] == ""


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
        with patch.object(logging.getLogger("gg-2reg-registry"), "info") as mock_log:
            _find_pdf_url(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _INDEX_URL in logged


class TestDownloadAndParse:
    def _mock_page(self, first_line: str, rows: list[list[dict]], header: float | None = 40.0) -> MagicMock:
        """Build a mock pdfplumber page with given first line and word rows."""
        page = MagicMock()
        page.extract_text.return_value = first_line + "\nsome content"
        all_words = [] if header is None else [{**w, "top": header} for w in _header_words()]
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
        words = [{"text": registration, "x0": 19.0, "top": top}]
        for i, part in enumerate(manufacturer.split()):
            words.append({"text": part, "x0": 81.0 + i * 40, "top": top})
        for i, part in enumerate(model.split()):
            words.append({"text": part, "x0": 244.0 + i * 30, "top": top})
        words.append({"text": serial, "x0": 366.0, "top": top})
        for i, part in enumerate(owner.split()):
            words.append({"text": part, "x0": 425.0 + i * 40, "top": top})
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
        with patch.object(logging.getLogger("gg-2reg-registry"), "info") as mock_log:
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
            with patch("gg_2reg_registry_main.pdfplumber.open") as mock_open:
                mock_pdf = MagicMock()
                mock_pdf.__enter__ = lambda s: mock_pdf
                mock_pdf.__exit__ = MagicMock(return_value=False)
                mock_pdf.pages = [page]
                mock_open.return_value = mock_pdf
                with pytest.raises(RuntimeError, match="No 2-prefix records"):
                    download_and_parse(session)

    def test_parses_main_register_page(self):
        words = self._make_data_row_words()
        page = self._mock_page("Registration Aircraft Manufacturer Type MSN Registered Owner", [words])
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [
            _make_response(text=html),
            _make_response(content=b"%PDF fake"),
        ]
        with patch("gg_2reg_registry_main.pdfplumber.open") as mock_open:
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
        with patch("gg_2reg_registry_main.pdfplumber.open") as mock_open:
            mock_pdf = MagicMock()
            mock_pdf.__enter__ = lambda s: mock_pdf
            mock_pdf.__exit__ = MagicMock(return_value=False)
            mock_pdf.pages = [page]
            mock_open.return_value = mock_pdf
            with pytest.raises(RuntimeError, match="No 2-prefix records"):
                download_and_parse(session)


    def _parse(self, pages):
        html = _make_index_page("https://www.2-reg.com/wp-content/uploads/2026/07/Register_20260701.pdf")
        session = MagicMock()
        session.get.side_effect = [_make_response(text=html), _make_response(content=b"%PDF fake")]
        with patch("gg_2reg_registry_main.pdfplumber.open") as mock_open:
            mock_pdf = MagicMock()
            mock_pdf.__enter__ = lambda s: mock_pdf
            mock_pdf.__exit__ = MagicMock(return_value=False)
            mock_pdf.pages = pages
            mock_open.return_value = mock_pdf
            return download_and_parse(session)

    def test_registration_is_bare_mark_and_fields_land_in_columns(self):
        words = self._make_data_row_words(
            registration="2-AACC", manufacturer="The Boeing Company",
            model="737-8", serial="43317", owner="AerFin Limited",
        )
        records = self._parse([self._mock_page("Aircraft register", [words])])
        assert records == [{
            "registration": "2-AACC",
            "manufacturer": "The Boeing Company",
            "model": "737-8",
            "serial": "43317",
            "owner": "AerFin Limited",
        }]

    def test_page_without_header_is_skipped_not_parsed_with_stale_boundaries(self):
        good = self._mock_page("Aircraft register", [self._make_data_row_words()])
        headerless = self._mock_page("Aircraft register", [self._make_data_row_words(registration="2-WXYZ")], header=None)
        records = self._parse([good, headerless])
        assert [r["registration"] for r in records] == ["2-ABCD"]

    def test_zero_records_parsed_raises(self):
        headerless = self._mock_page("Aircraft register", [self._make_data_row_words()], header=None)
        with pytest.raises(RuntimeError, match="No 2-prefix records"):
            self._parse([headerless])


class TestMainFailure:
    def _run(self, rows_or_exc, write_result):
        cfg = {"redis": {}, "mqtt": {}}
        with patch.object(_mod, "load_config", return_value=cfg), \
             patch.object(_mod, "configure_logging"), \
             patch.object(_mod, "build_redis_client", return_value=MagicMock()), \
             patch.object(_mod, "_ensure_search_index"), \
             patch.object(_mod, "download_and_parse", return_value=rows_or_exc), \
             patch.object(_mod, "write_to_redis", side_effect=write_result), \
             patch.object(_mod, "publish_completion_stats") as pub:
            with pytest.raises(SystemExit) as exc:
                _mod.main()
        return exc.value.code, pub

    def test_zero_match_publishes_failure_and_exits_nonzero(self):
        code, pub = self._run([_make_row()], RuntimeError("none matched"))
        assert code == 1
        assert pub.call_args.args[2] == "failure"


class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "4CA123", "2-ABCD")
        assert record["icao_hex"] == "4CA123"
        assert record["registration"] == "2-ABCD"
        assert record["source"] == "gg-2reg-registry"
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

    def test_serial_newlines_collapsed(self):
        row = _make_row(serial="12\n34")
        record = _build_record(row, "4CA123", "2-ABCD")
        assert record["aircraft"]["serial_number"] == "12 34"

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


class TestEscapeTag:
    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("2-ABCD")

    def test_plain_value_unchanged(self):
        assert _escape_tag("ABCD") == "ABCD"


class TestWriteToRedis:
    def test_record_written_when_found(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_zero_matches_raises(self):
        rows = [_make_row()]
        r = _make_redis_no_match()
        with pytest.raises(RuntimeError, match="matched the Mictronics index"):
            write_to_redis(rows, r, REDIS_TTL)

    def test_partial_match_not_written_for_unmatched(self):
        rows = [_make_row(), _make_row(registration="2-ZZZZ")]
        r = _make_redis_with_search(icao_hex="4CA123", registration="2-ABCD")
        assert write_to_redis(rows, r, REDIS_TTL) == 1

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
        assert set_call[0][2]["source"] == "gg-2reg-registry"

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


class TestPublishCompletionStats:
    def _setup_mock_client(self):
        mc = MagicMock()

        def fake_connect(host, port, keepalive):
            mc.on_connect(mc, None, None, 0, None)

        mc.connect.side_effect = fake_connect
        return mc

    def test_no_mqtt_config_skips(self):
        publish_completion_stats({}, 100, "success")

    def test_blank_host_skips_without_crashing(self):
        """Regression test: shared/config.py's mqtt_config() always returns a
        populated dict with host="" (never None/{}) when MQTT_HOST is unset
        -- the documented way to disable MQTT entirely. A guard that only
        checks `if not mc` doesn't catch this, since the dict itself is
        truthy; it then calls build_mqtt_client() (which correctly returns
        None for a blank host) and crashes assigning .on_connect on None.
        That crash gets silently swallowed by main()'s outer try/except, so
        the runner "succeeds" but MQTT stats never publish and a bogus
        warning gets logged every run. Must not raise."""
        cfg = {"mqtt": {"host": "", "port": 1883, "username": "", "password": ""}}
        publish_completion_stats(cfg, 100, "success")

    def test_mqtt_connect_timeout_does_not_raise(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        with patch("gg_2reg_registry_main.mqtt.Client") as mock_cls:
            mock_cls.return_value = MagicMock()
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "42"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("gg_2reg_registry_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "Failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/gg-2reg-registry"
