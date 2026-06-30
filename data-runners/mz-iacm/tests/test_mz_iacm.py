"""Tests for the Mozambique IACM data runner."""

from __future__ import annotations

import importlib.util
import json
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
        "mz_iacm_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["mz_iacm_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_normalize_registration = _mod._normalize_registration
_group_into_rows = _mod._group_into_rows
_split_by_gap = _mod._split_by_gap
_parse_row = _mod._parse_row
_extract_records_from_ocr_data = _mod._extract_records_from_ocr_data
_build_record = _mod._build_record
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_ROW_TOLERANCE = _mod._ROW_TOLERANCE
_COL_GAP = _mod._COL_GAP


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _word(text, left, top, right=None, width=20):
    return {
        "text": text,
        "left": left,
        "top": top,
        "right": right if right is not None else left + width,
    }


def _make_row(
    registration="C9-AAA",
    field_groups=None,
) -> dict:
    return {
        "registration": registration,
        "field_groups": field_groups if field_groups is not None else ["Cessna", "172S", "17281234", "Mozambique Airways"],
    }


def _make_redis_with_search(icao_hex="C8C001", registration="C9-AAA"):
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
# Tests: _normalize_registration
# ---------------------------------------------------------------------------

class TestNormalizeRegistration:
    def test_strips_space(self):
        assert _normalize_registration("C9 AAA") == "C9AAA"

    def test_uppercase(self):
        assert _normalize_registration("c9-aaa") == "C9-AAA"

    def test_already_normalized(self):
        assert _normalize_registration("C9-AAA") == "C9-AAA"


# ---------------------------------------------------------------------------
# Tests: _group_into_rows
# ---------------------------------------------------------------------------

class TestGroupIntoRows:
    def test_single_row(self):
        words = [_word("C9-AAA", 10, 100), _word("Cessna", 100, 102)]
        rows = _group_into_rows(words)
        assert len(rows) == 1
        assert len(rows[0]) == 2

    def test_two_separate_rows(self):
        words = [
            _word("C9-AAA", 10, 100),
            _word("C9-BBB", 10, 200),
        ]
        rows = _group_into_rows(words)
        assert len(rows) == 2

    def test_within_tolerance_grouped(self):
        words = [
            _word("C9-AAA", 10, 100),
            _word("Cessna", 100, 100 + _ROW_TOLERANCE - 1),
        ]
        rows = _group_into_rows(words)
        assert len(rows) == 1

    def test_at_tolerance_boundary_grouped(self):
        words = [
            _word("C9-AAA", 10, 100),
            _word("Cessna", 100, 100 + _ROW_TOLERANCE),
        ]
        rows = _group_into_rows(words)
        assert len(rows) == 1

    def test_beyond_tolerance_split(self):
        words = [
            _word("C9-AAA", 10, 100),
            _word("Cessna", 100, 100 + _ROW_TOLERANCE + 1),
        ]
        rows = _group_into_rows(words)
        assert len(rows) == 2

    def test_words_sorted_by_x_within_row(self):
        words = [_word("Cessna", 200, 100), _word("C9-AAA", 10, 100)]
        rows = _group_into_rows(words)
        assert rows[0][0]["text"] == "C9-AAA"
        assert rows[0][1]["text"] == "Cessna"

    def test_rows_sorted_by_y(self):
        words = [_word("C9-BBB", 10, 200), _word("C9-AAA", 10, 100)]
        rows = _group_into_rows(words)
        assert rows[0][0]["text"] == "C9-AAA"
        assert rows[1][0]["text"] == "C9-BBB"


# ---------------------------------------------------------------------------
# Tests: _split_by_gap
# ---------------------------------------------------------------------------

class TestSplitByGap:
    def test_single_word(self):
        words = [_word("Cessna", 10, 100, right=60)]
        groups = _split_by_gap(words)
        assert len(groups) == 1

    def test_small_gap_no_split(self):
        words = [
            _word("Cessna", 10, 100, right=60),
            _word("172S", 65, 100, right=90),
        ]
        groups = _split_by_gap(words)
        assert len(groups) == 1

    def test_large_gap_splits(self):
        words = [
            _word("Cessna", 10, 100, right=60),
            _word("172S", 60 + _COL_GAP, 100, right=60 + _COL_GAP + 30),
        ]
        groups = _split_by_gap(words)
        assert len(groups) == 2

    def test_multiple_columns(self):
        base = 0
        words = []
        for field in ["Cessna", "172S", "12345", "Airways"]:
            words.append(_word(field, base, 100, right=base + 50))
            base += 50 + _COL_GAP
        groups = _split_by_gap(words)
        assert len(groups) == 4

    def test_empty_input(self):
        assert _split_by_gap([]) == []


# ---------------------------------------------------------------------------
# Tests: _parse_row
# ---------------------------------------------------------------------------

class TestParseRow:
    def test_detects_c9_registration(self):
        row = [
            _word("C9-AAA", 10, 100, right=60),
            _word("Cessna", 60 + _COL_GAP, 100, right=120 + _COL_GAP),
        ]
        result = _parse_row(row)
        assert result is not None
        assert result["registration"] == "C9-AAA"

    def test_returns_none_for_header_row(self):
        row = [_word("Matricula", 10, 100), _word("Fabricante", 100, 100)]
        assert _parse_row(row) is None

    def test_field_groups_captured(self):
        row = [
            _word("C9-AAA", 10, 100, right=60),
            _word("Cessna", 110, 100, right=170),
            _word("172S", 220, 100, right=260),
        ]
        result = _parse_row(row)
        assert result is not None
        assert len(result["field_groups"]) >= 1

    def test_leading_row_number_ignored(self):
        row = [
            _word("1", 0, 100, right=15),
            _word("C9-AAA", 20, 100, right=80),
            _word("Cessna", 130, 100, right=190),
        ]
        result = _parse_row(row)
        assert result is not None
        assert result["registration"] == "C9-AAA"
        # "1" is to the left of C9 — not in field_groups
        assert all("1" != g for g in result["field_groups"])


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row, "C8C001", "C9-AAA")
        assert record["icao_hex"] == "C8C001"
        assert record["registration"] == "C9-AAA"
        assert record["source"] == "mz-iacm"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "17281234"
        assert record["registrant"]["names"] == ["Mozambique Airways"]

    def test_empty_manufacturer_omitted(self):
        row = _make_row(field_groups=["", "172S", "12345", "Owner"])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(field_groups=["Cessna", "", "12345", "Owner"])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(field_groups=["Cessna", "172S", "", "Owner"])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert "serial_number" not in record.get("aircraft", {})

    def test_empty_owner_omits_registrant(self):
        row = _make_row(field_groups=["Cessna", "172S", "12345", ""])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert "registrant" not in record

    def test_fewer_than_four_groups(self):
        row = _make_row(field_groups=["Cessna", "172S"])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert "serial_number" not in record.get("aircraft", {})
        assert "registrant" not in record

    def test_no_groups_omits_aircraft_and_registrant(self):
        row = _make_row(field_groups=[])
        record = _build_record(row, "C8C001", "C9-AAA")
        assert "aircraft" not in record
        assert "registrant" not in record


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("C9AAA") == "C9AAA"

    def test_hyphen_escaped(self):
        assert "\\-" in _escape_tag("C9-AAA")

    def test_empty_string(self):
        assert _escape_tag("") == ""


# ---------------------------------------------------------------------------
# Tests: download_and_parse (index scraping)
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def test_raises_on_index_error(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 404
        session.get.return_value = resp
        from mz_iacm_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 404"):
            download_and_parse(session)

    def test_raises_when_no_pdf_link(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = "<html><body><p>No PDF here</p></body></html>"
        session.get.return_value = resp
        from mz_iacm_main import download_and_parse
        with pytest.raises(RuntimeError, match="No DIRECCAO-DE-SEGURANCA PDF"):
            download_and_parse(session)

    def test_raises_on_pdf_download_error(self):
        pdf_url = "https://www.iacm.gov.mz/app/uploads/2025/04/DIRECCAO-DE-SEGURANCA-DE-VOO1.pdf"
        session = MagicMock()
        index_resp = MagicMock()
        index_resp.ok = True
        index_resp.text = f'<html><body><a href="{pdf_url}">PDF</a></body></html>'
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 403
        session.get.side_effect = [index_resp, pdf_resp]
        from mz_iacm_main import download_and_parse
        with pytest.raises(RuntimeError, match="HTTP 403"):
            download_and_parse(session)

    def test_relative_pdf_url_resolved(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = True
        resp.text = '<html><body><a href="/app/uploads/DIRECCAO-DE-SEGURANCA-VOO.pdf">PDF</a></body></html>'
        pdf_resp = MagicMock()
        pdf_resp.ok = False
        pdf_resp.status_code = 404
        session.get.side_effect = [resp, pdf_resp]
        from mz_iacm_main import download_and_parse
        with pytest.raises(RuntimeError):
            download_and_parse(session)
        called_url = session.get.call_args_list[1][0][0]
        assert called_url.startswith("https://www.iacm.gov.mz")

    def test_logs_index_url(self):
        session = MagicMock()
        resp = MagicMock()
        resp.ok = False
        resp.status_code = 503
        session.get.return_value = resp
        import logging
        from mz_iacm_main import download_and_parse, _INDEX_URL
        with patch.object(logging.getLogger("mz-iacm"), "info") as mock_log:
            with pytest.raises(RuntimeError):
                download_and_parse(session)
        logged = " ".join(str(a) for call in mock_log.call_args_list for a in call.args)
        assert _INDEX_URL in logged


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written_when_found_in_redis(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="C8C001", registration="C9-AAA")
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
        r = _make_redis_with_search(icao_hex="C8C001", registration="C9-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:detail:C8C001"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="C8C001", registration="C9-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "mz-iacm"

    def test_empty_list_returns_zero(self):
        r = _make_redis_no_match()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_ttl_applied(self):
        rows = [_make_row()]
        r = _make_redis_with_search(icao_hex="C8C001", registration="C9-AAA")
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:detail:C8C001", REDIS_TTL)

    def test_multiple_records(self):
        rows = [_make_row(registration="C9-AAA"), _make_row(registration="C9-BBB")]
        r = MagicMock()
        doc_a = MagicMock()
        doc_a.id = "aircraft:simple:C8C001"
        doc_a.registration = "C9-AAA"
        doc_b = MagicMock()
        doc_b.id = "aircraft:simple:C8C002"
        doc_b.registration = "C9-BBB"
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
        with patch("mz_iacm_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mz_iacm_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mz_iacm_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "42"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("mz_iacm_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/mz-iacm"
