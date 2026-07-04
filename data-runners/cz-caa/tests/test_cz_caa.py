"""Tests for the Czech CAA data runner."""

from __future__ import annotations

import importlib.util
import os
import sys
from unittest.mock import MagicMock, call, patch

import pytest
import requests

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
        "cz_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["cz_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_fetch_active_ids = _mod._fetch_active_ids
_fetch_detail = _mod._fetch_detail
_first_display_name = _mod._first_display_name
_all_display_names = _mod._all_display_names
_build_record = _mod._build_record
_CATEGORY_MAP = _mod._CATEGORY_MAP
_ENGINE_TYPE_MAP = _mod._ENGINE_TYPE_MAP
download_and_parse = _mod.download_and_parse
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
_LIST_URL = _mod._LIST_URL
_DETAIL_URL = _mod._DETAIL_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_response(json_data=None, status_code=200):
    resp = MagicMock()
    resp.ok = status_code < 400
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


def _list_response(records: list[dict]):
    return _make_response({"rows": records, "total": len(records)})


def _list_record(id=1, deletion_date=None):
    return {"id": id, "deletion_date": deletion_date}


def _detail_response(
    transponder="49B0AA",
    registration_number="NHE",
    manufacturer="Airbus",
    model="A320",
    serial_number="1234",
    manufacture_year=2005,
    category="AVREG_DATA.CATEGORIES.AIRPLANE",
    engine_type="AVREG_DATA.ENGINE_TYPES.PISTON",
    engine_count=2,
    max_on_board=6,
    owners=None,
    operators=None,
):
    return _make_response({
        "transponder": transponder,
        "registration_number": registration_number,
        "manufacturer": manufacturer,
        "model": model,
        "serial_number": serial_number,
        "manufacture_year": manufacture_year,
        "category": category,
        "engine_type": engine_type,
        "engine_count": engine_count,
        "max_on_board": max_on_board,
        "owners": owners if owners is not None else [{"display_name": "Owner Co"}],
        "operators": operators if operators is not None else [{"display_name": "Operator Co"}],
    })


def _make_row(
    icao_hex="49B0AA",
    registration="OK-NHE",
    category="AVREG_DATA.CATEGORIES.AIRPLANE",
    manufacturer="Airbus",
    model="A320",
    serial="1234",
    manufacture_year=2005,
    engine_type="AVREG_DATA.ENGINE_TYPES.PISTON",
    engine_count=2,
    seats=6,
    owners=None,
) -> dict:
    return {
        "icao_hex": icao_hex,
        "registration": registration,
        "category": category,
        "manufacturer": manufacturer,
        "model": model,
        "serial": serial,
        "manufacture_year": manufacture_year,
        "engine_type": engine_type,
        "engine_count": engine_count,
        "seats": seats,
        "owners": owners if owners is not None else ["Owner Co"],
    }


def _make_redis():
    r = MagicMock()
    return r


# ---------------------------------------------------------------------------
# Tests: _fetch_active_ids
# ---------------------------------------------------------------------------

class TestFetchActiveIds:
    def test_filters_deleted_records(self):
        session = MagicMock()
        session.get.return_value = _list_response([
            _list_record(id=1, deletion_date=None),
            _list_record(id=2, deletion_date="2024-01-01"),
            _list_record(id=3, deletion_date=None),
        ])
        ids = _fetch_active_ids(session)
        assert ids == [1, 3]

    def test_raises_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=503)
        with pytest.raises(RuntimeError, match="HTTP 503"):
            _fetch_active_ids(session)

    def test_logs_list_url(self):
        session = MagicMock()
        session.get.return_value = _list_response([])
        import logging
        with patch.object(logging.getLogger("cz-caa"), "info") as mock_log:
            _fetch_active_ids(session)
        logged = " ".join(str(a) for c in mock_log.call_args_list for a in c.args)
        assert _LIST_URL in logged

    def test_empty_list_returns_empty(self):
        session = MagicMock()
        session.get.return_value = _list_response([])
        assert _fetch_active_ids(session) == []

    def test_handles_rows_key(self):
        """API returns {rows: [...], total: N}."""
        session = MagicMock()
        session.get.return_value = _make_response({
            "rows": [_list_record(id=1), _list_record(id=2)],
            "total": 2,
        })
        ids = _fetch_active_ids(session)
        assert ids == [1, 2]

    def test_all_deleted_returns_empty(self):
        session = MagicMock()
        session.get.return_value = _list_response([
            _list_record(id=1, deletion_date="2023-01-01"),
        ])
        assert _fetch_active_ids(session) == []


# ---------------------------------------------------------------------------
# Tests: _fetch_detail
# ---------------------------------------------------------------------------

class TestFetchDetail:
    def test_returns_json_on_success(self):
        session = MagicMock()
        session.get.return_value = _detail_response()
        result = _fetch_detail(session, 1)
        assert result["transponder"] == "49B0AA"

    def test_returns_none_on_http_error(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=404)
        assert _fetch_detail(session, 1) is None

    def test_retries_on_connection_error_then_succeeds(self):
        session = MagicMock()
        session.get.side_effect = [
            requests.exceptions.ConnectionError("dropped"),
            _detail_response(),
        ]
        with patch("cz_caa_main.time.sleep"):
            result = _fetch_detail(session, 1)
        assert result is not None
        assert result["transponder"] == "49B0AA"

    def test_returns_none_after_all_retries_exhausted(self):
        session = MagicMock()
        session.get.side_effect = requests.exceptions.ConnectionError("dropped")
        with patch("cz_caa_main.time.sleep"):
            assert _fetch_detail(session, 1) is None

    def test_retry_uses_exponential_backoff(self):
        session = MagicMock()
        session.get.side_effect = [
            requests.exceptions.ConnectionError("dropped"),
            requests.exceptions.ConnectionError("dropped"),
            _detail_response(),
        ]
        with patch("cz_caa_main.time.sleep") as mock_sleep:
            _fetch_detail(session, 1)
        assert mock_sleep.call_args_list[0] == call(1)
        assert mock_sleep.call_args_list[1] == call(2)

    def test_logs_detail_url(self):
        session = MagicMock()
        session.get.return_value = _detail_response()
        import logging
        with patch.object(logging.getLogger("cz-caa"), "debug") as mock_log:
            _fetch_detail(session, 42)
        logged = " ".join(str(a) for c in mock_log.call_args_list for a in c.args)
        assert "42" in logged

    def test_uses_correct_url(self):
        session = MagicMock()
        session.get.return_value = _detail_response()
        _fetch_detail(session, 99)
        session.get.assert_called_once_with(
            _DETAIL_URL.format(id=99), timeout=30
        )

    def test_retries_on_403(self):
        session = MagicMock()
        session.get.side_effect = [_make_response(status_code=403), _detail_response()]
        with patch("cz_caa_main.time.sleep"):
            result = _fetch_detail(session, 1)
        assert result is not None
        assert session.get.call_count == 2

    def test_retries_on_429(self):
        session = MagicMock()
        session.get.side_effect = [_make_response(status_code=429), _detail_response()]
        with patch("cz_caa_main.time.sleep"):
            result = _fetch_detail(session, 1)
        assert result is not None
        assert session.get.call_count == 2

    def test_retries_on_503(self):
        session = MagicMock()
        session.get.side_effect = [_make_response(status_code=503), _detail_response()]
        with patch("cz_caa_main.time.sleep"):
            result = _fetch_detail(session, 1)
        assert result is not None
        assert session.get.call_count == 2

    def test_returns_none_after_all_http_retries_exhausted(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=403)
        with patch("cz_caa_main.time.sleep"):
            assert _fetch_detail(session, 1) is None
        assert session.get.call_count == 4  # initial + 3 retries

    def test_non_retriable_http_not_retried(self):
        session = MagicMock()
        session.get.return_value = _make_response(status_code=404)
        assert _fetch_detail(session, 1) is None
        assert session.get.call_count == 1

    def test_retriable_http_uses_exponential_backoff(self):
        session = MagicMock()
        session.get.side_effect = [
            _make_response(status_code=429),
            _make_response(status_code=429),
            _detail_response(),
        ]
        with patch("cz_caa_main.time.sleep") as mock_sleep:
            _fetch_detail(session, 1)
        assert mock_sleep.call_args_list[0] == call(1)
        assert mock_sleep.call_args_list[1] == call(2)


# ---------------------------------------------------------------------------
# Tests: _first_display_name
# ---------------------------------------------------------------------------

class TestFirstDisplayName:
    def test_returns_first_entry(self):
        assert _first_display_name([{"display_name": "Alice"}, {"display_name": "Bob"}]) == "Alice"

    def test_returns_empty_for_none(self):
        assert _first_display_name(None) == ""

    def test_returns_empty_for_empty_list(self):
        assert _first_display_name([]) == ""

    def test_normalizes_whitespace(self):
        assert _first_display_name([{"display_name": "  John  Doe  "}]) == "John Doe"

    def test_handles_missing_display_name(self):
        assert _first_display_name([{}]) == ""


class TestAllDisplayNames:
    def test_returns_all_names(self):
        entries = [{"display_name": "Alice"}, {"display_name": "Bob"}, {"display_name": "Carol"}]
        assert _all_display_names(entries) == ["Alice", "Bob", "Carol"]

    def test_returns_empty_for_none(self):
        assert _all_display_names(None) == []

    def test_returns_empty_for_empty_list(self):
        assert _all_display_names([]) == []

    def test_skips_empty_display_names(self):
        entries = [{"display_name": "Alice"}, {"display_name": ""}, {"display_name": "Carol"}]
        assert _all_display_names(entries) == ["Alice", "Carol"]

    def test_normalizes_whitespace(self):
        entries = [{"display_name": "  John  Doe  "}]
        assert _all_display_names(entries) == ["John Doe"]


# ---------------------------------------------------------------------------
# Tests: download_and_parse
# ---------------------------------------------------------------------------

class TestDownloadAndParse:
    def _session_with(self, list_records, detail_responses):
        session = MagicMock()
        calls = [_list_response(list_records)] + detail_responses
        session.get.side_effect = calls
        return session

    def _collect(self, session, delay=0):
        return list(download_and_parse(session, delay=delay))

    def test_basic_parse(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response()],
        )
        records = self._collect(session)
        assert len(records) == 1
        r = records[0]
        assert r["icao_hex"] == "49B0AA"
        assert r["category"] == "AVREG_DATA.CATEGORIES.AIRPLANE"
        assert r["manufacturer"] == "Airbus"
        assert r["model"] == "A320"
        assert r["serial"] == "1234"
        assert r["manufacture_year"] == 2005
        assert r["engine_type"] == "AVREG_DATA.ENGINE_TYPES.PISTON"
        assert r["engine_count"] == 2
        assert r["seats"] == 6
        assert r["owners"] == ["Owner Co"]
        assert "operator" not in r

    def test_registration_prefixed_with_ok(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(registration_number="NHE")],
        )
        records = self._collect(session)
        assert records[0]["registration"] == "OK-NHE"

    def test_numeric_registration_prefixed_with_ok(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(registration_number="1213")],
        )
        records = self._collect(session)
        assert records[0]["registration"] == "OK-1213"

    def test_empty_registration_stored_empty(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(registration_number=None)],
        )
        records = self._collect(session)
        assert records[0]["registration"] == ""

    def test_all_owners_collected(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(owners=[
                {"display_name": "Alice"},
                {"display_name": "Bob"},
                {"display_name": "Carol"},
            ])],
        )
        records = self._collect(session)
        assert records[0]["owners"] == ["Alice", "Bob", "Carol"]

    def test_skips_null_transponder(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(transponder=None)],
        )
        assert self._collect(session) == []

    def test_skips_empty_transponder(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(transponder="")],
        )
        assert self._collect(session) == []

    def test_transponder_uppercased(self):
        session = self._session_with(
            [_list_record(id=1)],
            [_detail_response(transponder="49b0aa")],
        )
        assert self._collect(session)[0]["icao_hex"] == "49B0AA"

    def test_skips_deleted_records(self):
        session = MagicMock()
        session.get.side_effect = [
            _list_response([
                _list_record(id=1, deletion_date=None),
                _list_record(id=2, deletion_date="2024-01-01"),
            ]),
            _detail_response(),
        ]
        assert len(self._collect(session)) == 1

    def test_continues_on_detail_error(self):
        session = MagicMock()
        session.get.side_effect = [
            _list_response([_list_record(id=1), _list_record(id=2)]),
            _make_response(status_code=404),
            _detail_response(transponder="AAAAAA"),
        ]
        records = self._collect(session)
        assert len(records) == 1
        assert records[0]["icao_hex"] == "AAAAAA"

    def test_rate_limiting_applied(self):
        session = MagicMock()
        session.get.side_effect = [
            _list_response([_list_record(id=1), _list_record(id=2)]),
            _detail_response(transponder="111111"),
            _detail_response(transponder="222222"),
        ]
        with patch("cz_caa_main.time.sleep") as mock_sleep:
            self._collect(session, delay=0.05)
        mock_sleep.assert_called_once_with(0.05)

    def test_no_sleep_before_first_record(self):
        session = MagicMock()
        session.get.side_effect = [
            _list_response([_list_record(id=1)]),
            _detail_response(),
        ]
        with patch("cz_caa_main.time.sleep") as mock_sleep:
            self._collect(session, delay=0.05)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row)
        assert record["icao_hex"] == "49B0AA"
        assert record["source"] == "cz-caa"
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Airbus"
        assert record["aircraft"]["model"] == "A320"
        assert record["aircraft"]["serial_number"] == "1234"
        assert record["aircraft"]["manufactured_date"] == "2005-01-01"
        assert record["aircraft"]["powerplant"] == {"type": "Piston", "count": 2}
        assert record["aircraft"]["seats"] == 6
        assert record["registrant"]["names"] == ["Owner Co"]

    def test_all_owners_in_names(self):
        row = _make_row(owners=["Alice", "Bob", "Carol"])
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Alice", "Bob", "Carol"]

    def test_no_registrant_when_no_owners(self):
        row = _make_row(owners=[])
        record = _build_record(row)
        assert "registrant" not in record

    def test_category_mapped(self):
        for key, expected in _CATEGORY_MAP.items():
            row = _make_row(category=key)
            assert _build_record(row)["aircraft"]["type"] == expected

    def test_unknown_category_omitted(self):
        row = _make_row(category="UNKNOWN_VALUE")
        record = _build_record(row)
        assert "type" not in record.get("aircraft", {})

    def test_none_category_omitted(self):
        row = _make_row(category=None)
        record = _build_record(row)
        assert "type" not in record.get("aircraft", {})

    def test_engine_type_mapped(self):
        for key, expected in _ENGINE_TYPE_MAP.items():
            row = _make_row(engine_type=key)
            assert _build_record(row)["aircraft"]["powerplant"]["type"] == expected

    def test_no_engine_type_omitted(self):
        row = _make_row(engine_type="AVREG_DATA.ENGINE_TYPES.NO_ENGINE", engine_count=0)
        record = _build_record(row)
        assert "powerplant" not in record.get("aircraft", {})

    def test_engine_count_zero_omitted(self):
        row = _make_row(engine_type=None, engine_count=0)
        record = _build_record(row)
        assert "powerplant" not in record.get("aircraft", {})

    def test_engine_count_without_type(self):
        row = _make_row(engine_type=None, engine_count=2)
        record = _build_record(row)
        assert record["aircraft"]["powerplant"] == {"count": 2}

    def test_seats_stored(self):
        row = _make_row(seats=4)
        record = _build_record(row)
        assert record["aircraft"]["seats"] == 4

    def test_seats_zero_omitted(self):
        row = _make_row(seats=0)
        record = _build_record(row)
        assert "seats" not in record.get("aircraft", {})

    def test_seats_none_omitted(self):
        row = _make_row(seats=None)
        record = _build_record(row)
        assert "seats" not in record.get("aircraft", {})

    def test_year_stored_as_date(self):
        row = _make_row(manufacture_year=1998)
        record = _build_record(row)
        assert record["aircraft"]["manufactured_date"] == "1998-01-01"

    def test_invalid_year_omitted(self):
        row = _make_row(manufacture_year=1800)
        record = _build_record(row)
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_none_year_omitted(self):
        row = _make_row(manufacture_year=None)
        record = _build_record(row)
        assert "manufactured_date" not in record.get("aircraft", {})

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row)
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_model_omitted(self):
        row = _make_row(model="")
        record = _build_record(row)
        assert "model" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row)
        assert "serial_number" not in record.get("aircraft", {})

    def test_registration_stored(self):
        row = _make_row(registration="OK-NHE")
        record = _build_record(row)
        assert record["registration"] == "OK-NHE"

    def test_empty_registration_omitted(self):
        row = _make_row(registration="")
        record = _build_record(row)
        assert "registration" not in record


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_writes_record(self):
        r = _make_redis()
        assert write_to_redis(_make_row(), r, REDIS_TTL) is True

    def test_writes_to_correct_key(self):
        r = _make_redis()
        write_to_redis(_make_row(icao_hex="49B0AA"), r, REDIS_TTL)
        key = r.json.return_value.set.call_args[0][0]
        assert key == "aircraft:registry:49B0AA"

    def test_source_field_in_record(self):
        r = _make_redis()
        write_to_redis(_make_row(), r, REDIS_TTL)
        written = r.json.return_value.set.call_args[0][2]
        assert written["source"] == "cz-caa"

    def test_ttl_applied(self):
        r = _make_redis()
        write_to_redis(_make_row(icao_hex="49B0AA"), r, REDIS_TTL)
        r.expire.assert_called_with("aircraft:registry:49B0AA", REDIS_TTL)

    def test_empty_icao_hex_returns_false(self):
        r = _make_redis()
        assert write_to_redis(_make_row(icao_hex=""), r, REDIS_TTL) is False

    def test_redis_error_returns_false(self):
        r = _make_redis()
        r.json.return_value.set.side_effect = Exception("connection refused")
        assert write_to_redis(_make_row(), r, REDIS_TTL) is False


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
        with patch("cz_caa_main.mqtt.Client") as mock_cls:
            mock_cls.return_value = MagicMock()
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("cz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 3486, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("cz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 3486, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "3486"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("cz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/cz-caa"
