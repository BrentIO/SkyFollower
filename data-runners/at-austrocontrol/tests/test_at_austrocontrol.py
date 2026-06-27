"""Tests for the Austria Austrocontrol data runner."""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from unittest.mock import MagicMock, patch

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
        "at_austrocontrol_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["at_austrocontrol_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_country = _mod._decode_country
_parse_halter = _mod._parse_halter
_build_record = _mod._build_record
_deep_merge = _mod._deep_merge
_escape_tag = _mod._escape_tag
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_item(
    oid=296490,
    kennzeichen="ARG",
    seriennummer="38-81A0102",
    luftfahrzeugart="Flugzeug",
    hersteller="Piper Aircraft Corp.",
    baumuster="PA-38-112",
    halter="Josef Mustermann\r\n1010 Wien, Ringstraße 1\r\nÖsterreich",
    loeschung=None,
) -> dict:
    return {
        "oid": oid,
        "kennzeichen": kennzeichen,
        "ordnungszahl": 2373,
        "seriennummer": seriennummer,
        "luftfahrzeugart": luftfahrzeugart,
        "luftfahrzeugartOid": 167,
        "mtom": 758.0,
        "hersteller": hersteller,
        "baumuster": baumuster,
        "halter": halter,
        "loeschung": loeschung,
    }


def _make_redis(existing=None):
    r = MagicMock()
    mget_result = [[existing]] if existing is not None else [None]
    r.json.return_value.mget.return_value = mget_result
    r.json.return_value.get.return_value = None
    return r


def _make_redis_with_search(icao_hex="440123", registration="OE-ARG"):
    """Redis mock that returns a search result and no existing JSON record."""
    r = _make_redis()
    doc = MagicMock()
    doc.id = f"icao_hex:{icao_hex}"
    doc.registration = registration
    results = MagicMock()
    results.docs = [doc]
    r.ft.return_value.search.return_value = results
    return r


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_flugzeug(self):
        assert _decode_aircraft_type("Flugzeug") == "Airplane"

    def test_hubschrauber(self):
        assert _decode_aircraft_type("Hubschrauber") == "Helicopter"

    def test_tragschrauber(self):
        assert _decode_aircraft_type("Tragschrauber") == "Gyroplane"

    def test_eigenstartfaehiger_motorsegler(self):
        assert _decode_aircraft_type("Eigenstartfähiger Motorsegler") == "Glider"

    def test_nicht_eigenstartfaehiger_motorsegler(self):
        assert _decode_aircraft_type("Nicht eigenstartfähiger Motorsegler") == "Glider"

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Zeppelin") == "Zeppelin"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_aircraft_type("   ") is None

    def test_strips_whitespace(self):
        assert _decode_aircraft_type("  Flugzeug  ") == "Airplane"


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_oesterreich(self):
        assert _decode_country("Österreich") == "AT"

    def test_deutschland(self):
        assert _decode_country("Deutschland") == "DE"

    def test_tschechische_republik(self):
        assert _decode_country("Tschechische Republik") == "CZ"

    def test_schweiz(self):
        assert _decode_country("Schweiz") == "CH"

    def test_spanien(self):
        assert _decode_country("Spanien") == "ES"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_country("   ") is None

    def test_all_known_countries_mapped(self):
        known = [
            "Belgien", "Bulgarien", "Deutschland", "Dänemark", "Estland",
            "Finnland", "Frankreich", "Griechenland", "Irland", "Italien",
            "Kroatien", "Litauen", "Luxemburg", "Malta", "Monaco",
            "Niederlande", "Norwegen", "Polen", "Portugal", "Rumänien",
            "Schweden", "Schweiz", "Slowakei", "Slowenien", "Spanien",
            "Tschechische Republik", "Ungarn", "Zypern", "Österreich",
        ]
        for name in known:
            result = _decode_country(name)
            assert result is not None and len(result) == 2, f"{name!r} → {result!r}"


# ---------------------------------------------------------------------------
# Tests: _parse_halter
# ---------------------------------------------------------------------------

class TestParseHalter:
    def test_austrian_individual(self):
        raw = "Josef Mustermann\r\n1010 Wien, Ringstraße 1\r\nÖsterreich"
        result = _parse_halter(raw)
        assert result["names"] == ["Josef Mustermann"]
        assert result["street"] == ["Ringstraße 1"]
        assert result["city"] == "Wien"
        assert result["postal_code"] == "1010"
        assert result["country"] == "AT"

    def test_foreign_individual(self):
        raw = "Josef Mokry\r\n54401 Dvur Kralove n. Labem, Celakovskeho 886\r\nTschechische Republik"
        result = _parse_halter(raw)
        assert result["names"] == ["Josef Mokry"]
        assert result["street"] == ["Celakovskeho 886"]
        assert result["city"] == "Dvur Kralove n. Labem"
        assert result["postal_code"] == "54401"
        assert result["country"] == "CZ"

    def test_organisation_with_quotes(self):
        raw = '"Luftsportverband Salzburg"\r\n5020 Salzburg, Kendlerstrasse 90\r\nÖsterreich'
        result = _parse_halter(raw)
        assert result["names"] == ["Luftsportverband Salzburg"]
        assert result["street"] == ["Kendlerstrasse 90"]
        assert result["city"] == "Salzburg"
        assert result["postal_code"] == "5020"
        assert result["country"] == "AT"

    def test_multi_owner_takes_first(self):
        raw = (
            "Renate Wildberger\r\n4070 Eferding, Molkereistraße 23\r\nÖsterreich\r\n"
            "Alois Wildberger\r\n4082 Aschach, Siernerstraße 35\r\nÖsterreich"
        )
        result = _parse_halter(raw)
        assert result["names"] == ["Renate Wildberger"]
        assert result["city"] == "Eferding"

    def test_unknown_country_passes_through(self):
        raw = "Test Name\r\n1010 Wien, Hauptstrasse 1\r\nUnknownland"
        result = _parse_halter(raw)
        assert result["country"] == "Unknownland"

    def test_empty_returns_none(self):
        assert _parse_halter("") is None

    def test_none_returns_none(self):
        assert _parse_halter(None) is None

    def test_na_returns_none(self):
        assert _parse_halter("N/A") is None

    def test_fewer_than_three_lines_returns_none(self):
        assert _parse_halter("Only one line") is None
        assert _parse_halter("Line 1\r\nLine 2") is None

    def test_address_without_comma(self):
        raw = "Test Name\r\n1010 Wien\r\nÖsterreich"
        result = _parse_halter(raw)
        assert result["names"] == ["Test Name"]
        assert result["postal_code"] == "1010"
        assert result["city"] == "Wien"
        assert result.get("street") is None

    def test_spanish_address(self):
        raw = "FIS ATO EUROPE SL\r\n11150 Vejer de la Frontera, Pago de Patria\r\nSpanien"
        result = _parse_halter(raw)
        assert result["names"] == ["FIS ATO EUROPE SL"]
        assert result["country"] == "ES"
        assert result["postal_code"] == "11150"
        assert result["city"] == "Vejer de la Frontera"
        assert result["street"] == ["Pago de Patria"]


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        item = _make_item()
        record = _build_record(item, "440123", "OE-ARG")
        assert record["icao_hex"] == "440123"
        assert record["registration"] == "OE-ARG"
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Piper Aircraft Corp."
        assert record["aircraft"]["model"] == "PA-38-112"
        assert record["aircraft"]["serial_number"] == "38-81A0102"
        assert record["registrant"]["names"] == ["Josef Mustermann"]
        assert record["registrant"]["country"] == "AT"

    def test_helicopter_type(self):
        item = _make_item(luftfahrzeugart="Hubschrauber")
        record = _build_record(item, "440123", "OE-ARG")
        assert record["aircraft"]["type"] == "Helicopter"

    def test_glider_type(self):
        item = _make_item(luftfahrzeugart="Eigenstartfähiger Motorsegler")
        record = _build_record(item, "440123", "OE-9522")
        assert record["aircraft"]["type"] == "Glider"

    def test_numeric_kennzeichen(self):
        item = _make_item(kennzeichen="9522", luftfahrzeugart="Eigenstartfähiger Motorsegler")
        record = _build_record(item, "440456", "OE-9522")
        assert record["registration"] == "OE-9522"

    def test_empty_halter_omits_registrant(self):
        item = _make_item(halter="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "registrant" not in record

    def test_empty_manufacturer_omitted(self):
        item = _make_item(hersteller="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "manufacturer" not in record["aircraft"]

    def test_empty_serial_number_omitted(self):
        item = _make_item(seriennummer="")
        record = _build_record(item, "440123", "OE-ARG")
        assert "serial_number" not in record["aircraft"]


# ---------------------------------------------------------------------------
# Tests: _deep_merge
# ---------------------------------------------------------------------------

class TestDeepMerge:
    def test_flat_update_wins(self):
        result = _deep_merge({"a": 1, "b": 2}, {"b": 99})
        assert result == {"a": 1, "b": 99}

    def test_nested_merge(self):
        base = {"aircraft": {"type": "Airplane", "manufacturer": "Boeing"}}
        update = {"aircraft": {"model": "737"}}
        result = _deep_merge(base, update)
        assert result["aircraft"]["type"] == "Airplane"
        assert result["aircraft"]["manufacturer"] == "Boeing"
        assert result["aircraft"]["model"] == "737"

    def test_update_overwrites_nested_value(self):
        base = {"aircraft": {"type": "Airplane"}}
        update = {"aircraft": {"type": "Helicopter"}}
        result = _deep_merge(base, update)
        assert result["aircraft"]["type"] == "Helicopter"

    def test_new_key_added(self):
        result = _deep_merge({"a": 1}, {"b": 2})
        assert result == {"a": 1, "b": 2}

    def test_base_unchanged(self):
        base = {"a": {"x": 1}}
        _deep_merge(base, {"a": {"y": 2}})
        assert "y" not in base["a"]


# ---------------------------------------------------------------------------
# Tests: _escape_tag
# ---------------------------------------------------------------------------

class TestEscapeTag:
    def test_plain_value_unchanged(self):
        assert _escape_tag("OEARG") == "OEARG"

    def test_hyphen_escaped(self):
        result = _escape_tag("OE-ARG")
        assert "\\-" in result

    def test_dot_escaped(self):
        result = _escape_tag("OE.ARG")
        assert "\\." in result


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_active_record_written(self):
        item = _make_item()
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 1
        r.json.return_value.mget.assert_called_once()

    def test_deregistered_record_skipped(self):
        item = _make_item(loeschung="2024-01-15")
        r = _make_redis_with_search()
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0
        r.ft.return_value.search.assert_not_called()

    def test_no_redis_match_not_written(self):
        item = _make_item()
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0
        r.json.return_value.mget.assert_not_called()

    def test_merges_with_existing_record(self):
        item = _make_item()
        existing = {"icao_hex": "440123", "registration": "OE-ARG", "military": False}
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        r.json.return_value.mget.return_value = [[existing]]

        captured = []

        def _capture_set(key, path, value):
            captured.append(value)
            return MagicMock()

        r.pipeline.return_value.__enter__ = MagicMock(return_value=r.pipeline.return_value)
        r.pipeline.return_value.__exit__ = MagicMock(return_value=False)
        r.pipeline.return_value.json.return_value.set.side_effect = _capture_set

        write_to_redis([item], r, REDIS_TTL)
        assert len(captured) == 1
        merged = captured[0]
        assert merged["military"] is False
        assert merged["aircraft"]["type"] == "Airplane"

    def test_registration_prefixed_with_oe(self):
        # Verify that at least one RediSearch query was issued for OE-ARG
        item = _make_item(kennzeichen="ARG")
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis([item], r, REDIS_TTL)
        # Registration OE-ARG matched → one record written
        assert count == 1
        # And the built record carries the OE- prefixed registration
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        if set_call:
            written = set_call[0][2]
            assert written.get("registration") == "OE-ARG"

    def test_numeric_kennzeichen_prefixed(self):
        item = _make_item(kennzeichen="9522", luftfahrzeugart="Eigenstartfähiger Motorsegler")
        r = _make_redis_with_search(icao_hex="440456", registration="OE-9522")
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 1

    def test_empty_kennzeichen_skipped(self):
        item = _make_item(kennzeichen="")
        r = _make_redis_with_search()
        count = write_to_redis([item], r, REDIS_TTL)
        assert count == 0

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        results = MagicMock()
        results.docs = []
        r.ft.return_value.search.return_value = results
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records_written(self):
        items = [
            _make_item(kennzeichen="ARG"),
            _make_item(kennzeichen="ARK", seriennummer="471"),
        ]
        r = _make_redis()

        call_count = [0]

        def _search(query):
            call_count[0] += 1
            results = MagicMock()
            docs = []
            for reg in ["OE-ARG", "OE-ARK"]:
                doc = MagicMock()
                doc.id = f"icao_hex:44012{call_count[0]}"
                doc.registration = reg
                docs.append(doc)
            results.docs = docs
            return results

        r.ft.return_value.search.side_effect = _search
        count = write_to_redis(items, r, REDIS_TTL)
        assert count == 2

    def test_mixed_active_and_deregistered(self):
        items = [
            _make_item(kennzeichen="ARG"),
            _make_item(kennzeichen="OLD", loeschung="2023-06-01"),
        ]
        r = _make_redis_with_search(icao_hex="440123", registration="OE-ARG")
        count = write_to_redis(items, r, REDIS_TTL)
        assert count == 1


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
        with patch("at_austrocontrol_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "77"

    def test_mqtt_publishes_last_run_at(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/last_run_at" in topics

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("at_austrocontrol_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/at-austrocontrol"
