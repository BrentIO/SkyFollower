"""Tests for the Norway CAA data runner."""

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
        "no_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["no_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_country = _mod._decode_country
_extract_icao_hex = _mod._extract_icao_hex
_parse_manufactured_date = _mod._parse_manufactured_date
_build_record = _mod._build_record
download_registry = _mod.download_registry
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
DOWNLOAD_URL = _mod.DOWNLOAD_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_owner(navn="ACME Corp", eier_type="Eier/Kontakt", gateadresse="Gate 1",
                 poststed="Oslo", postnummer="0001", land="Norge") -> dict:
    return {
        "Eier type": eier_type,
        "Navn": navn,
        "Gateadresse": gateadresse,
        "Poststed": poststed,
        "Postnummer": postnummer,
        "Land": land,
    }


def _make_row(
    icao_hex="4CA123",
    registration="LN-ABC",
    kategori="Fly",
    produsent="Cessna",
    type_="172S",
    serienummer="172S12345",
    byggeaar="2010",
    owners=None,
) -> dict:
    if owners is None:
        owners = [_make_owner()]
    return {
        "ICAO 24-bits adresse": [{"Heksadesimal": icao_hex}] if icao_hex else [],
        "Registreringsmerke": registration,
        "Kategori": kategori,
        "Produsent": produsent,
        "Type": type_,
        "Serienummer": serienummer,
        "Byggeår": byggeaar,
        "Eier(e)": owners,
    }


def _make_redis():
    return MagicMock()


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_fly_maps_to_airplane(self):
        assert _decode_aircraft_type("Fly") == "Airplane"

    def test_helikopter_maps_to_helicopter(self):
        assert _decode_aircraft_type("Helikopter") == "Helicopter"

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Something Else") == "Something Else"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_norwegian_name_maps_to_no(self):
        assert _decode_country("Norge") == "NO"

    def test_english_name_maps_to_iso(self):
        assert _decode_country("Germany") == "DE"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None


# ---------------------------------------------------------------------------
# Tests: _extract_icao_hex
# ---------------------------------------------------------------------------

class TestExtractIcaoHex:
    def test_finds_heksadesimal(self):
        assert _extract_icao_hex([{"Heksadesimal": "4ca123"}]) == "4CA123"

    def test_empty_array_returns_none(self):
        assert _extract_icao_hex([]) is None

    def test_entry_without_heksadesimal_key_skipped(self):
        assert _extract_icao_hex([{"SomeOtherKey": "x"}]) is None

    def test_empty_heksadesimal_value_returns_none(self):
        assert _extract_icao_hex([{"Heksadesimal": "   "}]) is None

    def test_finds_among_multiple_entries(self):
        entries = [{"SomeOtherKey": "x"}, {"Heksadesimal": "4ca123"}]
        assert _extract_icao_hex(entries) == "4CA123"


# ---------------------------------------------------------------------------
# Tests: _parse_manufactured_date
# ---------------------------------------------------------------------------

class TestParseManufacturedDate:
    def test_valid_year(self):
        assert _parse_manufactured_date("2010") == "2010-01-01T00:00:00Z"

    def test_empty_returns_none(self):
        assert _parse_manufactured_date("") is None

    def test_invalid_returns_none(self):
        assert _parse_manufactured_date("unknown") is None


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row()
        record = _build_record(row)
        assert record["icao_hex"] == "4CA123"
        assert record["registration"] == "LN-ABC"
        assert record["military"] is False
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["aircraft"]["manufactured_date"] == "2010-01-01T00:00:00Z"
        assert record["registrant"]["names"] == ["ACME Corp"]
        assert record["registrant"]["street"] == ["Gate 1"]
        assert record["registrant"]["city"] == "Oslo"
        assert record["registrant"]["postal_code"] == "0001"
        assert record["registrant"]["country"] == "NO"

    def test_no_icao_hex_returns_none(self):
        row = _make_row(icao_hex="")
        assert _build_record(row) is None

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(kategori="", produsent="", type_="", serienummer="", byggeaar="")
        record = _build_record(row)
        assert "aircraft" not in record

    def test_empty_owners_omits_registrant(self):
        row = _make_row(owners=[])
        record = _build_record(row)
        assert "registrant" not in record

    def test_contact_name_first_then_other_owners(self):
        owners = [
            _make_owner(navn="Contact Person", eier_type="Eier/Kontakt"),
            _make_owner(navn="Other Owner", eier_type="Owner"),
        ]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Contact Person", "Other Owner"]

    def test_duplicate_name_between_contact_and_other_deduplicated(self):
        owners = [
            _make_owner(navn="ACME Corp", eier_type="Eier/Kontakt"),
            _make_owner(navn="ACME Corp", eier_type="Owner"),
        ]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert record["registrant"]["names"] == ["ACME Corp"]

    def test_duplicate_names_among_non_contact_owners_deduplicated(self):
        owners = [
            _make_owner(navn="Alice", eier_type="Owner"),
            _make_owner(navn="Alice", eier_type="Owner"),
        ]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Alice"]

    def test_no_contact_owner_still_collects_names_but_no_address(self):
        """When no owner has 'Eier/Kontakt' type, names are still collected
        from all owners, but no address fields are set (only the contact
        entry's address is used)."""
        owners = [
            _make_owner(navn="Alice", eier_type="Owner"),
            _make_owner(navn="Bob", eier_type="Owner"),
        ]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert record["registrant"]["names"] == ["Alice", "Bob"]
        assert "street" not in record["registrant"]
        assert "city" not in record["registrant"]
        assert "country" not in record["registrant"]

    def test_contact_without_name_still_produces_address_only_registrant(self):
        owners = [_make_owner(navn="", eier_type="Eier/Kontakt")]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert "names" not in record["registrant"]
        assert record["registrant"]["street"] == ["Gate 1"]

    def test_contact_without_street_omits_street(self):
        owners = [_make_owner(gateadresse="")]
        row = _make_row(owners=owners)
        record = _build_record(row)
        assert "street" not in record["registrant"]

    def test_empty_manufacturer_omitted(self):
        row = _make_row(produsent="")
        record = _build_record(row)
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serienummer="")
        record = _build_record(row)
        assert "serial_number" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: download_registry
# ---------------------------------------------------------------------------

class TestDownloadRegistry:
    def test_returns_data_array(self):
        with patch("no_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = b"{}"
            resp.json.return_value = {"headers": [], "data": [{"Registreringsmerke": "LN-ABC"}]}
            mock_get.return_value = resp
            rows = download_registry(DOWNLOAD_URL)
        assert len(rows) == 1
        assert rows[0]["Registreringsmerke"] == "LN-ABC"

    def test_missing_data_key_returns_empty_list(self):
        with patch("no_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = b"{}"
            resp.json.return_value = {"headers": []}
            mock_get.return_value = resp
            rows = download_registry(DOWNLOAD_URL)
        assert rows == []

    def test_raises_on_http_error(self):
        with patch("no_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                download_registry(DOWNLOAD_URL)


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def test_record_written(self):
        rows = [_make_row()]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 1

    def test_missing_icao_hex_skipped(self):
        rows = [_make_row(icao_hex="")]
        r = _make_redis()
        count = write_to_redis(rows, r, REDIS_TTL)
        assert count == 0

    def test_writes_to_registry_key(self):
        rows = [_make_row(icao_hex="4CA123")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][0] == "aircraft:registry:4CA123"

    def test_source_field_in_written_record(self):
        rows = [_make_row()]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        set_call = r.pipeline.return_value.json.return_value.set.call_args
        assert set_call[0][2]["source"] == "no-caa"

    def test_ttl_applied(self):
        rows = [_make_row(icao_hex="4CA123")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4CA123", REDIS_TTL)

    def test_empty_rows_returns_zero(self):
        r = _make_redis()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records(self):
        rows = [_make_row(icao_hex="4CA123"), _make_row(icao_hex="4CA124")]
        r = _make_redis()
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
        with patch("no_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("no_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("no_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("no_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/no-caa"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.DOWNLOAD_URL, "no-caa", headers={"User-Agent": "P5Software SkyFollower"})
