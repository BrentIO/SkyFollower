"""Tests for the New Zealand CAA data runner."""

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
        "nz_caa_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["nz_caa_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

from shared.url_reachability import assert_url_reachable

_decode_aircraft_type = _mod._decode_aircraft_type
_decode_country = _mod._decode_country
_parse_address = _mod._parse_address
_build_record = _mod._build_record
download_csv = _mod.download_csv
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT
DOWNLOAD_URL = _mod.DOWNLOAD_URL


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_row(
    icao_hex="4CA123",
    registration="ZK-ABC",
    model_category="Aeroplane (Aircraft)",
    manufacturer="Cessna",
    model="172S",
    serial="172S12345",
    owner_name="John Smith",
    owner_address="",
) -> dict:
    return {
        "Mode S Code HEX": icao_hex,
        "Registration Mark": registration,
        "Model Category": model_category,
        "Manufacturer": manufacturer,
        "Model": model,
        "Serial No.": serial,
        "Owner Name": owner_name,
        "Owner Address": owner_address,
    }


def _make_redis():
    r = MagicMock()
    return r


# ---------------------------------------------------------------------------
# Tests: _decode_aircraft_type
# ---------------------------------------------------------------------------

class TestDecodeAircraftType:
    def test_aeroplane_maps_to_airplane(self):
        assert _decode_aircraft_type("Aeroplane (Aircraft)") == "Airplane"

    def test_amateur_built_aeroplane_maps_to_airplane(self):
        assert _decode_aircraft_type("Amateur Built Aeroplane") == "Airplane"

    def test_helicopter_maps_to_helicopter(self):
        assert _decode_aircraft_type("Helicopter") == "Helicopter"

    def test_microlight_class_1_maps_to_microlight(self):
        assert _decode_aircraft_type("Microlight Class 1") == "Microlight"

    def test_power_glider_maps_to_powered_glider(self):
        assert _decode_aircraft_type("Power Glider") == "Powered Glider"

    def test_balloon_maps_to_balloon(self):
        assert _decode_aircraft_type("Balloon (hot-air)") == "Balloon"

    def test_unknown_passes_through(self):
        assert _decode_aircraft_type("Something Else") == "Something Else"

    def test_empty_returns_none(self):
        assert _decode_aircraft_type("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_aircraft_type("   ") is None


# ---------------------------------------------------------------------------
# Tests: _decode_country
# ---------------------------------------------------------------------------

class TestDecodeCountry:
    def test_new_zealand_maps_to_nz(self):
        assert _decode_country("New Zealand") == "NZ"

    def test_australia_maps_to_au(self):
        assert _decode_country("Australia") == "AU"

    def test_united_kingdom_maps_to_gb(self):
        assert _decode_country("United Kingdom") == "GB"

    def test_unknown_passes_through(self):
        assert _decode_country("Utopia") == "Utopia"

    def test_empty_returns_none(self):
        assert _decode_country("") is None

    def test_whitespace_only_returns_none(self):
        assert _decode_country("   ") is None


# ---------------------------------------------------------------------------
# Tests: _parse_address
# ---------------------------------------------------------------------------

class TestParseAddress:
    def test_empty_string_returns_empty_dict(self):
        assert _parse_address("") == {}

    def test_whitespace_only_returns_empty_dict(self):
        assert _parse_address("   ") == {}

    def test_single_segment_stored_as_street_only(self):
        result = _parse_address("Just One Line")
        assert result == {"street": ["Just One Line"]}

    def test_street_city_postcode_country(self):
        result = _parse_address("Street 1, City 1234, New Zealand")
        assert result == {
            "street": ["Street 1"],
            "city": "City",
            "postal_code": "1234",
            "country": "NZ",
        }

    def test_multiple_street_lines(self):
        result = _parse_address("Unit 1, Street 1, City 1234, New Zealand")
        assert result["street"] == ["Unit 1", "Street 1"]
        assert result["city"] == "City"
        assert result["postal_code"] == "1234"

    def test_po_box_as_street_with_valid_city_postcode(self):
        """A PO Box in the street portion is fine — only the city/postcode
        segment itself is checked for the word 'box'."""
        result = _parse_address("PO Box 123, Auckland 1010, New Zealand")
        assert result == {
            "street": ["PO Box 123"],
            "city": "Auckland",
            "postal_code": "1010",
            "country": "NZ",
        }

    def test_po_box_in_city_postcode_position_falls_back_to_street(self):
        """When the second-to-last segment itself looks like a PO box
        (contains 'box' and ends in digits), it's treated as part of the
        street instead of being misparsed as a city/postcode pair."""
        result = _parse_address("Some Street, PO Box 4029, New Zealand")
        assert result == {
            "street": ["Some Street", "PO Box 4029"],
            "country": "NZ",
        }
        assert "city" not in result
        assert "postal_code" not in result

    def test_city_without_trailing_digits_falls_back_to_street(self):
        """If the city/postcode segment has no trailing digits, the regex
        doesn't match and the segment is folded into street instead."""
        result = _parse_address("PO Box 1234, Wellington, New Zealand")
        assert result == {
            "street": ["PO Box 1234", "Wellington"],
            "country": "NZ",
        }
        assert "city" not in result
        assert "postal_code" not in result

    def test_unmapped_country_passed_through(self):
        result = _parse_address("Street 1, City 1234, Utopia")
        assert result["country"] == "Utopia"

    def test_two_segments_only_treated_as_city_postcode_and_country(self):
        result = _parse_address("City 1234, New Zealand")
        assert result == {
            "city": "City",
            "postal_code": "1234",
            "country": "NZ",
        }
        assert "street" not in result


# ---------------------------------------------------------------------------
# Tests: _build_record
# ---------------------------------------------------------------------------

class TestBuildRecord:
    def test_full_record(self):
        row = _make_row(owner_address="Street 1, City 1234, New Zealand")
        record = _build_record(row)
        assert record["icao_hex"] == "4CA123"
        assert record["registration"] == "ZK-ABC"
        assert record["military"] is False
        assert record["aircraft"]["type"] == "Airplane"
        assert record["aircraft"]["manufacturer"] == "Cessna"
        assert record["aircraft"]["model"] == "172S"
        assert record["aircraft"]["serial_number"] == "172S12345"
        assert record["registrant"]["names"] == ["John Smith"]
        assert record["registrant"]["street"] == ["Street 1"]
        assert record["registrant"]["city"] == "City"
        assert record["registrant"]["postal_code"] == "1234"
        assert record["registrant"]["country"] == "NZ"

    def test_missing_icao_hex_returns_none(self):
        row = _make_row(icao_hex="")
        assert _build_record(row) is None

    def test_icao_hex_uppercased(self):
        row = _make_row(icao_hex="4ca123")
        record = _build_record(row)
        assert record["icao_hex"] == "4CA123"

    def test_empty_registration_omitted(self):
        row = _make_row(registration="")
        record = _build_record(row)
        assert record["registration"] is None

    def test_no_aircraft_fields_omits_aircraft_key(self):
        row = _make_row(model_category="", manufacturer="", model="", serial="")
        record = _build_record(row)
        assert "aircraft" not in record

    def test_owner_name_only_no_address(self):
        row = _make_row(owner_name="Jane Doe", owner_address="")
        record = _build_record(row)
        assert record["registrant"] == {"names": ["Jane Doe"]}

    def test_owner_address_only_no_name(self):
        row = _make_row(owner_name="", owner_address="Street 1, City 1234, New Zealand")
        record = _build_record(row)
        assert "names" not in record["registrant"]
        assert record["registrant"]["city"] == "City"

    def test_no_owner_name_or_address_omits_registrant(self):
        row = _make_row(owner_name="", owner_address="")
        record = _build_record(row)
        assert "registrant" not in record

    def test_empty_manufacturer_omitted(self):
        row = _make_row(manufacturer="")
        record = _build_record(row)
        assert "manufacturer" not in record.get("aircraft", {})

    def test_empty_serial_omitted(self):
        row = _make_row(serial="")
        record = _build_record(row)
        assert "serial_number" not in record.get("aircraft", {})


# ---------------------------------------------------------------------------
# Tests: download_csv
# ---------------------------------------------------------------------------

class TestDownloadCsv:
    def test_parses_rows(self):
        with patch("nz_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = (
                "Mode S Code HEX,Registration Mark\r\n"
                "4CA123,ZK-ABC\r\n"
            ).encode("utf-8-sig")
            mock_get.return_value = resp
            rows = download_csv(DOWNLOAD_URL)
        assert len(rows) == 1
        assert rows[0]["Mode S Code HEX"] == "4CA123"
        assert rows[0]["Registration Mark"] == "ZK-ABC"

    def test_raises_on_http_error(self):
        with patch("nz_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 503
            mock_get.return_value = resp
            with pytest.raises(RuntimeError, match="HTTP 503"):
                download_csv(DOWNLOAD_URL)

    def test_uses_download_url(self):
        with patch("nz_caa_main.requests.get") as mock_get:
            resp = MagicMock()
            resp.status_code = 200
            resp.content = b"Mode S Code HEX\r\n"
            mock_get.return_value = resp
            download_csv(DOWNLOAD_URL)
        args, kwargs = mock_get.call_args
        assert args[0] == DOWNLOAD_URL


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
        assert set_call[0][2]["source"] == "nz-caa"

    def test_ttl_applied(self):
        rows = [_make_row(icao_hex="4CA123")]
        r = _make_redis()
        write_to_redis(rows, r, REDIS_TTL)
        r.pipeline.return_value.expire.assert_called_with("aircraft:registry:4CA123", REDIS_TTL)

    def test_empty_list_returns_zero(self):
        r = _make_redis()
        count = write_to_redis([], r, REDIS_TTL)
        assert count == 0

    def test_multiple_records(self):
        rows = [
            _make_row(icao_hex="4CA123", registration="ZK-ABC"),
            _make_row(icao_hex="4CA124", registration="ZK-XYZ"),
        ]
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
        with patch("nz_caa_main.mqtt.Client") as mock_cls:
            mock_client = MagicMock()
            mock_cls.return_value = mock_client
            publish_completion_stats(cfg, 100, "success")

    def test_mqtt_publishes_records_imported(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert f"{MQTT_ROOT}/statistic/records_imported" in topics

    def test_mqtt_records_imported_value(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 366, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/records_imported"] == "366"

    def test_mqtt_publishes_last_run_status(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("nz_caa_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list}
        assert calls[f"{MQTT_ROOT}/statistic/last_run_status"] == "failure"

    def test_mqtt_root_topic(self):
        assert MQTT_ROOT == "SkyFollower/runner/nz-caa"


# ---------------------------------------------------------------------------
# Tests: network (real outbound HTTP call — see #405)
# ---------------------------------------------------------------------------

class TestNetwork:
    @pytest.mark.network
    def test_url_reachable(self):
        assert_url_reachable(_mod.DOWNLOAD_URL, "nz-caa", headers={"User-Agent": "P5Software SkyFollower"})
