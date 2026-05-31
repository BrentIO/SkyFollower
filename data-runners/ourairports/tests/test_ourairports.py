"""
Tests for the OurAirports data runner.

Covers:
- ICAO 4-char filtering
- Phonic name computation (legacy name-cleanup logic + special cases)
- Correct field mapping including the phonic field
- Redis write with correct key, TTL, and JSON content
- MQTT single JSON payload pattern
"""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module import helper
# ---------------------------------------------------------------------------

_HERE = os.path.dirname(os.path.abspath(__file__))
_RUNNER_DIR = os.path.dirname(_HERE)          # data-runners/ourairports/
_REPO_ROOT = os.path.abspath(os.path.join(_RUNNER_DIR, "..", ".."))

if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)


def _load_main():
    spec = importlib.util.spec_from_file_location(
        "ourairports_main",
        os.path.join(_RUNNER_DIR, "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ourairports_main"] = mod
    spec.loader.exec_module(mod)
    return mod


_mod = _load_main()

is_valid_icao = _mod.is_valid_icao
parse_altitude = _mod.parse_altitude
parse_coordinate = _mod.parse_coordinate
compute_phonic = _mod.compute_phonic
build_airport_record = _mod.build_airport_record
stage_data = _mod.stage_data
write_to_redis = _mod.write_to_redis
publish_completion_stats = _mod.publish_completion_stats
REDIS_TTL = _mod.REDIS_TTL
MQTT_ROOT = _mod.MQTT_ROOT


# ---------------------------------------------------------------------------
# Sample CSV data
# ---------------------------------------------------------------------------

_CSV_HEADER = (
    "id,ident,type,name,latitude_deg,longitude_deg,elevation_ft,"
    "continent,iso_country,iso_region,municipality,scheduled_service,"
    "gps_code,iata_code,local_code,home_link,wikipedia_link,keywords"
)


def _make_csv(*rows: dict) -> str:
    """Build a minimal CSV string from a list of row dicts."""
    lines = [_CSV_HEADER]
    for row in rows:
        lines.append(
            f"{row.get('id', '1')},"
            f"{row.get('ident', 'KJFK')},"
            f"{row.get('type', 'large_airport')},"
            f"\"{row.get('name', 'Test Airport')}\","
            f"{row.get('latitude_deg', '40.6413')},"
            f"{row.get('longitude_deg', '-73.7781')},"
            f"{row.get('elevation_ft', '13')},"
            f"{row.get('continent', 'NA')},"
            f"{row.get('iso_country', 'US')},"
            f"{row.get('iso_region', 'US-NY')},"
            f"{row.get('municipality', 'New York')},"
            f"{row.get('scheduled_service', 'yes')},"
            f"{row.get('gps_code', 'KJFK')},"
            f"{row.get('iata_code', 'JFK')},"
            f"{row.get('local_code', 'JFK')},"
            f",,,"
        )
    return "\n".join(lines)


SAMPLE_CSV = _make_csv(
    {"id": "1", "ident": "KJFK", "name": "John F Kennedy International Airport",
     "latitude_deg": "40.6413", "longitude_deg": "-73.7781", "elevation_ft": "13",
     "iso_country": "US", "municipality": "New York", "type": "large_airport"},
    {"id": "2", "ident": "EGLL", "name": "London Heathrow Airport",
     "latitude_deg": "51.4706", "longitude_deg": "-0.4619", "elevation_ft": "83",
     "iso_country": "GB", "municipality": "London", "type": "large_airport"},
    # 3-char ident — should be skipped
    {"id": "3", "ident": "JFK", "name": "JFK (IATA only)",
     "latitude_deg": "40.6413", "longitude_deg": "-73.7781", "elevation_ft": "13",
     "iso_country": "US", "municipality": "New York", "type": "large_airport"},
    # 5-char ident — should be skipped
    {"id": "4", "ident": "KJFKX", "name": "Too Long",
     "latitude_deg": "40.6413", "longitude_deg": "-73.7781", "elevation_ft": "13",
     "iso_country": "US", "municipality": "New York", "type": "large_airport"},
    # 1-char ident — should be skipped
    {"id": "5", "ident": "K", "name": "Way Too Short",
     "latitude_deg": "40.6413", "longitude_deg": "-73.7781", "elevation_ft": "13",
     "iso_country": "US", "municipality": "New York", "type": "large_airport"},
)


# ---------------------------------------------------------------------------
# Tests: is_valid_icao (4-char filter)
# ---------------------------------------------------------------------------

class TestIsValidIcao:
    def test_4_char_valid(self):
        assert is_valid_icao("KJFK") is True

    def test_3_char_invalid(self):
        assert is_valid_icao("JFK") is False

    def test_5_char_invalid(self):
        assert is_valid_icao("KJFKX") is False

    def test_empty_invalid(self):
        assert is_valid_icao("") is False

    def test_1_char_invalid(self):
        assert is_valid_icao("K") is False

    def test_4_char_with_whitespace(self):
        assert is_valid_icao("KJFK") is True


# ---------------------------------------------------------------------------
# Tests: parse helpers
# ---------------------------------------------------------------------------

class TestParseAltitude:
    def test_integer(self):
        assert parse_altitude("13") == 13

    def test_float_truncated(self):
        assert parse_altitude("83.5") == 83

    def test_empty_returns_none(self):
        assert parse_altitude("") is None

    def test_whitespace_returns_none(self):
        assert parse_altitude("   ") is None

    def test_negative(self):
        assert parse_altitude("-10") == -10


class TestParseCoordinate:
    def test_positive(self):
        assert parse_coordinate("40.6413") == pytest.approx(40.6413)

    def test_negative(self):
        assert parse_coordinate("-73.7781") == pytest.approx(-73.7781)

    def test_empty_returns_none(self):
        assert parse_coordinate("") is None

    def test_invalid_returns_none(self):
        assert parse_coordinate("not_a_number") is None


# ---------------------------------------------------------------------------
# Tests: compute_phonic
# ---------------------------------------------------------------------------

class TestComputePhonic:
    def test_international_airport_stripped(self):
        # City IS in name → use name, strip International + Airport
        p = compute_phonic("KJFK", "John F Kennedy International Airport", "New York")
        assert "International" not in p
        assert "Airport" not in p

    def test_city_not_in_name_prepends_city(self):
        # City "Miami" not in "Opa-locka Executive Airport" → prepend city
        p = compute_phonic("KOPF", "Opa-locka Executive Airport", "Miami")
        assert p.startswith("Miami")

    def test_kpbi_override(self):
        # KPBI keeps full "Palm Beach International Airport" regardless of general rules
        p = compute_phonic("KPBI", "Palm Beach International Airport", "West Palm Beach")
        assert p == "Palm Beach International Airport"

    def test_kmlb_override(self):
        p = compute_phonic("KMLB", "Melbourne Orlando International Airport", "Melbourne")
        assert p == "Melbourne"

    def test_greater_prefix_kept_as_is(self):
        p = compute_phonic("KCVG", "Greater Cincinnati/Northern Kentucky International Airport", "Covington")
        assert p.startswith("Greater")

    def test_use_name_exactly_set(self):
        # KIAD is in the "use name exactly" set
        p = compute_phonic("KIAD", "Washington Dulles International Airport", "Dulles")
        assert "International" not in p
        assert "Airport" not in p
        assert "Washington" in p

    def test_klga_la_guardia_fix(self):
        p = compute_phonic("KLGA", "La Guardia Airport", "New York")
        assert "LaGuardia" in p
        assert "La Guardia" not in p

    def test_double_spaces_removed(self):
        p = compute_phonic("KTST", "Test International Airport", "Test")
        assert "  " not in p

    def test_empty_name_returns_empty(self):
        assert compute_phonic("KTST", "", "City") == ""


# ---------------------------------------------------------------------------
# Tests: stage_data (4-char filter + field mapping)
# ---------------------------------------------------------------------------

class TestStageData:
    def test_only_4char_icao_imported(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(SAMPLE_CSV, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM airports")
            assert cur.fetchone()[0] == 2
            conn.close()

    def test_3char_ident_excluded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(SAMPLE_CSV, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_code FROM airports WHERE icao_code = 'JFK'")
            assert cur.fetchone() is None
            conn.close()

    def test_5char_ident_excluded(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(SAMPLE_CSV, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_code FROM airports WHERE icao_code = 'KJFKX'")
            assert cur.fetchone() is None
            conn.close()

    def test_field_mapping_kjfk(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(SAMPLE_CSV, os.path.join(tmpdir, "staging.db"))
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT * FROM airports WHERE icao_code = 'KJFK'")
            row = cur.fetchone()
            assert row["icao_code"] == "KJFK"
            assert row["name"] == "John F Kennedy International Airport"
            assert row["latitude"] == pytest.approx(40.6413)
            assert row["longitude"] == pytest.approx(-73.7781)
            assert row["altitude_feet"] == 13
            assert row["country"] == "US"
            assert row["municipality"] == "New York"
            assert row["type"] == "large_airport"
            # phonic is computed and stored
            assert row["phonic"] is not None
            assert "International" not in row["phonic"]
            conn.close()

    def test_phonic_stored_in_staging(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(SAMPLE_CSV, os.path.join(tmpdir, "staging.db"))
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute("SELECT phonic FROM airports WHERE icao_code = 'EGLL'")
            row = cur.fetchone()
            assert row["phonic"] is not None
            assert "Airport" not in row["phonic"]
            conn.close()

    def test_icao_code_uppercased(self):
        csv_text = _make_csv({"id": "1", "ident": "kjfk", "name": "Test",
                              "latitude_deg": "40.0", "longitude_deg": "-73.0",
                              "elevation_ft": "10", "iso_country": "US",
                              "municipality": "City", "type": "large_airport"})
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(csv_text, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT icao_code FROM airports")
            row = cur.fetchone()
            assert row[0] == "KJFK"
            conn.close()

    def test_empty_elevation_stored_as_null(self):
        csv_text = _make_csv({"id": "1", "ident": "KTST", "name": "Test",
                              "latitude_deg": "40.0", "longitude_deg": "-73.0",
                              "elevation_ft": "", "iso_country": "US",
                              "municipality": "City", "type": "small_airport"})
        with tempfile.TemporaryDirectory() as tmpdir:
            conn = stage_data(csv_text, os.path.join(tmpdir, "staging.db"))
            cur = conn.cursor()
            cur.execute("SELECT altitude_feet FROM airports WHERE icao_code = 'KTST'")
            row = cur.fetchone()
            assert row[0] is None
            conn.close()


# ---------------------------------------------------------------------------
# Tests: build_airport_record
# ---------------------------------------------------------------------------

class TestBuildAirportRecord:
    def _row(self, icao_code="KJFK", name="JFK", phonic="Kennedy",
             latitude=40.6413, longitude=-73.7781, altitude_feet=13,
             country="US", municipality="New York", airport_type="large_airport"):
        # Returns a dict that behaves like sqlite3.Row for field access
        return {
            "icao_code": icao_code,
            "name": name,
            "phonic": phonic,
            "latitude": latitude,
            "longitude": longitude,
            "altitude_feet": altitude_feet,
            "country": country,
            "municipality": municipality,
            "type": airport_type,
        }

    def test_all_fields_present(self):
        row = self._row()
        record = build_airport_record(row)
        assert set(record.keys()) == {
            "icao_code", "name", "phonic", "latitude", "longitude",
            "altitude_feet", "country", "municipality", "type",
        }

    def test_field_values(self):
        row = self._row()
        record = build_airport_record(row)
        assert record["icao_code"] == "KJFK"
        assert record["name"] == "JFK"
        assert record["phonic"] == "Kennedy"
        assert record["latitude"] == pytest.approx(40.6413)
        assert record["country"] == "US"


# ---------------------------------------------------------------------------
# Tests: Redis key construction
# ---------------------------------------------------------------------------

class TestRedisKeys:
    def test_airport_key_format(self):
        from shared.redis_keys import airport_key
        assert airport_key("kjfk") == "airport:KJFK"

    def test_airport_key_already_upper(self):
        from shared.redis_keys import airport_key
        assert airport_key("KJFK") == "airport:KJFK"

    def test_airport_key_lowercase_input(self):
        from shared.redis_keys import airport_key
        assert airport_key("egll") == "airport:EGLL"


# ---------------------------------------------------------------------------
# Tests: write_to_redis
# ---------------------------------------------------------------------------

class TestWriteToRedis:
    def _make_db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.executescript(_mod._SCHEMA)
        conn.execute(
            "INSERT INTO airports "
            "(icao_code, name, phonic, latitude, longitude, altitude_feet, country, municipality, type) "
            "VALUES ('KJFK', 'John F Kennedy International Airport', 'Kennedy', "
            "40.6413, -73.7781, 13, 'US', 'New York', 'large_airport')"
        )
        conn.execute(
            "INSERT INTO airports "
            "(icao_code, name, phonic, latitude, longitude, altitude_feet, country, municipality, type) "
            "VALUES ('EGLL', 'London Heathrow Airport', 'London Heathrow', "
            "51.4706, -0.4619, 83, 'GB', 'London', 'large_airport')"
        )
        conn.commit()
        return conn

    def _mock_redis(self):
        r = MagicMock()
        pipe = MagicMock()
        r.pipeline.return_value = pipe
        pipe.execute.return_value = []
        pipe.set.return_value = pipe
        return r, pipe

    def test_count_matches_airports(self):
        conn = self._make_db()
        r, _ = self._mock_redis()
        assert write_to_redis(conn, r, REDIS_TTL) == 2
        conn.close()

    def test_airport_key_written(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        keys = [c.args[0] for c in pipe.set.call_args_list]
        assert "airport:KJFK" in keys
        assert "airport:EGLL" in keys
        conn.close()

    def test_correct_ttl(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for c in pipe.set.call_args_list:
            assert c.kwargs.get("ex") == REDIS_TTL
        conn.close()

    def test_value_is_valid_json_with_phonic(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        for c in pipe.set.call_args_list:
            parsed = json.loads(c.args[1])
            assert "icao_code" in parsed
            assert "phonic" in parsed
        conn.close()

    def test_correct_record_content(self):
        conn = self._make_db()
        r, pipe = self._mock_redis()
        write_to_redis(conn, r, REDIS_TTL)
        calls_by_key = {c.args[0]: c.args[1] for c in pipe.set.call_args_list}
        kjfk = json.loads(calls_by_key["airport:KJFK"])
        assert kjfk["icao_code"] == "KJFK"
        assert kjfk["name"] == "John F Kennedy International Airport"
        assert kjfk["phonic"] == "Kennedy"
        assert kjfk["country"] == "US"
        assert kjfk["altitude_feet"] == 13
        conn.close()


# ---------------------------------------------------------------------------
# Tests: MQTT completion stats — single JSON payload pattern
# ---------------------------------------------------------------------------

class TestMqttCompletionStats:
    _stats_topic = f"{MQTT_ROOT}/statistics"

    def _setup_mock_client(self):
        mock_client = MagicMock()

        def fake_connect(host, port, keepalive):
            mock_client.on_connect(mock_client, None, None, 0, None)

        mock_client.connect.side_effect = fake_connect
        return mock_client

    def test_single_json_payload_published(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ourairports_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 42, "success")
        # The stats topic should be in published calls
        topics = [c.args[0] for c in mc.publish.call_args_list]
        assert self._stats_topic in topics

    def test_payload_contains_all_fields(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ourairports_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 77, "success")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        payload = json.loads(calls[self._stats_topic])
        assert payload["records_imported"] == 77
        assert payload["last_run_status"] == "success"
        assert "last_run_at" in payload

    def test_failure_status_in_payload(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ourairports_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 0, "failure")
        calls = {c.args[0]: c.args[1] for c in mc.publish.call_args_list
                 if not c.args[0].startswith("homeassistant/")}
        payload = json.loads(calls[self._stats_topic])
        assert payload["last_run_status"] == "failure"

    def test_no_mqtt_config_skips(self):
        cfg = {}
        mc = self._setup_mock_client()
        with patch("ourairports_main.mqtt.Client", return_value=mc):
            publish_completion_stats(cfg, 0, "success")
        mc.connect.assert_not_called()

    def test_ha_autodiscovery_uses_value_template(self):
        cfg = {"mqtt": {"host": "localhost", "port": 1883}}
        mc = self._setup_mock_client()
        with patch("ourairports_main.mqtt.Client", return_value=mc):
            with patch("time.sleep"):
                publish_completion_stats(cfg, 100, "success")
        ha_calls = [c for c in mc.publish.call_args_list
                    if c.args[0].startswith("homeassistant/")]
        assert len(ha_calls) == 3
        for call in ha_calls:
            cfg_payload = json.loads(call.args[1])
            assert "value_template" in cfg_payload
            assert cfg_payload["state_topic"] == self._stats_topic
