"""
Tests for message-processor/main.py components that don't require live
infrastructure.
"""

from __future__ import annotations

import importlib.util
import json
import logging
import os
import sqlite3
import sys
import tempfile
import time
from datetime import timezone
from unittest.mock import MagicMock, patch

import pytest

# message-processor/ can't be imported as a normal package -- the hyphen in
# the directory name isn't a valid Python identifier -- so register it under
# the dotted name 'message_processor' via importlib, the same workaround
# archive-processor/tests/conftest.py uses. This has to live inline here
# (not in a conftest.py) because pytest derives every conftest.py's plugin
# name from its "tests/conftest.py" path once the hyphenated parent breaks
# the dotted-name walk, so a second same-named conftest.py collides with
# archive-processor's at collection time.
_HERE = os.path.dirname(os.path.abspath(__file__))
_MESSAGE_PROCESSOR_DIR = os.path.dirname(_HERE)
_REPO_ROOT = os.path.dirname(_MESSAGE_PROCESSOR_DIR)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

if "message_processor" not in sys.modules:
    _spec = importlib.util.spec_from_file_location(
        "message_processor",
        os.path.join(_MESSAGE_PROCESSOR_DIR, "__init__.py"),
        submodule_search_locations=[_MESSAGE_PROCESSOR_DIR],
    )
    _pkg = importlib.util.module_from_spec(_spec)
    _pkg.__path__ = [_MESSAGE_PROCESSOR_DIR]
    _pkg.__package__ = "message_processor"
    sys.modules["message_processor"] = _pkg
    _spec.loader.exec_module(_pkg)

from message_processor.main import (  # noqa: E402  (after sys.path/package setup)
    Flight,
    MessageProcessor,
    _ArchiveFallbackQueue,
    _DepthHWM,
    _RateTracker,
    _TimeTracker,
    _SCHEMA,
)
from shared.models import InboundMessage, Position, Velocity


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript(_SCHEMA)
    return db


def _minimal_config() -> dict:
    return {
        "redis": {"host": "localhost"},
        "rabbitmq": {"host": "localhost", "username": "u", "password": "p"},
        "telemetry_interval_seconds": 30,
        "data_dir": tempfile.mkdtemp(),
    }


def _make_processor(cfg: dict | None = None) -> tuple[MessageProcessor, MagicMock]:
    """Construct a real MessageProcessor (file-backed active store) with Redis/rules/
    processor-ID-claim mocked out, matching TestProcessorEnrichment's pattern
    but keeping the real on-disk DB instead of swapping in an in-memory one —
    needed for the crash-recovery/message-clock tests below."""
    cfg = cfg or _minimal_config()
    with patch("message_processor.main.redis_lib.Redis") as MockRedis, \
         patch("message_processor.main.RulesEngine"), \
         patch("message_processor.main.pathlib.Path"), \
         patch.object(MessageProcessor, "_claim_message_processor_id"):
        mock_redis = MagicMock()
        mock_redis.script_load.return_value = "abc123sha"
        MockRedis.return_value = mock_redis
        p = MessageProcessor(cfg, message_processor_id=0)
        p._redis = mock_redis
        p._merge_sha = "abc123sha"
        p._route_sha = "routesha123"
        p._rules_engine.evaluate.return_value = []
        return p, mock_redis


# ---------------------------------------------------------------------------
# _decode_1090 (#302 — pyModeS 3.x migration)
#
# These hex frames are hand-crafted with pyModeS's own CRC function
# (pyModeS._bits.crc_remainder) rather than copy-pasted from elsewhere, so
# each one is deliberately built to exercise exactly one field combination
# and independently verified against pms.decode() directly before being
# used here. This is the coverage that would have caught the pre-#302 bug
# (pms.df() raising V2APIRemovedError on every message) — every other test
# in this file calls _update_flight directly with hand-built dicts and
# never touches real decode at all.
# ---------------------------------------------------------------------------

class TestDecode1090:
    def test_ident_and_wake_turbulence_category(self):
        # TC=4, category=5 (Heavy), callsign "TESTHVY1"
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7F255054D42166710A1432",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["ident"] == "TESTHVY1"
        assert data["wake_turbulence_category"] == "heavy"

    def test_gps_velocity(self):
        # TC=19 subtype 1 (GPS): groundspeed=159, track≈182.88, vertical_rate=-832
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D485020994409940838175B284F",
            icao_hex="485020", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["velocity"] == 159
        assert data["heading"] == pytest.approx(182.88, abs=0.01)
        assert data["vertical_speed"] == -832

    def test_airspeed_velocity(self):
        # TC=19 subtype 3 (airspeed): airspeed=250 IAS, heading=90.0
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7F9B05001F600000533884",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["velocity"] == 250
        assert data["heading"] == 90.0

    def test_position_with_configured_reference(self):
        p, _ = _make_processor(_minimal_config() | {"latitude": 52.2572, "longitude": 3.9198})
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A7",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["latitude"] == pytest.approx(52.2572, abs=0.001)
        assert data["longitude"] == pytest.approx(3.9198, abs=0.001)
        assert data["altitude"] == 38000

    def test_position_without_configured_reference(self):
        # No latitude/longitude in config — altitude still decodes, but
        # position can't be resolved from a single message without one.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A7",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert "latitude" not in data
        assert data["altitude"] == 38000

    def test_squawk_decoded_from_df21(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="A800030F992252CD453820AD87FB",
            icao_hex="71BE3A", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["squawk"] == "2646"

    def test_crc_valid_is_a_no_op_for_df21(self):
        """Documents real pyModeS behavior (verified by reading message.py):
        crc_valid is hardcoded True for DF0/4/5/11/16/20/21 regardless of
        the actual message content — their CRC field encodes the ICAO
        itself, so there's no single-message corruption signal for these
        DF types at all. A squawk is trusted once decoded; there's nothing
        else to check it against in single-message mode."""
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="A800030F992252CD453820AD87FB",
            icao_hex="000000", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["squawk"] == "2646"

    def test_adsb_version(self):
        # TC=31 subtype 0, version=2
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        data = p._decode_1090(msg)
        assert data["adsb_version"] == 2

    def test_corrupted_crc_rejected(self):
        # Same position message as above with the last hex char flipped —
        # pyModeS still returns the (now-untrustworthy) decoded fields with
        # crc_valid=False; the message must still be rejected.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D40621D58C382D690C8AC2863A0",
            icao_hex="40621D", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_message_type_with_no_tracked_fields_dropped(self):
        # TC=28 (ACAS RA broadcast) — no df/typecode filtering exists
        # anymore, so this relies purely on the message not populating any
        # field _decode_1090 extracts.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FE00000000000005E3ED8",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_garbage_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="not-valid-hex", icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None

    def test_too_short_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8D4B19", icao_hex="4B1900", received_at=1.0, source="1090",
        )
        assert p._decode_1090(msg) is None


# ---------------------------------------------------------------------------
# _decode_978 (#320 — pyModeS978 UAT decoding)
#
# These UAT frames are hand-crafted with a synthetic frame builder ported
# from pyModeS978's own test suite (tests/synth.py — test-only there, not
# shipped in the PyPI package) and independently verified against
# pyModeS978.decode() directly before being hardcoded here, matching the
# same verification discipline TestDecode1090 above uses for 1090 frames.
# ---------------------------------------------------------------------------

class TestDecode978:
    def test_ident_and_wake_turbulence_category(self):
        # payload_type=1 (long), category=4 (medium/large high vortex),
        # callsign "TEST978A"
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["ident"] == "TEST978A"
        assert data["wake_turbulence_category"] == "medium"

    def test_airborne_position_and_velocity(self):
        # Same frame as above: lat/lon/altitude + airborne groundspeed/track/vertical_rate
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["latitude"] == pytest.approx(37.6213, abs=0.001)
        assert data["longitude"] == pytest.approx(-122.3790, abs=0.001)
        assert data["altitude"] == 34875
        assert data["velocity"] == 141
        assert data["heading"] == 45.0
        assert data["vertical_speed"] == 832
        assert data["adsb_version"] == 2

    def test_squawk_decoded(self):
        # Same raw bits as callsign, CSID bit selects squawk instead — no
        # callsign, no category (NO_INFORMATION, not populated)
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E300000000000000000000000000002A0024E6C40000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["squawk"] == "1200"
        assert "ident" not in data
        assert "wake_turbulence_category" not in data

    def test_no_information_category_not_populated(self):
        # callsign present, category=0 (NO_INFORMATION) — mirrors
        # _decode_1090's "No category information" guard
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E30000000000000000000000000003B04CAD40610000000200000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["ident"] == "NOCATAC1"
        assert "wake_turbulence_category" not in data

    def test_ground_heading(self):
        # On-ground frame, type code selects heading (not track); category=9
        # (glider/sailplane) -- not one of the 7 DO-260B wake-turbulence
        # categories, so it's left unset rather than mapped to anything.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E300000000000000008042C000003AD755D6B3890000000200000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["velocity"] == 15
        assert data["heading"] == 270.0
        assert "wake_turbulence_category" not in data

    def test_ground_track(self):
        # On-ground frame, type code selects track (not heading)
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E30000000000000000805540000005C4E6C4E6C40000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        data = p._decode_978(msg)
        assert data["velocity"] == 20
        assert data["heading"] == 90.0

    def test_uplink_frame_returns_none(self):
        # FIS-B uplink frame (432-byte payload, '+' direction) carries no
        # traffic data — decode() returns None, not an error.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="+" + "00" * 432,
            icao_hex="000000", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None

    def test_malformed_input_dropped(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-ZZZZZZ", icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None

    def test_no_useful_fields_dropped(self):
        # payload_type=0 (short) — no Mode Status block present, so no
        # ident/squawk/category/version; every state-vector field left at
        # its "unavailable" raw value.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-00A3D3E30000000000000000000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_978(msg) is None


class TestDecodeMessageRouting:
    def test_978_source_routes_to_decode_978(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="-08A3D3E335818151F32A59C9019432C0E01D96B3912D0A0800000210000000000000",
            icao_hex="A3D3E3", received_at=1.0, source="978",
        )
        assert p._decode_message(msg)["ident"] == "TEST978A"

    def test_1090_source_routes_to_decode_1090(self):
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="1090",
        )
        assert p._decode_message(msg)["adsb_version"] == 2

    def test_mlat_source_routes_to_decode_1090(self):
        # MLAT frames are still raw Mode-S hex — same path as 1090, source
        # was never branched on before this PR either.
        p, _ = _make_processor()
        msg = InboundMessage(
            raw="8DA8AE7FF8000000004000F9567C",
            icao_hex="A8AE7F", received_at=1.0, source="MLAT",
        )
        assert p._decode_message(msg)["adsb_version"] == 2


# ---------------------------------------------------------------------------
# Flight SQLite operations
# ---------------------------------------------------------------------------

class TestFlight:
    def test_load_returns_false_for_unknown(self):
        db = _make_db()
        f = Flight(db)
        assert f.load("AAAAAA") is False

    def test_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 2000.0
        f.total_messages = 42
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        f.ident = "DAL659"
        f.receiver_sources = ["1090"]
        f.save()

        f2 = Flight(db)
        assert f2.load("A8AE7F") is True
        assert f2.ident == "DAL659"
        assert f2.total_messages == 42
        assert f2.aircraft["registration"] == "N659DL"

    def test_receiver_sources_and_force_archive_round_trip(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 2000.0
        f.total_messages = 3
        f.receiver_sources = ["MLAT", "1090"]
        f.force_archive = True
        f.save()

        f2 = Flight(db)
        assert f2.load("A8AE7F") is True
        assert f2.receiver_sources == ["MLAT", "1090"]
        assert f2.force_archive is True

    def test_receiver_sources_and_force_archive_defaults(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.save()

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert f2.receiver_sources == []
        assert f2.force_archive is False

    def test_add_position(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1000.0
        f.last_message = 1000.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_position(Position(timestamp=1000.0, latitude=40.0, longitude=-73.0, altitude=5000))

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert len(f2.positions) == 1
        assert f2.positions[0].altitude == 5000

    def test_add_velocity(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "BBBBBB"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_velocity(Velocity(timestamp=1.0, velocity=450.0, heading=270.0, vertical_speed=500))

        f2 = Flight(db)
        f2.load("BBBBBB")
        assert len(f2.velocities) == 1
        assert f2.velocities[0].velocity == 450.0

    def test_delete_removes_all_rows(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "CCCCCC"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        f.add_position(Position(timestamp=1.0, latitude=0.0, longitude=0.0))
        f.add_velocity(Velocity(timestamp=1.0, velocity=100.0))
        f.delete()

        f2 = Flight(db)
        assert f2.load("CCCCCC") is False
        cur = db.cursor()
        cur.execute("SELECT COUNT(*) FROM positions WHERE icao_hex='CCCCCC'")
        assert cur.fetchone()[0] == 0

    def test_to_completed_flight_shape(self):
        from datetime import datetime
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1717100000.0
        f.last_message = 1717100060.0
        f.total_messages = 10
        f.aircraft = {"icao_hex": "A8AE7F", "registration": "N659DL", "military": False}
        f.ident = "DAL659"
        f.operator = {"airline_designator": "DAL", "source": "mictronics"}
        f.squawk = "1234"
        f.origin = "KATL"
        f.destination = "KLAX"
        f.matched_rules = ["rule_1"]
        f.receiver_sources = ["1090"]
        f.save()

        cf = f.to_completed_flight()

        # _id is UUID-v7 string
        assert isinstance(cf.id, str)
        assert "-" in cf.id

        # military=False stripped
        assert "military" not in cf.aircraft

        # operator source key stripped
        assert "source" not in cf.operator

        # timestamps are UTC-aware datetimes
        assert cf.first_message.tzinfo is not None
        assert cf.destination == "KLAX"
        assert cf.matched_rules == ["rule_1"]
        assert cf.receiver_sources == ["1090"]
        assert cf.force_archive is False

    def test_to_completed_flight_no_aircraft_sets_icao_hex(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "FFFFFF"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["978"]
        f.save()
        cf = f.to_completed_flight()
        assert cf.aircraft["icao_hex"] == "FFFFFF"

    def test_to_completed_flight_force_archive_true(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["MLAT"]
        f.force_archive = True
        f.save()
        cf = f.to_completed_flight()
        assert cf.receiver_sources == ["MLAT"]
        assert cf.force_archive is True


# ---------------------------------------------------------------------------
# MessageProcessor._archive — live-path publish, mirroring receiver._publish()'s
# rmq_connected reset on a basic_publish failure (see #533)
# ---------------------------------------------------------------------------

class TestProcessorArchive:
    def _make_completed_flight(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        return f.to_completed_flight()

    def test_archive_publishes_when_connected(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        p._rmq_channel = mock_channel
        p._rmq_connected = True

        p._archive(self._make_completed_flight())

        mock_channel.basic_publish.assert_called_once()
        assert mock_channel.basic_publish.call_args.kwargs["routing_key"] == "archive"
        assert p._fallback.depth() == 0
        assert p._rmq_connected is True

    def test_archive_falls_back_when_not_connected(self):
        p, _ = _make_processor()
        p._rmq_connected = False

        p._archive(self._make_completed_flight())

        assert p._fallback.depth() == 1
        assert p._rmq_connected is False

    def test_archive_resets_rmq_connected_on_publish_failure(self):
        """A live basic_publish failure is the only self-correcting path
        this component has — unlike the receiver, nothing else in
        _archive() ever flips rmq_connected back, so a failure here must
        set it False rather than leaving it pinned True (see #533)."""
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = RuntimeError("boom")
        p._rmq_channel = mock_channel
        p._rmq_connected = True

        p._archive(self._make_completed_flight())

        assert p._rmq_connected is False
        assert p._fallback.depth() == 1


# ---------------------------------------------------------------------------
# _ArchiveFallbackQueue
# ---------------------------------------------------------------------------

class TestArchiveFallbackQueue:
    def test_put_and_depth(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            assert q.depth() == 0
            q.put('{"test": 1}')
            q.put('{"test": 2}')
            assert q.depth() == 2

    def test_drain_calls_publish_in_order(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("first")
            q.put("second")
            published = []
            q.drain(published.append)
            assert published == ["first", "second"]
            assert q.depth() == 0

    def test_drain_stops_on_exception(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("first")
            q.put("second")

            def fail(_):
                raise ConnectionError("gone")

            q.drain(fail)
            assert q.depth() == 2  # nothing removed

    def test_survives_reopen(self):
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
            path = tmp.name
        q = _ArchiveFallbackQueue(path)
        q.put("persistent")
        del q
        q2 = _ArchiveFallbackQueue(path)
        assert q2.depth() == 1

    def test_drain_in_background_is_a_noop_while_a_drain_is_already_in_progress(self):
        """Simulates the reconnect-triggered and periodic telemetry-tick
        triggers firing close together (#534): if a drain is already
        holding the single-flight guard, a second call must not spawn
        another drain -- overlapping drains could otherwise both SELECT
        the same oldest row before either DELETEs it and duplicate-publish
        it."""
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("payload")
            q._drain_lock.acquire()  # simulate an in-progress drain
            try:
                calls = []
                q.drain_in_background(calls.append)
                time.sleep(0.05)  # give a wrongly-spawned thread a chance to run
                assert calls == []
                assert q.depth() == 1
            finally:
                q._drain_lock.release()

    def test_drain_in_background_runs_and_releases_the_guard(self):
        with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
            q = _ArchiveFallbackQueue(tmp.name)
            q.put("payload")
            calls = []

            q.drain_in_background(calls.append)

            deadline = time.monotonic() + 2
            while q.depth() != 0 and time.monotonic() < deadline:
                time.sleep(0.01)

            assert calls == ["payload"]
            assert q.depth() == 0
            assert not q._drain_lock.locked()


# ---------------------------------------------------------------------------
# MessageProcessor._drain_fallback — and its periodic-tick trigger from
# _telemetry_loop, alongside the existing RabbitMQ-reconnect trigger (#526)
# ---------------------------------------------------------------------------

def _synchronous_drain_thread():
    """Patch threading.Thread so drain_in_background's spawned thread runs
    synchronously in the caller's thread instead of racing the test's own
    assertions against a real background thread. Production code still
    spawns a genuine thread; this only affects the test."""
    class _ImmediateThread:
        def __init__(self, target=None, daemon=None, name=None):
            self._target = target

        def start(self):
            if self._target:
                self._target()

    return patch("message_processor.main.threading.Thread", _ImmediateThread)


class TestProcessorDrainFallback:
    def test_drain_fallback_publishes_queued_items(self):
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        mock_channel = MagicMock()
        p._rmq_channel = mock_channel

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._fallback.depth() == 0
        mock_channel.basic_publish.assert_called_once()
        assert mock_channel.basic_publish.call_args.kwargs["routing_key"] == "archive"

    def test_drain_fallback_leaves_items_queued_on_publish_error(self):
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = ConnectionError("gone")
        p._rmq_channel = mock_channel

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._fallback.depth() == 1

    def test_drain_fallback_resets_rmq_connected_on_publish_failure(self):
        """A failed basic_publish during draining is just as much evidence
        the connection is broken as a failed live-path publish — mirror
        _archive()'s handling (and the receiver's #531 fix)."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        mock_channel = MagicMock()
        mock_channel.basic_publish.side_effect = RuntimeError("boom")
        p._rmq_channel = mock_channel
        p._rmq_connected = True

        with _synchronous_drain_thread():
            p._drain_fallback()

        assert p._rmq_connected is False
        assert p._fallback.depth() == 1

    def _run_one_telemetry_tick(self, p) -> None:
        """Run the real _telemetry_loop for exactly one iteration, by
        making the mocked time.sleep set _shutdown so the loop body runs
        once and then exits — rather than re-implementing the loop's
        conditional in the test, which wouldn't actually exercise it."""
        def fake_sleep(_seconds):
            p._shutdown.set()

        with patch("message_processor.main.time.sleep", side_effect=fake_sleep), \
             patch.object(p, "_publish_telemetry"):
            p._telemetry_loop()

    def test_telemetry_loop_drains_when_connected(self):
        """The periodic tick must attempt a drain when RabbitMQ is
        connected — this is what lets a stuck/missed reconnect-triggered
        drain (see #526) still recover on the next tick, since a publish
        failure can pin _rmq_connected False without the underlying
        connection ever raising AMQPConnectionError to re-enter
        _consume_loop's own reconnect-triggered drain."""
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        p._rmq_channel = MagicMock()
        p._rmq_connected = True

        with _synchronous_drain_thread():
            self._run_one_telemetry_tick(p)

        assert p._fallback.depth() == 0

    def test_telemetry_tick_does_not_drain_when_disconnected(self):
        p, _ = _make_processor()
        p._fallback.put('{"_id": "a"}')
        p._rmq_connected = False

        self._run_one_telemetry_tick(p)

        assert p._fallback.depth() == 1


# ---------------------------------------------------------------------------
# _RateTracker
# ---------------------------------------------------------------------------

class TestRateTracker:
    def test_zero_rate_when_empty(self):
        rt = _RateTracker(window=30)
        assert rt.rate() == 0.0

    def test_rate_reflects_recent_messages(self):
        rt = _RateTracker(window=30)
        for _ in range(30):
            rt.record()
        assert 0.9 <= rt.rate() <= 1.1  # ~1/s


# ---------------------------------------------------------------------------
# _TimeTracker
# ---------------------------------------------------------------------------

class TestTimeTracker:
    def test_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.record(200.0)
        assert tt.avg_ms() == 150.0

    def test_hwm_resets_on_read(self):
        tt = _TimeTracker()
        tt.record_hwm(500)
        tt.record_hwm(200)
        assert tt.hwm_ms_and_reset() == 500
        assert tt.hwm_ms_and_reset() == 0  # reset

    def test_reset_clears_avg(self):
        tt = _TimeTracker()
        tt.record(100.0)
        tt.reset()
        assert tt.avg_ms() == 0.0


# ---------------------------------------------------------------------------
# _DepthHWM — high-water-mark tracker backing rabbitmq_input_queue_depth_hwm
# ---------------------------------------------------------------------------

class TestDepthHWM:
    def test_starts_at_negative_one(self):
        assert _DepthHWM().value_and_reset() == -1

    def test_tracks_max_of_recorded_values(self):
        hwm = _DepthHWM()
        hwm.record(3)
        hwm.record(9)
        hwm.record(5)
        assert hwm.value_and_reset() == 9

    def test_resets_after_read(self):
        hwm = _DepthHWM()
        hwm.record(9)
        assert hwm.value_and_reset() == 9
        assert hwm.value_and_reset() == -1

    def test_error_reading_does_not_clobber_a_prior_valid_max(self):
        """A -1 (error/no-sample) reading is still passed to record() by the
        sampler loop — it must never win against a real depth already seen
        this window, since -1 always loses the max() comparison."""
        hwm = _DepthHWM()
        hwm.record(4)
        hwm.record(-1)
        assert hwm.value_and_reset() == 4

    def test_all_error_readings_report_negative_one(self):
        hwm = _DepthHWM()
        hwm.record(-1)
        hwm.record(-1)
        assert hwm.value_and_reset() == -1


# ---------------------------------------------------------------------------
# MessageProcessor._rmq_queue_depth — passive queue_declare on this processor's own
# input queue, reusing the existing consumer channel
# ---------------------------------------------------------------------------

class TestRmqQueueDepth:
    def test_no_channel_returns_negative_one(self):
        p, _ = _make_processor()
        p._rmq_channel = None
        assert p._rmq_queue_depth() == -1

    def test_returns_message_count_from_passive_declare(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 7
        p._rmq_channel = mock_channel

        assert p._rmq_queue_depth() == 7
        mock_channel.queue_declare.assert_called_once_with(
            queue=p._queue_name, durable=True, passive=True
        )

    def test_declare_error_returns_negative_one(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.side_effect = ConnectionError("gone")
        p._rmq_channel = mock_channel

        assert p._rmq_queue_depth() == -1


# ---------------------------------------------------------------------------
# MessageProcessor._rmq_queue_depth_sampler_loop — polls at most once every 10
# seconds, independent of telemetry_interval_seconds
# ---------------------------------------------------------------------------

class TestRmqQueueDepthSamplerLoop:
    def _run_one_sample_tick(self, p) -> None:
        """Run the real _rmq_queue_depth_sampler_loop for exactly one
        iteration, by making the mocked time.sleep set _shutdown so the
        loop body runs once and then exits — mirrors
        _run_one_telemetry_tick's approach for _telemetry_loop."""
        def fake_sleep(_seconds):
            p._shutdown.set()

        with patch("message_processor.main.time.sleep", side_effect=fake_sleep):
            p._rmq_queue_depth_sampler_loop()

    def test_sleeps_ten_seconds_between_samples(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 0
        p._rmq_channel = mock_channel

        with patch("message_processor.main.time.sleep") as mock_sleep:
            mock_sleep.side_effect = lambda _s: p._shutdown.set()
            p._rmq_queue_depth_sampler_loop()

        mock_sleep.assert_called_once_with(10)

    def test_one_tick_records_sampled_depth_into_hwm(self):
        p, _ = _make_processor()
        mock_channel = MagicMock()
        mock_channel.queue_declare.return_value.method.message_count = 12
        p._rmq_channel = mock_channel

        self._run_one_sample_tick(p)

        assert p._rmq_queue_depth_hwm.value_and_reset() == 12

    def test_one_tick_with_no_channel_records_negative_one(self):
        p, _ = _make_processor()
        p._rmq_channel = None

        self._run_one_sample_tick(p)

        assert p._rmq_queue_depth_hwm.value_and_reset() == -1


# ---------------------------------------------------------------------------
# MessageProcessor enrichment logic (unit tests with mocked Redis)
# ---------------------------------------------------------------------------

class TestProcessorEnrichment:
    def _make_processor(self):
        cfg = _minimal_config()
        with patch("message_processor.main.redis_lib.Redis") as MockRedis, \
             patch("message_processor.main.RulesEngine"), \
             patch("message_processor.main.pathlib.Path"), \
             patch.object(MessageProcessor, "_claim_message_processor_id"):
            mock_redis = MagicMock()
            mock_redis.script_load.return_value = "abc123sha"
            MockRedis.return_value = mock_redis
            p = MessageProcessor(cfg, message_processor_id=0)
            p._redis = mock_redis
            p._merge_sha = "abc123sha"
            p._db = _make_db()
            return p, mock_redis

    def test_enrich_aircraft_populates_from_redis(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL", "type_designator": "B763"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"
        mock_redis.evalsha.assert_called_once_with("abc123sha", 0, "A8AE7F")

    def test_enrich_aircraft_increments_miss_on_cache_miss(self):
        p, mock_redis = self._make_processor()
        mock_redis.evalsha.return_value = None

        f = Flight(p._db)
        f.icao_hex = "ZZZZZZ"
        p._enrich_aircraft(f)

        mock_redis.incr.assert_called()
        assert f.aircraft.get("icao_hex") == "ZZZZZZ"

    def test_enrich_aircraft_strips_registry_wake_turbulence_category(self):
        # wake_turbulence_category is receiver-decode-only -- a merged Redis
        # document (still possibly carrying a registry-sourced value, until
        # the data-runner side stops writing it) must never seed the field.
        p, mock_redis = self._make_processor()
        aircraft_data = {
            "icao_hex": "A8AE7F", "registration": "N659DL",
            "wake_turbulence_category": "heavy",
        }
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        assert f.aircraft["registration"] == "N659DL"
        assert "wake_turbulence_category" not in f.aircraft

    def test_enrich_aircraft_no_registration_key_written(self):
        p, mock_redis = self._make_processor()
        aircraft_data = {"icao_hex": "A8AE7F", "registration": "N659DL"}
        mock_redis.evalsha.return_value = json.dumps(aircraft_data)

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        p._enrich_aircraft(f)

        for call in mock_redis.set.call_args_list:
            assert not str(call).startswith("registration:")

    def test_enrich_operator_skips_us_tail_number(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "N12345"  # US registration
        f.aircraft = {}
        p._enrich_operator(f)
        mock_redis.get.assert_not_called()

    def test_enrich_operator_skips_military(self):
        p, mock_redis = self._make_processor()
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {"military": True}
        p._enrich_operator(f)
        mock_redis.get.assert_not_called()

    def test_enrich_operator_extracts_prefix(self):
        p, mock_redis = self._make_processor()
        mock_redis.get.return_value = json.dumps({"airline_designator": "DAL", "name": "Delta"})

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.ident = "DAL659"
        f.aircraft = {}
        f.operator = {}
        p._enrich_operator(f)

        assert f.operator["airline_designator"] == "DAL"


# ---------------------------------------------------------------------------
# _route_ready / _maybe_resolve_route — route:{ident} leg resolution (#498)
#
# The heuristics themselves (proximity, heading-vs-bearing, cross-track
# sanity check) are unit tested in isolation in test_route_resolver.py;
# these tests cover the message-processor-side wiring: resolution runs the
# moment ident/position/altitude/heading are all known for a flight (not at
# archive time), at most once per flight, and never for an ident that's
# just the aircraft's own tail number.
# ---------------------------------------------------------------------------

def _evalsha_dispatch(route_sha: str, route_return=None, aircraft_return=None):
    """mock_redis.evalsha is shared between merge_aircraft.lua (icao_hex)
    and route_airports.lua (ident) calls -- dispatch on which sha was
    passed so a test can control each independently."""
    def _side_effect(sha, _numkeys, *_args):
        if sha == route_sha:
            return route_return
        return aircraft_return

    return _side_effect


class TestRouteReady:
    def _make_flight(self, p, ident="", registration=None) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-route-ready"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        if registration is not None:
            f.aircraft = {"icao_hex": "A8AE7F", "registration": registration}
        f.save()
        return f

    def test_not_ready_without_ident(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="")
        assert p._route_ready(f) is False

    def test_not_ready_when_ident_matches_registration(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="VPCKA", registration="VP-CKA")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False

    def test_not_ready_without_altitude(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=None))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False

    def test_not_ready_without_heading(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=None))
        assert p._route_ready(f) is False

    def test_ready_when_all_conditions_met(self):
        p, _ = _make_processor()
        f = self._make_flight(p, ident="DAL659")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is True

    def test_ready_regardless_of_arrival_order(self):
        """Altitude/heading recorded on earlier messages than the ident
        still counts -- readiness is checked against the flight's full
        history, not just this message's fields."""
        p, _ = _make_processor()
        f = self._make_flight(p, ident="")
        f.add_position(Position(timestamp=1.0, latitude=1.0, longitude=1.0, altitude=1000))
        f.add_velocity(Velocity(timestamp=1.0, heading=90))
        assert p._route_ready(f) is False  # no ident yet
        f.ident = "DAL659"
        assert p._route_ready(f) is True


class TestMaybeResolveRoute:
    def _make_ready_flight(self, p, ident="DAL659", positions=None, velocities=None) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-resolve-route"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        f.save()
        for pos in positions or [(37.0, -79.0, 35000)]:
            f.add_position(Position(timestamp=1.0, latitude=pos[0], longitude=pos[1], altitude=pos[2]))
        for vel in velocities or [(90,)]:
            f.add_velocity(Velocity(timestamp=1.0, heading=vel[0]))
        return f

    def test_not_triggered_until_ready(self):
        p, mock_redis = _make_processor()
        f = self._make_ready_flight(p, positions=[(37.0, -79.0, None)])  # no altitude yet

        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_not_called()
        assert f.route_resolution_attempted is False

    def test_resolves_direct_two_airport_route(self):
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789},
            {"icao_code": "KATL", "latitude": 33.6367, "longitude": -84.4281},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_called_once_with(p._route_sha, 0, "DAL659")
        assert f.origin == "KJFK"
        assert f.destination == "KATL"
        assert f.route_resolution_attempted is True

    def test_leaves_unset_when_no_route_known(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None
        assert f.route_resolution_attempted is True

    def test_no_route_known_logs_debug_with_redis_response(self, caplog):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p, ident="DAL659")

        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._maybe_resolve_route(f)

        assert "DAL659" in caplog.text
        assert "A8AE7F" in caplog.text
        assert "[]" in caplog.text

    def test_sanity_check_rejection_leaves_unset(self):
        """Real-world case from #498: a flight's actual track ran ~800nm
        from the KMSP-KMKE great-circle line -- the route entry was bogus
        for this flight, and neither field should be set."""
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KMSP", "latitude": 44.882, "longitude": -93.222},
            {"icao_code": "KMKE", "latitude": 42.947, "longitude": -87.897},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p, ident="BOGUS1", positions=[(25.0, -90.0, 35000)])

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None

    def test_sanity_check_rejection_logs_debug_with_reason_and_redis_response(self, caplog):
        """The requested DEBUG log: what route_airports.lua returned, and
        why the candidate was rejected."""
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KMSP", "latitude": 44.882, "longitude": -93.222},
            {"icao_code": "KMKE", "latitude": 42.947, "longitude": -87.897},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_ready_flight(p, ident="BOGUS1", positions=[(25.0, -90.0, 35000)])

        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._maybe_resolve_route(f)

        assert "BOGUS1" in caplog.text
        assert "KMSP" in caplog.text and "KMKE" in caplog.text
        assert "sanity check" in caplog.text

    def test_redis_error_leaves_unset_but_marks_attempted(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = RuntimeError("boom")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)

        assert f.origin is None
        assert f.destination is None
        assert f.route_resolution_attempted is True

    def test_does_not_requery_redis_once_attempted(self):
        """The one-shot guard: a valid ident with no known route must not
        be re-queried against Redis on every subsequent message."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return="[]")
        f = self._make_ready_flight(p)

        p._maybe_resolve_route(f)
        p._maybe_resolve_route(f)
        p._maybe_resolve_route(f)

        mock_redis.evalsha.assert_called_once()


class TestMaybeResolveRouteHeadingStability:
    """A multi-leg route whose heading hasn't stabilized yet (a single
    reading, or a genuinely circling/holding aircraft) must not be marked
    route_resolution_attempted -- it's re-evaluated on later messages once
    more heading samples arrive -- but the fetched airport records are
    cached so the retry never repeats the Redis round trip (#498 follow-up:
    the storm-holding-pattern scenario)."""

    KJFK = {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789}
    KMIA = {"icao_code": "KMIA", "latitude": 25.7959, "longitude": -80.2870}
    KMCO = {"icao_code": "KMCO", "latitude": 28.4294, "longitude": -81.3089}

    def _make_flight(self, p, ident="DAL659") -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-heading-stability"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.ident = ident
        f.save()
        return f

    def test_single_heading_sample_defers_without_marking_attempted(self):
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is False
        assert f.origin is None
        assert f.destination is None
        mock_redis.evalsha.assert_called_once()  # fetched once, cached
        assert f.route_candidate_airports == json.dumps(airports)

    def test_retry_uses_cached_airports_not_a_second_redis_call(self):
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)  # first call: fetches + caches, defers
        p._maybe_resolve_route(f)  # retry: still only one sample -- still deferred
        p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is False
        mock_redis.evalsha.assert_called_once()

    def test_stabilizes_after_more_samples_without_a_second_redis_call(self):
        """Once enough consistent headings accumulate, the same holding
        heading (~350deg) is trusted -- but the along-track sanity check
        still rejects the wrong leg it would otherwise match, all without
        ever calling Redis a second time."""
        p, mock_redis = _make_processor()
        airports = [self.KJFK, self.KMIA, self.KMCO, self.KJFK]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))
        f = self._make_flight(p)
        f.add_position(Position(timestamp=1.0, latitude=30.3, longitude=-81.0, altitude=22000))
        f.add_velocity(Velocity(timestamp=1.0, heading=350))

        p._maybe_resolve_route(f)
        assert f.route_resolution_attempted is False

        for t, heading in enumerate((349, 351, 350), start=2):
            f.add_velocity(Velocity(timestamp=float(t), heading=heading))
            p._maybe_resolve_route(f)

        assert f.route_resolution_attempted is True
        assert f.origin is None
        assert f.destination is None  # along-track check rejects KMIA->KMCO
        mock_redis.evalsha.assert_called_once()


class TestWakeTurbulenceCategoryLiveOverwrite:
    """wake_turbulence_category has exactly one writer (live decode) --
    each new reading replaces the last one outright, no first-wins
    protection (unlike the old setdefault-based merge)."""

    def test_later_message_overwrites_earlier_value(self):
        p, mock_redis = _make_processor()
        icao_hex = "A8AE7F"
        t = 1_700_000_000.0

        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "wake_turbulence_category": "medium"},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.aircraft["wake_turbulence_category"] == "medium"

        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "wake_turbulence_category": "heavy"},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 1, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.aircraft["wake_turbulence_category"] == "heavy"


class TestUpdateFlightTriggersRouteResolution:
    """End-to-end through _update_flight: resolution fires mid-flight, the
    moment the last of ident/position/altitude/heading arrives -- not at
    archive time -- and never re-fires afterward."""

    def test_resolves_as_soon_as_all_fields_present_across_messages(self):
        p, mock_redis = _make_processor()
        airports = [
            {"icao_code": "KJFK", "latitude": 40.6398, "longitude": -73.7789},
            {"icao_code": "KATL", "latitude": 33.6367, "longitude": -84.4281},
        ]
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, route_return=json.dumps(airports))

        icao_hex = "A8AE7F"
        t = 1_700_000_000.0

        # Message 1: ident only.
        with p._db_lock:
            p._update_flight({"icao_hex": icao_hex, "ident": "DAL659"},
                              InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"))
        mock_redis.evalsha.assert_called_once()  # aircraft enrichment only

        # Message 2: position + altitude, still no heading.
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "latitude": 37.0, "longitude": -79.0, "altitude": 35000},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 1, source="1090"),
            )
        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is False

        # Message 3: heading arrives (alongside velocity, as a real airborne
        # velocity message would) -- all four conditions now satisfied.
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "velocity": 450, "heading": 20},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t + 2, source="1090"),
            )

        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is True
        assert f.origin == "KJFK"
        assert f.destination == "KATL"

    def test_never_triggers_for_tail_number_ident(self):
        p, mock_redis = _make_processor()
        aircraft = {"icao_hex": "A8AE7F", "registration": "VP-CKA"}
        mock_redis.evalsha.side_effect = _evalsha_dispatch(p._route_sha, aircraft_return=json.dumps(aircraft))

        icao_hex = "A8AE7F"
        t = 1_700_000_000.0
        with p._db_lock:
            p._update_flight(
                {"icao_hex": icao_hex, "ident": "VPCKA", "latitude": 1.0, "longitude": 1.0,
                 "altitude": 1000, "velocity": 200, "heading": 90},
                InboundMessage(raw="00" * 14, icao_hex=icao_hex, received_at=t, source="1090"),
            )

        f = Flight(p._db)
        f.load(icao_hex)
        assert f.route_resolution_attempted is False
        mock_redis.evalsha.assert_called_once()  # aircraft enrichment only, no route lookup


# ---------------------------------------------------------------------------
# Telemetry — one retained topic per stat
# ---------------------------------------------------------------------------

class TestTelemetryPayload:
    """Tests for _publish_telemetry()'s one-retained-topic-per-stat behaviour."""

    def _make_processor(self) -> MessageProcessor:
        with patch("message_processor.main.redis_lib.Redis"):
            p = MessageProcessor(_minimal_config(), message_processor_id=0)
        return p

    def test_correct_base_topic(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        topics = [c.args[0] for c in mock_mqtt.publish.call_args_list]
        assert all(t.startswith("SkyFollower/message-processor/0/statistic/") for t in topics)

    def test_retained(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        for call in mock_mqtt.publish.call_args_list:
            assert call.kwargs.get("retain") is True

    def test_payload_fields(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_telemetry()
        base = "SkyFollower/message-processor/0/statistic"
        topics = {c.args[0] for c in mock_mqtt.publish.call_args_list}
        expected = {
            "started_at", "messages_per_second", "processing_time_hwm_ms",
            "rules_engine_hwm_ms", "rabbitmq_input_queue_depth_hwm",
            "local_archive_queue_depth", "active_flights",
            "registration_misses_hour", "registration_misses_today",
            "aircraft_type_misses_hour", "aircraft_type_misses_today",
        }
        assert {f"{base}/{name}" for name in expected}.issubset(topics)

    def test_processing_time_hwm_not_avg(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._processing_time.record(10.0)
        p._processing_time.record_hwm(10.0)
        p._processing_time.record(50.0)
        p._processing_time.record_hwm(50.0)
        p._publish_telemetry()
        calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert calls["SkyFollower/message-processor/0/statistic/processing_time_hwm_ms"] == "50.0"

    def test_rmq_queue_depth_hwm_publishes_recorded_max_then_resets(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._rmq_queue_depth_hwm.record(3)
        p._rmq_queue_depth_hwm.record(15)
        p._rmq_queue_depth_hwm.record(8)
        topic = "SkyFollower/message-processor/0/statistic/rabbitmq_input_queue_depth_hwm"

        p._publish_telemetry()
        first_calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert first_calls[topic] == "15"

        mock_mqtt.reset_mock()
        p._publish_telemetry()
        second_calls = {c.args[0]: c.args[1] for c in mock_mqtt.publish.call_args_list}
        assert second_calls[topic] == "-1"

    def test_no_publish_when_mqtt_not_connected(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = False
        p._publish_telemetry()
        mock_mqtt.publish.assert_not_called()

    def test_ha_autodiscovery_uses_direct_state_topic(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        base = "SkyFollower/message-processor/0/statistic/"
        for call in mock_mqtt.publish.call_args_list:
            if call.args[0].startswith("homeassistant/"):
                cfg = json.loads(call.args[1])
                assert "value_template" not in cfg
                assert cfg["state_topic"].startswith(base)

    def test_ha_autodiscovery_no_avg_processing_time(self):
        p = self._make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        p._publish_ha_autodiscovery()
        for call in mock_mqtt.publish.call_args_list:
            assert "avg_processing_time_ms" not in call[0][0]


# ---------------------------------------------------------------------------
# Crash-durable active store
# ---------------------------------------------------------------------------

class TestCrashRecovery:
    """Active store is file-backed; a process restart (crash or deliberate
    stop, handled identically) must recover it without eagerly archiving
    based on wall-clock time elapsed while the process was down."""

    def _write_active_flights_db(self, data_dir, icao_hex, last_message, flight_id="pre-crash-id"):
        path = os.path.join(data_dir, "active_flights.db")
        db = sqlite3.connect(path)
        db.executescript(_SCHEMA)
        db.execute(
            "INSERT INTO flights (icao_hex, flight_id, first_message, last_message, "
            "total_messages, aircraft, ident, operator, squawk, origin, destination, "
            "matched_rules, receiver_sources, force_archive) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (icao_hex, flight_id, last_message - 10, last_message, 5,
             "{}", "", "{}", "", None, None, '["rule_a"]', '["1090"]', 0),
        )
        db.commit()
        db.close()

    def test_recovers_flight_without_archiving(self):
        data_dir = tempfile.mkdtemp()
        # 10 minutes old — would look wall-clock-stale against a 300s TTL,
        # the exact scenario a naive wall-clock eviction check gets wrong.
        old_last_message = time.time() - 600
        self._write_active_flights_db(data_dir, "A8AE7F", old_last_message)

        cfg = _minimal_config()
        cfg["data_dir"] = data_dir
        p, _ = _make_processor(cfg)

        # message_clock floors at the recovered flight's last_message, not
        # at wall-clock "now" — see MessageProcessor.__init__.
        assert p._message_clock == pytest.approx(old_last_message)

        f = Flight(p._db)
        assert f.load("A8AE7F") is True
        assert f.matched_rules == ["rule_a"]
        assert f.flight_id == "pre-crash-id"

        # A periodic eviction sweep right after startup must not archive it
        # — message_clock hasn't advanced past its TTL window yet.
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True

    def test_empty_store_uses_wall_clock(self):
        cfg = _minimal_config()
        p, _ = _make_processor(cfg)
        assert p._message_clock == pytest.approx(time.time(), abs=5)


# ---------------------------------------------------------------------------
# Per-message gap check
# ---------------------------------------------------------------------------

class TestPerMessageGapCheck:
    def test_gap_beyond_ttl_archives_old_and_starts_new(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None  # aircraft enrichment miss

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "old-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.matched_rules = ["rule_a"]
        f.receiver_sources = ["1090"]
        f.save()

        ttl = p._flight_ttl_seconds
        new_time = old_time + ttl + 50  # gap exceeds ttl

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        # Old flight landed in the local fallback (no RabbitMQ connected in
        # this test — MessageProcessor was never start()ed).
        assert p._fallback.depth() == 1

        # A fresh row now exists for the same icao_hex, not an extension of
        # the old one.
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id != "old-flight-id"
        assert f2.matched_rules == []
        assert f2.total_messages == 1
        assert f2.first_message == new_time

    def test_gap_within_ttl_extends_existing_flight(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "same-flight-id"
        f.first_message = old_time - 100
        f.last_message = old_time
        f.total_messages = 5
        f.receiver_sources = ["1090"]
        f.save()

        new_time = old_time + 10  # well within the default 300s ttl
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=new_time, source="1090")
        data = {"icao_hex": "A8AE7F"}

        with p._db_lock:
            p._update_flight(data, msg)

        assert p._fallback.depth() == 0
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True
        assert f2.flight_id == "same-flight-id"
        assert f2.total_messages == 6


# ---------------------------------------------------------------------------
# receiver_sources accumulation + force_archive
# ---------------------------------------------------------------------------

class TestReceiverSourcesAccumulation:
    def test_accumulates_across_messages_with_different_sources(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="MLAT")
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["MLAT", "1090"]

    def test_dedupes_repeated_same_source(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="1090")
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="1090")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["1090"]

    def test_not_only_set_on_first_message(self):
        """Regression guard: source used to be set only inside the
        `if not exists:` branch, so a flight created on MLAT that later got
        picked up on 1090 never reflected the second source."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="MLAT")
        p_db_lock_msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="978")

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)
        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.receiver_sources == ["MLAT"]

        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, p_db_lock_msg2)
        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.receiver_sources == ["MLAT", "978"]


class TestForceArchiveFromRules:
    def test_set_when_matched_rule_has_force_archive(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": True, "conditions": []},
        ]

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="MLAT")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is True
        assert f.matched_rules == ["rule1"]

    def test_not_set_when_matched_rule_lacks_force_archive(self):
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None
        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": False, "conditions": []},
        ]

        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="MLAT")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is False

    def test_sticky_once_set_stays_set(self):
        """A later message with no matching rules must not clear a
        force_archive already set by an earlier match — only one persist-
        worthy match ever needs to have happened for the flight."""
        p, mock_redis = _make_processor()
        mock_redis.evalsha.return_value = None

        p._rules_engine.evaluate.return_value = [
            {"identifier": "rule1", "name": "", "description": "", "force_archive": True, "conditions": []},
        ]
        msg1 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=1.0, source="MLAT")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg1)

        # Second message: rule already in matched_rules, so evaluate()
        # would naturally skip it in the real engine — simulate that here.
        p._rules_engine.evaluate.return_value = []
        msg2 = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=2.0, source="MLAT")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg2)

        f = Flight(p._db)
        f.load("A8AE7F")
        assert f.force_archive is True


# ---------------------------------------------------------------------------
# Flight ID assigned at creation, not archive time
# ---------------------------------------------------------------------------

class TestFlightIdStability:
    def test_persists_across_save_and_reload(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        f2 = Flight(db)
        f2.load("A8AE7F")
        assert f2.flight_id == "abc-123"

    def test_to_completed_flight_reuses_flight_id(self):
        db = _make_db()
        f = Flight(db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "abc-123"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # A duplicate archive attempt (e.g. a crash between the archive
        # commit and the active-store delete) reuses the same _id, landing
        # as an idempotent overwrite of the same S3 object rather than a
        # duplicate record.
        cf1 = f.to_completed_flight()
        cf2 = f.to_completed_flight()
        assert cf1.id == "abc-123"
        assert cf2.id == "abc-123"


# ---------------------------------------------------------------------------
# message_clock gates eviction, not wall-clock time
# ---------------------------------------------------------------------------

class TestMessageClockGatesEviction:
    def test_does_not_evict_ahead_of_message_clock(self):
        p, _ = _make_processor()
        ttl = p._flight_ttl_seconds

        last_message = time.time() - 600  # 10 minutes old by wall-clock

        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = last_message - 10
        f.last_message = last_message
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # Backlog replay has only reached 100s past this flight's last
        # message so far — well within the ttl window, even though real
        # wall-clock time has moved on much further.
        p._message_clock = last_message + 100
        p._evict_stale()
        f2 = Flight(p._db)
        assert f2.load("A8AE7F") is True  # not evicted

        # Once message_clock actually catches up past the ttl window, the
        # normal eviction outcome applies.
        p._message_clock = last_message + ttl + 1
        p._evict_stale()
        f3 = Flight(p._db)
        assert f3.load("A8AE7F") is False  # now evicted


# ---------------------------------------------------------------------------
# MQTT rule-notification flood guard
# ---------------------------------------------------------------------------

class TestMqttLagGuard:
    def _make_flight(self, p) -> Flight:
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "fid-1"
        f.first_message = 1.0
        f.last_message = 1.0
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()
        return f

    def test_suppresses_backlogged_message_notification(self, caplog):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        old_received_at = time.time() - 3600  # an hour old — backlog replay
        with caplog.at_level(logging.DEBUG, logger="message_processor"):
            p._publish_rule_notification(f, {"identifier": "rule_a"}, old_received_at)

        mock_mqtt.publish.assert_not_called()
        assert "rule_a" in caplog.text
        assert "A8AE7F" in caplog.text

    def test_publishes_recent_message_notification(self):
        p, _ = _make_processor()
        mock_mqtt = MagicMock()
        p._mqtt = mock_mqtt
        p._mqtt_connected = True
        f = self._make_flight(p)

        recent_received_at = time.time() - 1
        p._publish_rule_notification(f, {"identifier": "rule_a"}, recent_received_at)
        mock_mqtt.publish.assert_called_once()


# ---------------------------------------------------------------------------
# flight_ttl_seconds: shared Redis config (#477)
# ---------------------------------------------------------------------------

class TestFlightTtlLoad:
    def test_defaults_to_300_when_unset(self):
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = None
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 300

    def test_loads_from_redis_value(self):
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = "600"
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 600

    def test_keeps_default_on_redis_error(self):
        p, mock_redis = _make_processor()
        mock_redis.get.side_effect = ConnectionError("redis down")
        p._load_flight_ttl_seconds()
        assert p._flight_ttl_seconds == 300

    def test_gap_check_uses_loaded_value_not_config(self):
        """The per-message gap check must read the cached attribute, not
        settings.json — config no longer carries flight_ttl_seconds at all."""
        p, mock_redis = _make_processor()
        mock_redis.get.return_value = "10"
        mock_redis.evalsha.return_value = None
        p._load_flight_ttl_seconds()
        assert "flight_ttl_seconds" not in p._cfg

        old_time = 1_700_000_000.0
        f = Flight(p._db)
        f.icao_hex = "A8AE7F"
        f.flight_id = "old-flight-id"
        f.first_message = old_time
        f.last_message = old_time
        f.total_messages = 1
        f.receiver_sources = ["1090"]
        f.save()

        # Gap of 20s exceeds the refreshed 10s ttl, so this should split.
        msg = InboundMessage(raw="00" * 14, icao_hex="A8AE7F", received_at=old_time + 20, source="1090")
        with p._db_lock:
            p._update_flight({"icao_hex": "A8AE7F"}, msg)

        f2 = Flight(p._db)
        f2.load("A8AE7F")
        assert f2.flight_id != "old-flight-id"
