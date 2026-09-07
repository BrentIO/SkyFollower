"""
Unit tests for map/main.py's UDP-datagram dispatch logic
(_extract_timestamp / _handle_packet), isolated from Redis with a fake
FlightStateStore stand-in -- the real merge/TTL/eviction semantics are
covered against a live Redis in test_state_store.py; this file is about
"did the right thing get extracted from the wire payload and handed to the
store, and did the right WebSocket event get published."
"""

from __future__ import annotations

from datetime import datetime, timezone

import map.main as map_main


class _FakeStore:
    def __init__(self):
        self.calls: list[tuple] = []
        self.next_result = {}

    def apply_update(self, icao_hex, msg_type, timestamp, fields):
        self.calls.append((icao_hex, msg_type, timestamp, dict(fields)))
        return self.next_result


class _FakeConnections:
    def __init__(self):
        self.published: list[dict] = []

    def publish(self, event):
        self.published.append(event)


def _install_fakes(monkeypatch):
    store = _FakeStore()
    connections = _FakeConnections()
    monkeypatch.setattr(map_main, "_store", store)
    monkeypatch.setattr(map_main, "_connections", connections)
    return store, connections


# ---------------------------------------------------------------------------
# _extract_timestamp
# ---------------------------------------------------------------------------

def test_extract_timestamp_from_position_packet():
    assert map_main._extract_timestamp({"type": "position", "timestamp": 123.5}) == 123.5


def test_extract_timestamp_from_metadata_packet_last_message():
    iso = "2026-09-06T12:00:00.500000+00:00"
    expected = datetime.fromisoformat(iso).timestamp()
    assert map_main._extract_timestamp({"type": "metadata", "last_message": iso}) == expected


def test_extract_timestamp_from_metadata_packet_z_suffix():
    iso = "2026-09-06T12:00:00Z"
    expected = datetime.fromisoformat("2026-09-06T12:00:00+00:00").timestamp()
    assert map_main._extract_timestamp({"type": "metadata", "last_message": iso}) == expected


def test_extract_timestamp_missing_returns_none():
    assert map_main._extract_timestamp({"type": "metadata"}) is None


def test_extract_timestamp_malformed_returns_none():
    assert map_main._extract_timestamp({"type": "position", "timestamp": "not-a-number"}) is None
    assert map_main._extract_timestamp({"type": "metadata", "last_message": "garbage"}) is None


# ---------------------------------------------------------------------------
# _handle_packet -- position
# ---------------------------------------------------------------------------

def test_handle_position_packet_extracts_only_position_fields(monkeypatch):
    store, connections = _install_fakes(monkeypatch)
    store.next_result = {
        "icao_hex": "A8AE7F", "latitude": 1.0, "longitude": 2.0, "altitude": 3000,
    }

    map_main._handle_packet({
        "type": "position", "icao_hex": "A8AE7F", "timestamp": 500.0,
        "latitude": 1.0, "longitude": 2.0, "altitude": 3000,
    })

    assert len(store.calls) == 1
    icao_hex, msg_type, timestamp, fields = store.calls[0]
    assert icao_hex == "A8AE7F"
    assert msg_type == "position"
    assert timestamp == 500.0
    assert fields == {"latitude": 1.0, "longitude": 2.0, "altitude": 3000}

    assert connections.published == [
        {"type": "position", "icao_hex": "A8AE7F", "latitude": 1.0, "longitude": 2.0, "altitude": 3000},
    ]


def test_handle_position_packet_omits_absent_fields_from_published_event(monkeypatch):
    """The published `position` event only carries whatever position
    fields the merged state actually has -- it must not synthesize a null
    for a field the aircraft has simply never reported."""
    store, connections = _install_fakes(monkeypatch)
    store.next_result = {"icao_hex": "A8AE7F", "latitude": 1.0, "longitude": 2.0}

    map_main._handle_packet({
        "type": "position", "icao_hex": "A8AE7F", "timestamp": 500.0,
        "latitude": 1.0, "longitude": 2.0,
    })

    event = connections.published[0]
    assert "velocity" not in event
    assert "heading" not in event
    assert "vertical_speed" not in event
    assert "altitude" not in event


def test_handle_packet_dropped_by_store_publishes_nothing(monkeypatch):
    store, connections = _install_fakes(monkeypatch)
    store.next_result = None  # out-of-order, dropped

    map_main._handle_packet({
        "type": "position", "icao_hex": "A8AE7F", "timestamp": 1.0, "latitude": 1.0, "longitude": 1.0,
    })

    assert connections.published == []


# ---------------------------------------------------------------------------
# _handle_packet -- metadata
# ---------------------------------------------------------------------------

def test_handle_metadata_packet_reads_icao_hex_from_nested_aircraft(monkeypatch):
    """metadata packets never carry a top-level icao_hex -- only nested
    inside `aircraft` (see CompletedFlight.aircraft in shared/models.py)."""
    store, connections = _install_fakes(monkeypatch)
    store.next_result = {
        "icao_hex": "A8AE7F", "ident": "DAL2", "aircraft": {"icao_hex": "A8AE7F"},
    }

    map_main._handle_packet({
        "type": "metadata",
        "aircraft": {"icao_hex": "A8AE7F", "registration": "N1"},
        "ident": "DAL2",
        "last_message": "2026-09-06T00:00:00+00:00",
    })

    icao_hex, msg_type, _timestamp, fields = store.calls[0]
    assert icao_hex == "A8AE7F"
    assert msg_type == "metadata"
    assert "type" not in fields
    assert "icao_hex" not in fields  # never a top-level key on a metadata packet
    assert fields["ident"] == "DAL2"
    assert fields["aircraft"] == {"icao_hex": "A8AE7F", "registration": "N1"}


def test_handle_metadata_packet_publishes_full_merged_record(monkeypatch):
    """The `metadata` WebSocket event carries the *full* merged current-
    state -- both position and metadata fields -- matching GET
    /api/flights' shape exactly (see map/main.py's get_flights)."""
    store, connections = _install_fakes(monkeypatch)
    store.next_result = {
        "icao_hex": "A8AE7F", "latitude": 1.0, "longitude": 2.0,
        "ident": "DAL2", "aircraft": {"icao_hex": "A8AE7F"},
    }

    map_main._handle_packet({
        "type": "metadata",
        "aircraft": {"icao_hex": "A8AE7F"},
        "ident": "DAL2",
        "last_message": "2026-09-06T00:00:00+00:00",
    })

    assert connections.published == [{
        "type": "metadata", "icao_hex": "A8AE7F", "latitude": 1.0, "longitude": 2.0,
        "ident": "DAL2", "aircraft": {"icao_hex": "A8AE7F"},
    }]


def test_handle_packet_missing_icao_hex_is_ignored(monkeypatch):
    store, connections = _install_fakes(monkeypatch)

    map_main._handle_packet({"type": "metadata", "ident": "DAL2"})  # no aircraft.icao_hex
    map_main._handle_packet({"type": "position", "timestamp": 1.0})  # no icao_hex

    assert store.calls == []
    assert connections.published == []


def test_handle_packet_unknown_type_is_ignored(monkeypatch):
    store, connections = _install_fakes(monkeypatch)

    map_main._handle_packet({"type": "something-else", "icao_hex": "A8AE7F"})

    assert store.calls == []
    assert connections.published == []
