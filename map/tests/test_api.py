"""
Integration tests for map/main.py's FastAPI app (GET /api/flights, WS /ws)
against a live Redis, a real `python -m map.main` subprocess, and a real
`websockets` client -- exercises the whole stack end to end: a UDP
datagram in, Redis state merged, a WebSocket event batched and delivered
out, matching the REST snapshot shape. Not mocked -- this is deliberately
the slower, higher-fidelity counterpart to test_udp_handling.py's isolated
dispatch-logic tests.

A real subprocess + a real `websockets` client is used instead of FastAPI's
TestClient for the WebSocket tests: TestClient's synchronous
blocking-portal WebSocket bridge (starlette.testclient) proved unreliable
in this suite for any wait spanning more than one ~250ms batch window
(`ws.receive_json()` would hang indefinitely on a real `Condition.wait()`)
even though the underlying application logic -- proven separately against
this same real-subprocess setup -- is correct and delivers every event
within about two seconds. A real server on a real socket sidesteps that
harness limitation entirely and is arguably the more faithful test besides.
GET /api/flights, which never hit that issue, still just uses a plain HTTP
request (urllib, stdlib-only).

Requires a reachable Redis at REDIS_TEST_HOST:REDIS_TEST_PORT (defaults to
localhost:6379, matching .github/workflows/run-tests.yaml's redis-stack
service). Skipped entirely if none is reachable.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
import uuid
from datetime import datetime, timezone

import pytest

redis = pytest.importorskip("redis")
websockets = pytest.importorskip("websockets")

# All of this module's tests spawn their own real server subprocess on
# freshly-chosen ports, so they don't strictly need to share one worker the
# way test_state_store.py's tests (mutating shared live Redis state) do --
# but pinning them anyway keeps this suite's live-Redis footprint
# predictable and matches shared/tests/test_route_airports_lua.py's
# precedent for live-Redis tests.
pytestmark = pytest.mark.xdist_group(name="map_api")

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_REDIS_HOST = os.environ.get("REDIS_TEST_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("REDIS_TEST_PORT", "6379"))


def _redis_reachable() -> bool:
    try:
        client = redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT, socket_connect_timeout=2)
        client.ping()
        client.close()
        return True
    except Exception:
        return False


if not _redis_reachable():
    pytest.skip(f"No Redis reachable at {_REDIS_HOST}:{_REDIS_PORT} for map API testing", allow_module_level=True)


def _free_port(kind=socket.SOCK_STREAM) -> int:
    s = socket.socket(socket.AF_INET, kind)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _hex() -> str:
    return uuid.uuid4().hex[:6].upper()


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


class _Server:
    """A real `python -m map.main` subprocess, bound to freshly-chosen
    ports so parallel xdist workers/tests never collide."""

    def __init__(self):
        self.udp_port = _free_port(socket.SOCK_DGRAM)
        self.http_port = _free_port()
        env = dict(os.environ)
        env.update({
            "MAP_REDIS_HOST": _REDIS_HOST,
            "MAP_REDIS_PORT": str(_REDIS_PORT),
            "MAP_LISTEN_HOST": "127.0.0.1",
            "MAP_LISTEN_PORT": str(self.udp_port),
            "MAP_HTTP_HOST": "127.0.0.1",
            "MAP_HTTP_PORT": str(self.http_port),
            "MAP_STALE_SECONDS": "1",
            "MAP_EVICT_SECONDS": "2",
            "PYTHONPATH": _REPO_ROOT,
        })
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "map.main"], cwd=_REPO_ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self._wait_until_ready()

    def _wait_until_ready(self, timeout: float = 10.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(f"map service exited early:\n{self.proc.stdout.read()}")
            try:
                s = socket.create_connection(("127.0.0.1", self.http_port), timeout=0.2)
                s.close()
                return
            except OSError:
                time.sleep(0.1)
        raise RuntimeError("map service never became reachable within the timeout")

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.http_port}/ws"

    def send_udp(self, payload: dict) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(json.dumps(payload).encode("utf-8"), ("127.0.0.1", self.udp_port))
        finally:
            sock.close()

    def get_flights(self) -> list[dict]:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port}/api/flights", timeout=5) as resp:
            return json.loads(resp.read())

    def get_processors(self) -> dict:
        with urllib.request.urlopen(f"http://127.0.0.1:{self.http_port}/api/processors", timeout=5) as resp:
            return json.loads(resp.read())

    def close(self) -> None:
        self.proc.terminate()
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()


@pytest.fixture
def server():
    # map:processors (the processor roster hash) has no TTL by design (see
    # map/state_store.py) -- unlike flight:live/flight:detail, which TTL
    # themselves out within this file's MAP_STALE_SECONDS/MAP_EVICT_SECONDS
    # window regardless of test order, a roster entry from an earlier test
    # in this module would otherwise sit forever in this shared real Redis
    # (all of this module's tests use the same db, unlike
    # test_state_store.py's dedicated db 15) and pollute a later test's
    # roster/overall-status assertions.
    client = redis.Redis(host=_REDIS_HOST, port=_REDIS_PORT, socket_connect_timeout=2)
    try:
        client.delete("map:processors")
    finally:
        client.close()

    srv = _Server()
    yield srv
    srv.close()


def _wait_for_flight(server: _Server, icao_hex: str, timeout: float = 3.0, predicate=None) -> dict:
    """Polls GET /api/flights until icao_hex appears -- and, if given,
    `predicate(flight)` is also true. The predicate matters whenever a test
    sends more than one UDP packet in a row: the aircraft can legitimately
    appear in the snapshot after the first packet but before the second
    one has been processed by the (single-threaded, sequential) UDP
    listener, and a caller checking for a field only the second packet adds
    needs to keep polling past that first, incomplete sighting rather than
    asserting against it."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        flights = {f["icao_hex"]: f for f in server.get_flights()}
        if icao_hex in flights:
            last = flights[icao_hex]
            if predicate is None or predicate(last):
                return last
        time.sleep(0.05)
    raise AssertionError(f"{icao_hex} never satisfied the expected condition in GET /api/flights (last seen: {last})")


def test_get_flights_reflects_udp_position_update(server):
    icao_hex = _hex()
    server.send_udp({
        "type": "position", "icao_hex": icao_hex, "timestamp": time.time(),
        "latitude": 12.3, "longitude": 45.6, "altitude": 3500,
    })

    flight = _wait_for_flight(server, icao_hex)
    assert flight["latitude"] == 12.3
    assert flight["longitude"] == 45.6
    assert flight["altitude"] == 3500


def test_get_flights_merges_position_and_metadata_into_one_object(server):
    icao_hex = _hex()
    ts = time.time()
    server.send_udp({
        "type": "position", "icao_hex": icao_hex, "timestamp": ts,
        "latitude": 1.0, "longitude": 2.0,
    })
    server.send_udp({
        "type": "metadata",
        "aircraft": {"icao_hex": icao_hex, "registration": "N1"},
        "ident": "DAL1",
        "last_message": _iso(ts),
        "first_message": _iso(ts),
        "total_messages": 1,
    })

    flight = _wait_for_flight(server, icao_hex, predicate=lambda f: "ident" in f)
    # Same object carries both the position fields and the metadata
    # fields -- not two separate lists (see map/README.md).
    assert flight["latitude"] == 1.0
    assert flight["longitude"] == 2.0
    assert flight["ident"] == "DAL1"
    assert flight["aircraft"] == {"icao_hex": icao_hex, "registration": "N1"}


def test_websocket_receives_batched_position_and_metadata_events(server):
    icao_hex = _hex()
    ts = time.time()

    async def scenario() -> set[str]:
        async with websockets.connect(server.ws_url) as ws:
            server.send_udp({
                "type": "position", "icao_hex": icao_hex, "timestamp": ts,
                "latitude": 9.0, "longitude": 8.0,
            })
            server.send_udp({
                "type": "metadata", "aircraft": {"icao_hex": icao_hex},
                "ident": "UAL1", "last_message": _iso(ts),
            })

            seen_types: set[str] = set()
            async with asyncio.timeout(5):
                while not {"position", "metadata"} <= seen_types:
                    batch = json.loads(await ws.recv())
                    seen_types.update(e["type"] for e in batch if e.get("icao_hex") == icao_hex)
            return seen_types

    seen_types = asyncio.run(scenario())
    assert "position" in seen_types
    assert "metadata" in seen_types


def test_websocket_receives_stale_then_remove_on_eviction(server):
    icao_hex = _hex()

    async def scenario() -> list[str]:
        async with websockets.connect(server.ws_url) as ws:
            server.send_udp({
                "type": "position", "icao_hex": icao_hex, "timestamp": time.time(),
                "latitude": 1.0, "longitude": 1.0,
            })

            order: list[str] = []
            async with asyncio.timeout(8):
                while not {"stale", "remove"} <= set(order):
                    batch = json.loads(await ws.recv())
                    order.extend(
                        e["type"] for e in batch
                        if e.get("icao_hex") == icao_hex and e["type"] in ("stale", "remove")
                    )
            return order

    order = asyncio.run(scenario())
    assert order == ["stale", "remove"], f"unexpected event order: {order}"


def test_get_flights_only_lists_currently_tracked_aircraft(server):
    """An aircraft this test never sent a packet for must not appear --
    confirms the endpoint reflects real Redis state, not some fixture
    leftover from an earlier test."""
    flights = server.get_flights()
    assert isinstance(flights, list)
    assert _hex() not in {f["icao_hex"] for f in flights}


# ---------------------------------------------------------------------------
# GET /api/processors -- message-processor liveness roster/status
# ---------------------------------------------------------------------------

def _wait_for_processor(server, processor_id: str, timeout: float = 3.0) -> dict:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        body = server.get_processors()
        by_id = {p["processor_id"]: p for p in body["processors"]}
        if processor_id in by_id:
            return by_id[processor_id]
        last = body
        time.sleep(0.05)
    raise AssertionError(f"{processor_id} never appeared in GET /api/processors (last seen: {last})")


def test_heartbeat_datagram_adds_processor_to_roster(server):
    processor_id = f"mp-{_hex()}"
    server.send_udp({"type": "heartbeat", "processor_id": processor_id, "timestamp": time.time()})

    entry = _wait_for_processor(server, processor_id)
    assert entry["status"] == "green"


def test_position_datagram_with_processor_id_also_updates_roster(server):
    """The traffic-reduction design's core claim: ordinary position
    traffic, not just a dedicated heartbeat, keeps a processor's roster
    entry alive."""
    processor_id = f"mp-{_hex()}"
    server.send_udp({
        "type": "position", "icao_hex": _hex(), "timestamp": time.time(),
        "processor_id": processor_id, "latitude": 1.0, "longitude": 2.0,
    })

    entry = _wait_for_processor(server, processor_id)
    assert entry["status"] == "green"


def test_metadata_datagram_with_processor_id_also_updates_roster(server):
    processor_id = f"mp-{_hex()}"
    icao_hex = _hex()
    server.send_udp({
        "type": "metadata",
        "aircraft": {"icao_hex": icao_hex},
        "ident": "DAL2",
        "processor_id": processor_id,
        "last_message": _iso(time.time()),
    })

    entry = _wait_for_processor(server, processor_id)
    assert entry["status"] == "green"


def test_processor_id_never_leaks_into_flight_state(server):
    """processor_id describes the sender, not the aircraft -- it must never
    show up on the GET /api/flights record it arrived alongside."""
    processor_id = f"mp-{_hex()}"
    icao_hex = _hex()
    server.send_udp({
        "type": "metadata",
        "aircraft": {"icao_hex": icao_hex},
        "ident": "DAL2",
        "processor_id": processor_id,
        "last_message": _iso(time.time()),
    })

    flight = _wait_for_flight(server, icao_hex, predicate=lambda f: "ident" in f)
    assert "processor_id" not in flight


def test_overall_status_green_when_every_processor_green(server):
    server.send_udp({"type": "heartbeat", "processor_id": f"mp-{_hex()}", "timestamp": time.time()})

    deadline = time.monotonic() + 3.0
    body = server.get_processors()
    while body["overall"] != "green" and time.monotonic() < deadline:
        time.sleep(0.05)
        body = server.get_processors()
    assert body["overall"] == "green"
    assert all(p["status"] == "green" for p in body["processors"])


def test_processors_endpoint_empty_roster_reports_red_overall(server):
    """A map instance that has received nothing at all reports `overall:
    red` -- not some neutral "unknown" state -- with an empty processors
    list."""
    body = server.get_processors()
    assert body["processors"] == []
    assert body["overall"] == "red"
