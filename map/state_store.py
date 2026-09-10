"""
Redis-backed per-aircraft live state for the map service.

Three keys per tracked aircraft, all in the map service's own dedicated
Redis instance (never core Redis -- see map/README.md):

- ``flight:live:{icao_hex}`` -- short-TTL sentinel, no meaningful value.
  Its expiry is the "stale" signal (see FlightStateStore.parse_expired_key).
- ``flight:visible:{icao_hex}`` -- a second, longer-TTL sentinel. Its
  expiry is the "hide" signal: the aircraft leaves the map's screen, but
  its detail/trail data is left alone so a resumed flight reappears as one
  continuous track.
- ``flight:detail:{icao_hex}`` -- a Redis hash holding the aircraft's
  merged current-state: every field known from both `position` and
  `metadata` UDP messages, field-level HSET on each update so a partial
  update never clobbers fields it didn't carry. Its expiry is the "remove"
  signal.

``flight:trail:{icao_hex}`` is a fourth, related key: a plain list of JSON
lat/lon/altitude snapshots appended on every accepted `position` update,
refreshed onto the same TTL/lifecycle as ``flight:detail`` so it lives and
dies alongside the aircraft's detail record -- independent of the
stale/hide sentinels above, so it survives both.

A fifth key, unrelated to any one aircraft, tracks message-processor
liveness instead: ``map:processors`` -- a Redis hash (field = processor_id,
value = last-seen epoch timestamp) recording every message processor this
map instance has seen a `heartbeat`/`position`/`metadata` UDP packet from.
Unlike the four keys above, it has no TTL -- see
``FlightStateStore.record_processor_seen``'s docstring for why, and
``processor_status``/``overall_processor_status`` for how a roster entry
becomes a green/amber/red status.

These key families are local to this service -- they're not part of
shared/redis_keys.py's schema, which documents *core* Redis's keys. The map
service's Redis is a second, separate instance this service alone owns, so
its key namespace has no reason to be centralized alongside core's.
"""

from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Optional

from shared.timing import (
    MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS,
    MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS,
)

logger = logging.getLogger("map.state_store")

_LUA_PATH = pathlib.Path(__file__).parent.parent / "shared" / "lua" / "map_apply_update.lua"

_LIVE_PREFIX = "flight:live:"
_VISIBLE_PREFIX = "flight:visible:"
_DETAIL_PREFIX = "flight:detail:"
_TRAIL_PREFIX = "flight:trail:"

# Upper bound on how many points flight:trail:{icao_hex} retains -- the Lua
# LTRIMs to the most recent MAX_TRAIL_POINTS after every append. Mirrors the
# frontend's own MAX_TRAIL_POINTS (map/frontend/src/lib/aircraftState.ts):
# GET /api/flights/{icao_hex} hands this list back as the seed for the
# client-side trail, so a server cap larger than the client's would just be
# trimmed again on arrival, and a smaller one would lose history the client
# would otherwise keep. Kept in sync by hand -- the two can't share a
# constant across the Python/TypeScript boundary.
MAX_TRAIL_POINTS = 300

# Single Redis hash (field = processor_id, value = last-seen epoch
# timestamp) tracking every message processor this map instance has heard
# from -- see FlightStateStore.record_processor_seen/get_processor_roster.
# Deliberately not TTL'd like flight:live/flight:detail above: a processor
# that goes silent is meant to sit in the roster as "red" indefinitely (so
# an operator sees it), not quietly disappear the way a completed flight
# does. The roster resets only when this no-persistence Redis instance
# itself restarts -- see map/README.md's "Processor Roster" section.
_PROCESSOR_ROSTER_KEY = "map:processors"

# Internal bookkeeping field on the flight:detail hash -- the timestamp of
# the last packet actually applied for this icao_hex, used for the
# out-of-order guard (see apply_update). Never returned from get_flight().
_LAST_APPLIED_TIMESTAMP_FIELD = "_last_applied_timestamp"

# Fields carried by a `position` UDP message (shared/config.py's
# map_udp_config() destination; wire format is message-processor's
# _publish_map_position()). A field absent from a given packet is left
# untouched on the merged hash, not blanked -- see apply_update.
POSITION_FIELDS = ("lat", "lon", "alt", "velocity", "hdg", "vs")


def flight_live_key(icao_hex: str) -> str:
    return f"{_LIVE_PREFIX}{icao_hex}"


def flight_visible_key(icao_hex: str) -> str:
    return f"{_VISIBLE_PREFIX}{icao_hex}"


def flight_detail_key(icao_hex: str) -> str:
    return f"{_DETAIL_PREFIX}{icao_hex}"


def flight_trail_key(icao_hex: str) -> str:
    return f"{_TRAIL_PREFIX}{icao_hex}"


def processor_status(last_seen: Optional[float], now: float) -> str:
    """One message processor's liveness classification (final thresholds,
    see shared/timing.py's MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS /
    MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS):

    - "green" ("Connected") -- last_seen at most the green threshold ago.
    - "amber" ("Reconnecting") -- between the green and amber thresholds.
    - "red" ("Disconnected") -- beyond the amber threshold, or last_seen is
      None (never seen at all).
    """
    if last_seen is None:
        return "red"
    age = now - last_seen
    if age <= MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS:
        return "green"
    if age <= MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS:
        return "amber"
    return "red"


def overall_processor_status(statuses: list[str]) -> str:
    """The map connection indicator's aggregate color: green only when
    every rostered processor is green, red only when every rostered
    processor is red (an empty roster -- nothing has ever been received --
    counts as red too), amber otherwise (at least one green, at least one
    amber/red)."""
    if not statuses:
        return "red"
    if all(s == "green" for s in statuses):
        return "green"
    if any(s == "green" for s in statuses):
        return "amber"
    return "red"


def parse_expired_key(key: str) -> Optional[tuple[str, str]]:
    """Classifies a key name reported by a Redis `expired` keyevent
    notification. Returns (kind, icao_hex) where kind is "live", "visible",
    or "detail", or None for a key this service doesn't act on the expiry
    of (flight:trail:* expires silently -- it's cleaned up explicitly as a
    side effect of the "detail" case instead, see MapService._handle_expired_key)."""
    if key.startswith(_LIVE_PREFIX):
        return "live", key[len(_LIVE_PREFIX):]
    if key.startswith(_VISIBLE_PREFIX):
        return "visible", key[len(_VISIBLE_PREFIX):]
    if key.startswith(_DETAIL_PREFIX):
        return "detail", key[len(_DETAIL_PREFIX):]
    return None


class FlightStateStore:
    """Wraps the dedicated map Redis instance: merge-on-write current
    state, TTL refresh, trail accumulation, and the out-of-order guard.
    Callers are expected to serialize calls for a given icao_hex (the UDP
    listener processes datagrams on a single thread) -- no locking or
    Lua-script atomicity is used here, since there's exactly one writer.
    """

    def __init__(
        self, redis_client, stale_seconds: int, hide_seconds: int, evict_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._stale_seconds = stale_seconds
        self._hide_seconds = hide_seconds
        self._evict_seconds = evict_seconds
        self._apply_update_sha = redis_client.script_load(_LUA_PATH.read_text())

    def enable_keyspace_notifications(self) -> None:
        """Best-effort -- a CONFIG SET, not persisted by this
        no-persistence Redis instance, so this must be re-applied on every
        connect/reconnect (see MapService._eviction_loop). 'Ex' = keyevent
        notifications for expired keys only; that's the only class of
        event this service needs."""
        try:
            self._redis.config_set("notify-keyspace-events", "Ex")
        except Exception as exc:
            logger.warning("Could not enable Redis keyspace notifications: %r", exc)

    def apply_update(
        self, icao_hex: str, msg_type: str, timestamp: float, fields: dict,
    ) -> Optional[dict]:
        """Merges `fields` into icao_hex's current-state hash and refreshes
        all three TTLs, unless `timestamp` is at or before -- for `position`
        packets only, strictly before -- the last-applied timestamp for
        this aircraft (see the note on equal timestamps below).

        Returns the full merged, decoded current-state dict on success, or
        None if the packet was dropped as out-of-order.

        Equal timestamps are accepted, not dropped: message-processor's
        `_publish_map_position`/`_maybe_publish_map_metadata` both stamp
        their payload from the exact same `received_at` value for one
        source ADS-B message, so a `position` packet and a same-tick
        `metadata` packet for a brand-new aircraft legitimately share one
        timestamp. Rejecting ties (a literal "at or before" reading) would
        silently drop that metadata packet every time. Only a packet
        strictly older than the last one applied is treated as
        out-of-order/reordered.

        A small tolerance is applied around that comparison rather than a
        bare `<` (see map_apply_update.lua's TIMESTAMP_EPSILON_SECONDS):
        `position`'s `timestamp` is
        message-processor's raw float `received_at`, while `metadata`'s
        comparison key is derived by round-tripping that same float through
        `datetime.fromtimestamp(...).isoformat()` and back (see
        map/main.py's `_extract_timestamp`) -- a conversion that only keeps
        microsecond precision. For one source message whose `position` and
        `metadata` packets legitimately share a timestamp, that round-trip
        can come back a hair below the original float, which a bare `<`
        would misread as "older" and silently drop the metadata packet.
        The tolerance is many orders of magnitude tighter than any real
        ADS-B message spacing, so a genuinely reordered/stale packet is
        still rejected.

        The out-of-order check, the merge HSET, all three TTL refreshes, and
        the trail RPUSH (position packets only) are all done server-side in one
        round trip by map_apply_update.lua (shared/lua/), which also
        returns the merged hash -- see that script for the field-by-field
        protocol.
        """
        mapping = {k: json.dumps(v) for k, v in fields.items()}
        field_names = list(mapping.keys())
        field_values = list(mapping.values())

        raw = self._redis.evalsha(
            self._apply_update_sha, 0,
            icao_hex, msg_type, timestamp,
            json.dumps(field_names), json.dumps(field_values),
            self._stale_seconds, self._evict_seconds, self._hide_seconds,
            MAX_TRAIL_POINTS,
        )
        if raw is None:
            logger.debug(
                "Dropping out-of-order %s packet for %s at timestamp %s",
                msg_type, icao_hex, timestamp,
            )
            return None
        return json.loads(raw)

    @staticmethod
    def _decode_hash(raw: dict) -> dict:
        """Shared by get_flight() and list_flights(): every field
        JSON-decoded uniformly (nested objects like `aircraft`/`operator`
        round-trip as dicts, lists as lists), with the internal
        out-of-order bookkeeping field stripped."""
        result: dict = {}
        for field, value in raw.items():
            if field == _LAST_APPLIED_TIMESTAMP_FIELD:
                continue
            try:
                result[field] = json.loads(value)
            except (TypeError, ValueError):
                result[field] = value
        return result

    def get_flight(self, icao_hex: str) -> Optional[dict]:
        """Decodes icao_hex's flight:detail hash into a plain dict. None if
        the aircraft isn't currently tracked (hash doesn't exist / already
        evicted)."""
        raw = self._redis.hgetall(flight_detail_key(icao_hex))
        if not raw:
            return None
        return self._decode_hash(raw)

    def list_flights(self) -> list[dict]:
        """One decoded current-state dict per currently-*visible* aircraft,
        i.e. one per flight:visible:{icao_hex} sentinel that currently
        exists -- matches GET /api/flights exactly (see map/main.py).

        Deliberately keyed off flight:visible rather than flight:detail: a
        hidden aircraft (past MAP_HIDE_SECONDS but not yet evicted) still
        has a flight:detail hash and trail, but a client connecting fresh
        during that gap has no client-accumulated trail for it either --
        a lone frozen icon with no trail would be worse than omitting the
        aircraft entirely. It reappears for everyone the moment a
        `position`/`metadata` event arrives again.

        The SCAN itself may take more than one round trip on a large
        keyspace (redis-py's scan_iter pages through cursors), but every
        aircraft's HGETALL is issued as one pipeline -- a single round
        trip -- instead of one-HGETALL-per-aircraft N+1."""
        keys = list(self._redis.scan_iter(match=f"{_VISIBLE_PREFIX}*"))
        if not keys:
            return []
        pipe = self._redis.pipeline()
        for key in keys:
            icao_hex = key[len(_VISIBLE_PREFIX):]
            pipe.hgetall(flight_detail_key(icao_hex))
        results = pipe.execute()
        return [self._decode_hash(raw) for raw in results if raw]

    def get_trail(self, icao_hex: str) -> list[dict]:
        """Every accumulated trail point for icao_hex, oldest first."""
        raw = self._redis.lrange(flight_trail_key(icao_hex), 0, -1)
        return [json.loads(p) for p in raw]

    def handle_expired_key(self, key: str) -> Optional[dict]:
        """Turns a Redis `expired` keyevent's key name into the WebSocket
        event to broadcast ("stale" for flight:live:*, "hide" for
        flight:visible:*, "remove" for flight:detail:*), or None for a key
        this service doesn't act on.

        The "hide" path deliberately touches nothing else -- flight:detail
        and flight:trail are left exactly as they are, so a flight that
        resumes after the hidden gap reappears with its pre-gap trail
        intact. Only "remove" (flight:detail:{icao_hex} expired) evicts
        data, proactively deleting flight:trail:{icao_hex} -- it's
        refreshed onto the same TTL on every update, so it will expire on
        its own moments later in the normal case, but this guarantees no
        leftover trail key can survive a detail-key eviction even if the
        two TTLs ever drift apart."""
        parsed = parse_expired_key(key)
        if parsed is None:
            return None
        kind, icao_hex = parsed
        if kind == "live":
            return {"type": "stale", "icao_hex": icao_hex}
        if kind == "visible":
            return {"type": "hide", "icao_hex": icao_hex}
        # kind == "detail"
        try:
            self._redis.delete(flight_trail_key(icao_hex))
        except Exception as exc:
            logger.debug("Trail cleanup failed for %s: %r", icao_hex, exc)
        return {"type": "remove", "icao_hex": icao_hex}

    # ------------------------------------------------------------------
    # Processor roster -- per-message-processor liveness, derived from
    # *any* map UDP message type carrying a processor_id (heartbeat,
    # position, or metadata alike). See map/main.py's _handle_packet for
    # where this is called from, and the module-level docstring above for
    # the roster's reset semantics.
    # ------------------------------------------------------------------

    def record_processor_seen(self, processor_id: str, timestamp: float) -> None:
        """Records processor_id as alive as of `timestamp` (the map
        service's own receipt time -- see map/main.py's _handle_packet,
        not the sending processor's clock, so cross-host clock skew can
        never distort the green/amber/red thresholds). A later call simply
        overwrites the earlier last-seen value; there is no history kept
        beyond "most recent"."""
        self._redis.hset(_PROCESSOR_ROSTER_KEY, processor_id, timestamp)

    def get_processor_roster(self) -> dict[str, float]:
        """Every processor_id ever recorded since this Redis instance's
        roster hash was last reset (i.e. since its own last restart -- see
        the module docstring), mapped to its last-seen epoch timestamp. A
        malformed value (should never happen outside direct Redis
        tampering) is skipped rather than raising."""
        raw = self._redis.hgetall(_PROCESSOR_ROSTER_KEY)
        roster: dict[str, float] = {}
        for processor_id, value in raw.items():
            try:
                roster[processor_id] = float(value)
            except (TypeError, ValueError):
                logger.warning("Discarding malformed roster entry %s=%r", processor_id, value)
        return roster

    def get_processor_statuses(self, now: Optional[float] = None) -> list[dict]:
        """One {"processor_id", "last_seen", "status"} dict per rostered
        processor, sorted by processor_id -- a stable order for GET
        /api/processors, independent of insertion/recency order."""
        if now is None:
            now = time.time()
        roster = self.get_processor_roster()
        return [
            {
                "processor_id": processor_id,
                "last_seen": last_seen,
                "status": processor_status(last_seen, now),
            }
            for processor_id, last_seen in sorted(roster.items())
        ]
