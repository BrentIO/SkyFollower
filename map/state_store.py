"""
Redis-backed per-aircraft live state for the map service.

Three keys per tracked aircraft, all in the map service's own dedicated
Redis instance (never core Redis):

- ``flight:live:{icao_hex}`` -- short-TTL sentinel; its expiry is the
  "stale" signal. Refreshed only by `position` packets, not `metadata`.
- ``flight:visible:{icao_hex}`` -- a longer-TTL sentinel; its expiry is the
  "hide" signal (leaves the map's screen, but detail/trail data is kept so
  a resumed flight reappears as one continuous track).
- ``flight:detail:{icao_hex}`` -- a Redis hash holding the aircraft's
  merged current-state (field-level HSET per update so a partial update
  never clobbers fields it didn't carry). Its expiry is the "remove" signal.

``flight:trail:{icao_hex}`` is a fourth, related key: a plain list of JSON
lat/lon/altitude snapshots appended on every accepted `position` update,
sharing ``flight:detail``'s TTL/lifecycle independent of the stale/hide
sentinels above, so it survives both.

A fifth key, unrelated to any one aircraft, tracks message-processor
liveness: ``map:processors`` -- a Redis hash (field = processor_id, value =
last-seen epoch timestamp). Unlike the four keys above it has no TTL (see
``FlightStateStore.record_processor_seen``).

These key families are local to this service, not part of
shared/redis_keys.py's core-Redis schema -- the map service's Redis is a
separate instance this service alone owns.
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
# LTRIMs to the most recent MAX_TRAIL_POINTS after every append. Must match
# the frontend's own trail-line cap (aircraftState.ts's MAX_TRAIL_POINTS) by
# hand, since a mismatch either re-trims on arrival or silently loses
# history the client would otherwise keep.
MAX_TRAIL_POINTS = 25000

# Single Redis hash (field = processor_id, value = last-seen epoch
# timestamp) tracking every message processor this map instance has heard
# from. Deliberately not TTL'd like flight:live/flight:detail above: a
# processor that goes silent should sit in the roster as "red" indefinitely
# for an operator to see, not quietly disappear. Resets only when this
# no-persistence Redis instance itself restarts.
_PROCESSOR_ROSTER_KEY = "map:processors"

# Internal bookkeeping field on the flight:detail hash -- last-applied
# timestamp for this icao_hex, used by the out-of-order guard (see
# apply_update). Never returned from get_flight().
_LAST_APPLIED_TIMESTAMP_FIELD = "_last_applied_timestamp"

# Fields carried by a `position` UDP message. A field absent from a given
# packet is left untouched on the merged hash, not blanked -- see apply_update.
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
    """One message processor's liveness classification, using
    shared/timing.py's MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS /
    MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS thresholds: "green" (Connected)
    at or under the green threshold, "amber" (Reconnecting) up to the
    amber threshold, else "red" (Disconnected), including never-seen."""
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
    or "detail", or None for a key this service doesn't act on directly
    (flight:trail:* is cleaned up explicitly as a side effect of "detail"
    instead -- see handle_expired_key)."""
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
        """Best-effort -- a CONFIG SET this no-persistence Redis instance
        never remembers across restart, so it must be re-applied on every
        connect/reconnect. 'Ex' = expired-key keyevents only."""
        try:
            self._redis.config_set("notify-keyspace-events", "Ex")
        except Exception as exc:
            logger.warning("Could not enable Redis keyspace notifications: %r", exc)

    def apply_update(
        self, icao_hex: str, msg_type: str, timestamp: float, fields: dict,
    ) -> Optional[dict]:
        """Merges `fields` into icao_hex's current-state hash and refreshes
        flight:detail's and flight:visible's TTLs, unless the packet is
        older than the last one applied for this aircraft (equal timestamps
        are accepted, not dropped, since a `position` and a same-tick
        `metadata` packet for one source message legitimately share a
        timestamp -- rejecting ties would silently drop that metadata
        packet). A small tolerance is applied around that comparison (see
        map_apply_update.lua's TIMESTAMP_EPSILON_SECONDS) because
        `metadata`'s comparison key round-trips through an ISO-8601 string
        and can come back a hair below the original float.

        flight:live's TTL -- the "stale"/"live" signal -- is refreshed only
        for `msg_type == "position"`, not `"metadata"`: message-processor
        periodically re-sends an active flight's metadata regardless of
        whether anything changed, and refreshing flight:live on that resend
        would cycle stale/live independent of any real data arriving.
        flight:detail/flight:visible refresh on both packet types.

        Returns the full merged, decoded current-state dict on success, or
        None if the packet was dropped as out-of-order.

        The out-of-order check, the merge HSET, all three TTL refreshes,
        and the trail RPUSH (position packets only) are all done
        server-side in one round trip by map_apply_update.lua
        (shared/lua/), which also returns the merged hash.
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
        """One decoded current-state dict per currently-*visible* aircraft
        (one per flight:visible:{icao_hex} sentinel), matching GET
        /api/flights exactly. Keyed off flight:visible rather than
        flight:detail: a hidden-but-not-evicted aircraft has no
        client-accumulated trail for a freshly connecting client, so a
        frozen icon with no trail would be worse than omitting it.

        Every aircraft's HGETALL is issued as one pipeline -- a single
        round trip -- instead of one-HGETALL-per-aircraft N+1."""
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

    def get_flights_batch(self, icao_hex_list: list[str]) -> list[dict]:
        """Batched get_flight()+get_trail(): one pipelined HGETALL round
        for every hex, then one pipelined LRANGE round for only the ones
        that came back tracked -- two round trips total, not 2N. Untracked
        hexes are silently omitted."""
        if not icao_hex_list:
            return []

        detail_pipe = self._redis.pipeline()
        for icao_hex in icao_hex_list:
            detail_pipe.hgetall(flight_detail_key(icao_hex))
        detail_results = detail_pipe.execute()

        tracked: list[tuple[str, dict]] = [
            (icao_hex, self._decode_hash(raw))
            for icao_hex, raw in zip(icao_hex_list, detail_results)
            if raw
        ]
        if not tracked:
            return []

        trail_pipe = self._redis.pipeline()
        for icao_hex, _flight in tracked:
            trail_pipe.lrange(flight_trail_key(icao_hex), 0, -1)
        trail_results = trail_pipe.execute()

        flights: list[dict] = []
        for (_icao_hex, flight), raw_trail in zip(tracked, trail_results):
            flight["trail"] = [json.loads(p) for p in raw_trail]
            flights.append(flight)
        return flights

    def handle_expired_key(self, key: str) -> Optional[dict]:
        """Turns a Redis `expired` keyevent's key name into the WebSocket
        event to broadcast ("stale" for flight:live:*, "hide" for
        flight:visible:*, "remove" for flight:detail:*), or None for a key
        this service doesn't act on.

        Only "remove" evicts data, proactively deleting flight:trail:
        {icao_hex} so no leftover trail key can survive a detail-key
        eviction even if the two TTLs drift apart. "hide" deliberately
        touches nothing, so a flight resuming after the gap keeps its
        pre-gap trail."""
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
    # any map UDP message type carrying a processor_id.
    # ------------------------------------------------------------------

    def record_processor_seen(self, processor_id: str, timestamp: float) -> None:
        """Records processor_id as alive as of `timestamp` (the map
        service's own receipt time, not the sender's clock, so cross-host
        clock skew can't distort the green/amber/red thresholds)."""
        self._redis.hset(_PROCESSOR_ROSTER_KEY, processor_id, timestamp)

    def get_processor_roster(self) -> dict[str, float]:
        """Every processor_id recorded since the roster hash was last
        reset, mapped to its last-seen epoch timestamp. A malformed value
        is skipped rather than raising."""
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
