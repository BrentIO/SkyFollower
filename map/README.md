# Map Service

Backend for the live-map frontend (`frontend/`, see [Frontend](#frontend-frontend)
below). Three jobs in one process:

1. A UDP listener that receives `position`/`metadata`/`heartbeat` datagrams
   from any/all `message-processor` instances (see
   [message-processor/README.md](../message-processor/README.md)'s "Map UDP
   Publisher" section for the wire format), merges `position`/`metadata`
   into per-aircraft current-state, and records every message's
   `processor_id` (all three types carry one) into a per-processor
   liveness roster -- see [Processor Roster](#processor-roster) below --
   both held in a dedicated Redis instance.
2. A Redis keyspace-notification listener that turns key expiry into
   `stale`/`hide`/`remove` WebSocket events -- the three-stage lifecycle
   described in [Redis State](#redis-state) below -- driven entirely by
   that signal, with no app-level timer loop scanning for expired aircraft.
3. A FastAPI app exposing `GET /api/flights` (a snapshot), `GET
   /api/processors` (the processor roster/status), and `WS /ws` (a live,
   batched relay of `position`/`metadata`/`stale`/`hide`/`remove` events),
   and serving the built frontend SPA itself under `/map` (see
   [Frontend](#frontend-frontend) below).

There is exactly one map service instance -- unlike `message-processor`,
this is not horizontally scaled (no `MAP_SERVICE_ID`-style claim/heartbeat
mechanism).

**Data boundary**: this service's only inputs are the UDP feed from
`message-processor` and its own dedicated Redis. It never queries core
Redis (enrichment keys, `config:*`, etc.) directly -- all enrichment needed
for display (registration, operator, aircraft type) arrives pre-resolved in
`message-processor`'s `metadata` payload.

## TLS

`GET /api/flights`, `WS /ws`, and the served frontend SPA are all HTTPS on
`MAP_HTTP_PORT` (default `443`) by default, terminated directly by uvicorn --
unlike `management-ui`, there's no nginx in front of this service, so
`uvicorn.run()` itself is handed `ssl_certfile`/`ssl_keyfile`
(`map/main.py`'s `_uvicorn_tls_kwargs()`) rather than a reverse proxy doing
TLS termination. The service binds this one port only -- there is no
separate plain-HTTP listener redirecting to it, unlike `management-ui`'s
`80 -> 443` nginx redirect. There is no authentication either way (see the top of
this README) -- TLS here is about encrypting traffic on the LAN, not
access control. The UDP listener (message-processor's position/metadata
feed) is unaffected -- UDP has no TLS.

- **Certificate generation**: `scripts/install.sh` generates a ~10-year
  self-signed cert the first time you install or re-run it for the `map`
  role, writing `cert.pem`/`key.pem` into `./data/map/tls/` (bind-mounted
  read-only into the container at `/app/tls/` -- see
  `docker-compose.map.yaml`). Same SAN-prompting and idempotent-skip
  behavior as `management-ui`'s cert generation -- see
  [management-ui/README.md](../management-ui/README.md#tls).
- **Bring your own certificate**: drop your own `cert.pem`/`key.pem` into
  `./data/map/tls/` (matching those exact filenames) before running
  `scripts/install.sh` -- it leaves an existing pair alone entirely.
- **No cert present**: `_uvicorn_tls_kwargs()` falls back to plain HTTP
  with a logged warning rather than failing to start -- covers running
  `python -m map.main` (or bare `uvicorn map.main:app`) standalone outside
  the installer flow, where no TLS directory was ever generated or
  mounted.
- **First-visit browser warning**: since the certificate is self-signed,
  every browser shows an untrusted-certificate warning the first time you
  visit. Click through it (or add an exception), or import `cert.pem` into
  your OS/browser's trust store if you'd rather not see it again. No
  ACME/Let's Encrypt integration, for the same LAN-only reasoning as
  `management-ui`.
- **Replacing a certificate**: overwrite `cert.pem`/`key.pem` in
  `./data/map/tls/` and restart the container (`docker compose -f
  docker-compose.map.yaml restart map`).

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("map_redis", "map", "mqtt")`, interpolated by Compose from this
host's `.env`.

| Variable | Required | Default | Description |
|---|---|---|---|
| `MAP_LISTEN_HOST` | ❌ | `0.0.0.0` | Bind address for the UDP listener |
| `MAP_LISTEN_PORT` | ✅ | — | Bind port for the UDP listener. Must match whatever port a message processor's `MAP_UDP_PORT` sends datagrams to -- see [Deliberately distinct variable names](#deliberately-distinct-variable-names) below |
| `MAP_HTTP_HOST` | ❌ | `0.0.0.0` | Bind address for the REST/WebSocket API |
| `MAP_HTTP_PORT` | ❌ | `443` | Bind port for the REST/WebSocket API. HTTPS when a TLS cert/key pair is present (see [TLS](#tls) above) — the normal case, hence the `443` default — plain HTTP otherwise. There is no separate plain-HTTP redirect listener; the service binds this one port only |
| `MAP_REDIS_HOST` | ✅ | — | Dedicated Redis instance for this service's own live aircraft state -- **not** core Redis (see the repo root docs' Redis Key Schema for core's schema; this service never reads or writes any of those keys) |
| `MAP_REDIS_PORT` | ❌ | `6379` | |
| `MAP_REDIS_PASSWORD` | ❌ | — | Optional, unlike core's `REDIS_PASSWORD` -- see [Why `MAP_REDIS_PASSWORD` is optional](#why-map_redis_password-is-optional) below |
| `MAP_STALE_SECONDS` | ❌ | `15` | TTL on `flight:live:{icao_hex}`; expiry fades an aircraft client-side (a `stale` WebSocket event) without removing it |
| `MAP_HIDE_SECONDS` | ❌ | `60` | TTL on `flight:visible:{icao_hex}`; expiry drops the aircraft from view (a `hide` WebSocket event) while leaving its `flight:detail:{icao_hex}`/`flight:trail:{icao_hex}` untouched -- see [Lifecycle](#lifecycle) below |
| `MAP_EVICT_SECONDS` | ❌ | `300` | TTL on `flight:detail:{icao_hex}` (and its `flight:trail:{icao_hex}`); expiry hard-removes the aircraft (a `remove` WebSocket event). Should equal the deployment's `flight_ttl_seconds` (core Redis's `config:flight_ttl_seconds`, default 300) -- this service never queries core Redis (see [Data boundary](#map-service) above), so keeping the two in agreement is an operator responsibility, not something enforced across services |

`MAP_STALE_SECONDS < MAP_HIDE_SECONDS < MAP_EVICT_SECONDS` must hold --
`shared/config.py`'s `map_config()` rejects a misordered `.env` at startup.
| `MAP_CENTER_LATITUDE` | ❌ | — | Centered reference point ("center") for the frontend's on-map marker, initial camera position, and "Return to center" button. Both required together, or neither -- without them the map still renders, just without a center marker/recenter target. Read at runtime and served to the frontend over `GET /api/config` (see [Frontend Configuration](#frontend-configuration) below) -- **not** a Vite build-time value, so changing it takes effect on the next page load with no image rebuild |
| `MAP_CENTER_LONGITUDE` | ❌ | — | |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely (see [MQTT and Home Assistant](#mqtt-and-home-assistant) below) |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

The WebSocket batching interval (~250ms) is not an environment variable --
it's a fixed constant, `MAP_WS_BATCH_INTERVAL_SECONDS` in
`shared/timing.py`, same convention as `MQTT_PUBLISH_INTERVAL_SECONDS` and
friends.

### Deliberately distinct variable names

`MAP_LISTEN_HOST`/`MAP_LISTEN_PORT` (this service's own bind address) are
deliberately different names from message-processor's
`MAP_UDP_HOST`/`MAP_UDP_PORT` (the unicast *destination* it sends to --
see `shared/config.py`'s `map_udp_config()`), even though
`MAP_LISTEN_PORT` must operationally equal whatever port a message
processor's `MAP_UDP_PORT` points at. The two values live in separate
`.env` files for separate hosts/components; "bind address" and
"send-to destination" are different concepts, and reusing one variable
name for both risked being misread as a single setting shared between two
components' `.env` files instead of two independent ones that must simply
be kept in agreement operationally.

### Why `MAP_REDIS_PASSWORD` is optional

Core Redis (`REDIS_PASSWORD`) is required everywhere it's consumed --
it holds enrichment data, rules/areas configuration, and cross-fleet
coordination state. This service's dedicated Redis holds none of that: a
pure, in-memory, fully reconstructible-from-live-UDP-traffic cache with no
persistence at all (see [Fault Tolerance](#fault-tolerance) below).
Requiring authentication on it is a deployment choice available via
`MAP_REDIS_PASSWORD`, not a hard requirement the way it is for core Redis.

## MQTT and Home Assistant

MQTT is entirely optional -- leave `MQTT_HOST` unset and the service never
opens a broker connection. When it is set, the service publishes a
**minimal presence** and nothing else: there is no telemetry loop, no
periodic publish, and none of the operational stats the receiver, message
processor, or archive processor emit. The client (its own paho network
loop, running alongside the UDP/eviction/healthcheck/range-outline
background threads) connects once and stays connected.

On every connect/reconnect it publishes, all **retained**:

| Topic | Payload | Purpose |
|---|---|---|
| `SkyFollower/map/status` | `ONLINE` | Liveness. A Last Will and Testament flips this to `OFFLINE` if the connection drops uncleanly; a clean shutdown publishes `OFFLINE` explicitly. |
| `SkyFollower/map/statistic/started_at` | UTC ISO-8601 timestamp | Process start time. |
| `SkyFollower/map/statistic/version` | Version string (`dev` if the image was built without a `VERSION`) | The running image version. |
| `homeassistant/sensor/SkyFollower_map_started_at/config` | HA MQTT discovery config | Registers a "SkyFollower Map" device (whose `sw_version` carries the running version) with a single Start Time sensor (`device_class: timestamp`). |

The running version is carried both in the discovery `device` block's
`sw_version` and as the plain `statistic/version` topic, so there is
deliberately no standalone version sensor entity -- the same choice the
receiver and archive processor make.

The **"update available"** entity for this service is not published here.
A separate health component polls GHCR for the latest released image tag,
compares it against the `statistic/version` value above, and publishes the
Home Assistant `update` entity on this service's behalf.

## UDP Listener

Receives `position`, `metadata`, and `heartbeat` JSON datagrams -- one
object per UDP packet, disambiguated by a `"type"` field -- from any/all
message-processor instances. No subscriber registration, no 1:1 pairing:
this service just listens on `MAP_LISTEN_HOST:MAP_LISTEN_PORT` and
processes whatever arrives from whoever sends it. See
message-processor/README.md's "Map UDP Publisher" section for the exact
payload shapes (`message-processor/main.py`'s `_publish_map_position`/
`_maybe_publish_map_metadata`/`_map_heartbeat_loop`).

All three message types carry a `processor_id` field (`MESSAGE_PROCESSOR_ID`
of the sender) -- see [Processor Roster](#processor-roster) below for what
this service does with it. `heartbeat` carries nothing else: it has no
`icao_hex` and never touches per-aircraft state at all.

The socket requests a larger-than-default kernel receive buffer
(`SO_RCVBUF`, best-effort -- the OS caps this at its own configured
maximum) so a brief processing stall doesn't cause the kernel to silently
drop datagrams that arrive in the meantime.

### Out-of-order protection

Each incoming packet carries the source ADS-B message's original
timestamp (`position`'s `ts` field directly; `metadata`'s
`last_message` field, since it has no dedicated `ts` key -- both
are stamped from the same `received_at` value for one source message, see
`_extract_timestamp()` in `map/main.py`). A packet strictly older than the
last one actually applied for that `icao_hex` is silently dropped, so a
late/reordered UDP packet can never visually "rewind" an aircraft's
displayed position.

**Equal timestamps are accepted, not dropped.** message-processor stamps a
`position` packet and a same-tick `metadata` packet (when metadata changes
on the very message that also updates position) from the *identical*
`received_at` value. Treating a tie as "at or before, so drop" -- a literal
reading of "drop any packet at or before the last-applied timestamp" --
would silently drop that metadata packet every time it coincides with a
position update for the same message, which is the common case for a
brand-new aircraft's first sighting. Only a packet strictly older than the
last-applied timestamp is treated as out-of-order.

This is a per-aircraft check on the packet's timestamp only -- it has no
bearing on which fields a packet may contain (see Partial Updates below).

### Partial updates -- merge, never overwrite

A real ADS-B message almost never carries position (lat/lon/altitude) and
velocity (groundspeed/heading/vertical_speed) data at the same time --
they come from physically different squitter types. `position` messages
routinely carry only a subset of `{lat, lon, alt, velocity, hdg, vs}`
(the wire protocol's deliberately abbreviated field names -- see
[Map UDP Publisher](../message-processor/README.md#map-udp-publisher) in
message-processor/README.md).

Every incoming field is merged into the aircraft's existing current-state
via a field-level Redis `HSET` on `flight:detail:{icao_hex}` -- never a
read-modify-write of a single serialized JSON blob, and never a wholesale
overwrite. A heading-only update does not blank out a previously known
position, and a position-only update does not blank out a previously known
heading; it's expected and normal for an aircraft's displayed position to
sit still while its heading updates on its own, or vice versa. `metadata`
fields merge into the same per-aircraft hash the same way.

The out-of-order check, the merge `HSET`, both TTL refreshes, and the
trail `RPUSH` (`position` packets only) are all performed server-side in
a single round trip by `EVALSHA` → `shared/lua/map_apply_update.lua`
(`SCRIPT LOAD`ed once at startup, matching message-processor's
`merge_aircraft.lua`/`route_airports.lua` convention), which also returns
the merged current-state so `FlightStateStore.apply_update()` never needs
a separate `HGETALL` to build the WebSocket event payload.

## Redis State

Dedicated Redis instance (`MAP_REDIS_*`), no persistence -- pure in-memory,
fully reconstructible from live UDP traffic (the range outline additionally
persists to disk -- see [Range Outline](#range-outline)). Four keys per
tracked aircraft, all TTL'd in seconds and refreshed on every UDP update
for that aircraft, plus two untracked-by-aircraft keys:

| Key | TTL | Contents |
|---|---|---|
| `flight:live:{icao_hex}` | `MAP_STALE_SECONDS` | Lightweight sentinel, no meaningful value. Expiry → `stale` |
| `flight:visible:{icao_hex}` | `MAP_HIDE_SECONDS` | Lightweight sentinel, no meaningful value. Expiry → `hide` |
| `flight:detail:{icao_hex}` | `MAP_EVICT_SECONDS` | A Redis **hash** holding the aircraft's actual merged current-state -- every known field from both `position` and `metadata` messages. This is what `GET /api/flights` and the WebSocket relay read from. Expiry → `remove` |
| `flight:trail:{icao_hex}` | `MAP_EVICT_SECONDS` | A Redis **list** of JSON `{lat, lon, alt}` snapshots, one `RPUSH` per accepted `position` update (once lat/lon are actually known), `LTRIM`med to the most recent `MAX_TRAIL_POINTS` (25,000 -- see [Trail History Caps](#trail-history-caps)) after each append. Refreshed onto the same TTL/lifecycle as `flight:detail` -- it lives and dies alongside the aircraft's detail record, independent of the stale/hide sentinels above. Served by `GET /api/flights/{icao_hex}` |
| `map:processors` | none | A Redis **hash** (field = `processor_id`, value = last-seen epoch timestamp) -- see [Processor Roster](#processor-roster) below |
| `map:range:outline` | 2 days (safety net only) | A Redis **hash** (field = `"{bearing_index}:{band}"`, `bearing_index` a half-degree index `0`-`719`, value = JSON `{nm, lat, lon, alt, ts}`) holding the current UTC day's reception range outline. The disk snapshots are the real store; this TTL only cleans up after a process that died without rolling over -- see [Range Outline](#range-outline) |

These key families are local to this service and are not part of
`shared/redis_keys.py`'s schema, which documents *core* Redis's keys --
this service's Redis is a second, separate instance it alone owns, so its
key namespace has no reason to be centralized alongside core's.

### Lifecycle

A tracked aircraft moves through three stages, each driven by one of the
TTL'd keys above expiring:

| Stage | Redis key | TTL | Event | Effect on the map |
|---|---|---|---|---|
| Stale | `flight:live:{icao_hex}` | `MAP_STALE_SECONDS` | `stale` | Icon + trail fade to grey |
| Hidden | `flight:visible:{icao_hex}` | `MAP_HIDE_SECONDS` | `hide` | Icon + trail removed from view; trail data retained |
| Evicted | `flight:detail:{icao_hex}` | `MAP_EVICT_SECONDS` | `remove` | Everything evicted (detail + trail) |

The hidden stage exists so a briefly-lost aircraft leaves the screen
quickly without losing its trail history: if contact resumes before
`MAP_EVICT_SECONDS`, the aircraft reappears (a `position`/`metadata` event
clears both `stale` and `hidden` client-side, see
`src/lib/aircraftState.ts`) with its pre-gap trail intact, rendered as one
continuous flight -- the gap itself renders as a normal trail segment, with
no special dashed/faded styling.

`GET /api/flights` only ever lists currently-*visible* aircraft (backed by
`flight:visible:{icao_hex}`, not `flight:detail:{icao_hex}` -- see
`map/state_store.py`'s `list_flights()`): a client connecting fresh during
a hidden gap has no client-accumulated trail for that aircraft either, so a
lone frozen icon with no trail would be worse than omitting it entirely. It
reappears for everyone the moment traffic resumes.

### Eviction

Driven entirely by Redis keyspace notifications (`notify-keyspace-events
Ex`, enabled by this service itself on every connect/reconnect -- it's a
runtime `CONFIG SET`, not persisted by this no-persistence Redis instance
across its own restart). A key's expiry *is* the signal: there is no
app-level timer loop scanning for expired aircraft. See
`map/state_store.py`'s `FlightStateStore.handle_expired_key()` and
`map/main.py`'s `_eviction_loop()`.

The `hide` path (`flight:visible:{icao_hex}` expiring) deliberately
touches nothing else -- `flight:detail`/`flight:trail` are left exactly as
they are. Only `flight:detail:{icao_hex}` expiring (`remove`) evicts data,
proactively deleting `flight:trail:{icao_hex}` -- it's refreshed onto the
same TTL on every update so it would expire on its own moments later
regardless, but this guarantees no leftover trail key can survive a
detail-key eviction even if the two TTLs ever drift apart.

### Trail History Caps

Two independent caps, deliberately not sharing one constant:

- **Trail-line history** -- `map/state_store.py`'s `MAX_TRAIL_POINTS`
  (server-side `flight:trail:{icao_hex}` `LTRIM` bound) and
  `frontend/src/lib/aircraftState.ts`'s own `MAX_TRAIL_POINTS` (client-
  accumulated trail, and the cap applied to the server trail once it's
  fetched as a page-reload/selection seed) -- both **25,000** points, kept
  in sync by hand since the two can't share a constant across the Python/
  TypeScript boundary. This is the history drawn as the aircraft's trail
  line on the map.
- **Trace Points sample buffer** -- `aircraftState.ts`'s `MAX_TRACE_POINTS`
  (**300**), capping only the Aircraft Detail Panel's Trace Points feature
  (one labeled dot per sample, with speed/altitude/local-time). Server-side
  has no equivalent at all -- Trace Points is client-accumulated only.

Both were previously one shared `MAX_TRAIL_POINTS = 300`. Splitting them
reflects that they trade off very differently:

- Trace Points renders a labeled dot (plus a text label) per sample, not a
  thin line segment, so it stays cheap to look at only while the sample
  count stays small -- 300 is still generous for spot-checking a flight's
  recent history and was never the actual bottleneck this cap split was
  about, so it's left where it was.
- The trail line is one two-point `LineString` feature per consecutive
  point pair (`frontend/src/lib/featureCollections.ts`, `trailFeatureCollection`
  / `buildTrailSegments`), rebuilt for the entire tracked-aircraft set on
  every position/metadata update and coalesced to at most once per 200ms
  by `frontend/src/lib/syncThrottle.ts` -- that coalescing bounds *how
  often* the rebuild runs, not *how large* each rebuild is, so the cap on
  a single aircraft's point count still matters even with the throttle in
  place.

25,000 was chosen, rather than removing the trail-line cap outright, by
weighing both costs it's meant to bound:

- **Redis memory.** Each trail point is a small JSON object
  (`{"lat", "lon", "alt"}`), well under 100 bytes as a Redis list element
  including per-entry list overhead. 25,000 of them is on the order of
  2-3MB for a single aircraft that actually reaches the cap -- trivial for
  `map-redis`'s no-persistence, in-memory-only footprint (see
  [Configuration](#configuration) above and `docker-compose.map.yaml`)
  even with several such aircraft airborne at once, at this service's
  documented "a few dozen aircraft" design scale. No `maxmemory` is
  configured on `map-redis` today, so there's no hard ceiling this could
  collide with either way.
- **Rendering cost.** The message processor enforces a floor of one
  `position` UDP packet per aircraft per second
  (`shared/timing.py`'s `DEFAULT_MAP_UDP_MIN_POSITION_INTERVAL_SECONDS`),
  so 25,000 points is a worst case of roughly 7 hours of uninterrupted
  max-rate tracking for one aircraft -- comfortably past a typical domestic
  flight, and past most international ones too, since a ground-based
  ADS-B/EXTERNAL-feed network rarely holds uninterrupted contact with one
  aircraft much longer than that (coverage gaps over open ocean/remote
  terrain are the norm, unlike a satellite-fed source). A single aircraft's
  worst-case segment count at this cap (24,999) is the same order of
  magnitude as the full-fleet worst case already reasoned tolerable under
  the sync throttle (roughly 50 aircraft x the old 300-point cap =~ 14,950
  segments) -- a genuinely unbounded per-aircraft trail would let one
  long-haul flight alone exceed that by an arbitrary, unbounded factor
  instead.

A true ultra-long-haul flight tracked gapless for longer than ~7 hours is
the one case that still trims its oldest history under this cap -- accepted
as a deliberate tradeoff rather than removing the cap outright, per the
reasoning above.

## Processor Roster

Which message processors are alive and feeding data, shown as a
per-processor green/amber/red status and rolled up into the frontend's
overall connection indicator (see [Frontend](#frontend-frontend) below).

**Derived from any UDP message carrying `processor_id`, not just
`heartbeat`.** `_handle_packet` records `map:processors[processor_id] =
now` (this service's own receipt time, never the sender's clock, so
cross-host clock skew can't distort the thresholds) on *every* incoming
`heartbeat`/`position`/`metadata` datagram, before any type-specific
handling. This is why a busy processor's ordinary position/metadata
traffic almost never needs message-processor's dedicated 5s `heartbeat`
loop to actually fire -- see message-processor/README.md's "Map UDP
Publisher" section.

**Roster scope: since this map service's own Redis last reset, not
since the aircraft started transmitting.** `map:processors` carries no
TTL, unlike the three aircraft-scoped keys above -- a processor that goes
silent is meant to sit in the roster as permanently red (so an operator
notices it), not quietly disappear the way a completed flight does.
Because the dedicated Redis instance has no persistence (see [Fault
Tolerance](#fault-tolerance) below), the roster resets to empty exactly
when the rest of this service's state does: a restart of the map
service's Redis. This is deliberate, and gives an operator a concrete
lever: to retire a decommissioned processor's entry, restart the map
stack (map + its Redis) and it drops off (the connection indicator goes
back to all-green-by-omission) rather than sticking as a permanent red
forever. **A processor never seen this session that starts sending
mid-run joins the roster silently -- no special UI notice.**

**Status thresholds** (`shared/timing.py`'s
`MAP_PROCESSOR_GREEN_MAX_AGE_SECONDS`/`MAP_PROCESSOR_AMBER_MAX_AGE_SECONDS`,
not environment variables -- same convention as `MAP_WS_BATCH_INTERVAL_SECONDS`
above), applied to `now - last_seen`:

| Status | Age | Meaning |
|---|---|---|
| green | ≤ 15s | "Connected" -- at most three missed 5s heartbeat ticks |
| amber | 15-60s | "Reconnecting" |
| red | > 60s, or never seen | "Disconnected" -- twelve missed heartbeat ticks |

**Overall indicator aggregation** (`map/state_store.py`'s
`overall_processor_status`): green only when every rostered processor is
green; red only when every rostered processor is red (an empty roster --
nothing has ever been received -- counts as red too); amber otherwise (at
least one green, at least one amber/red).

## REST API

`GET /api/flights` -- a JSON list, one object per currently-*visible*
aircraft (i.e. one per `flight:visible:{icao_hex}` sentinel that currently
exists -- see [Lifecycle](#lifecycle) above; a hidden-but-not-yet-evicted
aircraft is omitted). Each object is the same shape a WebSocket `metadata`
message carries: the full merged current-state, combining both position
fields and metadata fields into one object per aircraft, not two separate
lists. `FlightStateStore.list_flights()` finds the visible aircraft with
`SCAN` over `flight:visible:*`, then issues every aircraft's `flight:detail`
`HGETALL` in a single pipeline -- one round trip regardless of aircraft
count, not one `HGETALL` per aircraft.

`GET /api/flights/{icao_hex}` -- one aircraft's merged current-state (same
per-aircraft shape as `GET /api/flights`) plus a `trail` array: every
accumulated `{lat, lon, alt}` point for the current flight, oldest first,
`alt` `null` where it wasn't known when the point was recorded. This is the
map service's *own* server-side trail (`flight:trail:{icao_hex}`, one point
per accepted `position` packet, capped at the most recent
`MAX_TRAIL_POINTS` -- 25,000, see [Trail History Caps](#trail-history-caps)
-- and lifecycled exactly like `flight:detail`, see
[Lifecycle](#lifecycle)). The frontend fetches this when an aircraft is
selected so the drawn trail covers the whole flight, not just what that
browser has seen since it connected -- and so it survives a page reload.
`HTTP 404` when the aircraft isn't currently tracked (never seen, or
evicted past `MAP_EVICT_SECONDS` of silence); `trail` is `[]` when the
aircraft is known but has only ever sent velocity/heading-only position
packets.

```json
{
  "icao_hex": "A8AE7F",
  "lat": 33.94, "lon": -118.41, "alt": 8600, "hdg": 271,
  "ident": "SWA1234",
  "trail": [
    { "lat": 33.90, "lon": -118.30, "alt": 6000 },
    { "lat": 33.92, "lon": -118.36, "alt": 7300 },
    { "lat": 33.94, "lon": -118.41, "alt": 8600 }
  ]
}
```

`GET /api/processors` -- the message-processor liveness roster (see
[Processor Roster](#processor-roster) above), computed fresh on every
request rather than pushed over `WS /ws`: a processor's status can change
purely from time passing (green ageing into amber/red) with no new packet
to trigger a push, so the frontend polls this instead (see
`src/hooks/useProcessorRoster.ts` under [Frontend](#frontend-frontend)
below).

```json
{
  "overall": "amber",
  "processors": [
    { "processor_id": "mp-1", "last_seen": 1725720000.0, "status": "green" },
    { "processor_id": "mp-2", "last_seen": 1725719920.0, "status": "red" }
  ]
}
```

`processors` is sorted by `processor_id`, not recency, for a stable
response shape; a processor never seen this session is simply absent (no
placeholder entry).

`GET /api/config` -- runtime configuration the frontend can't otherwise
get at, since Vite bakes `VITE_*` values into the bundle at `npm run
build` time and the published image is built with none of them set. A
flat top-level object with named sub-keys, so a later addition doesn't
need a breaking shape change:

```json
{ "center": { "latitude": 33.9425, "longitude": -118.4081 } }
```

or `{ "center": null }` when `MAP_CENTER_LATITUDE`/`MAP_CENTER_LONGITUDE` are
unset. See [Configuration](#configuration) above and `src/lib/config.ts`
under [Frontend](#frontend-frontend) below.

## Range Outline

The map service builds a **daily reception range outline** -- "how far can
this whole system hear, per compass bearing, per altitude band" -- from
the same `position` UDP stream the live map runs on. It buckets at 720
half-degree bearing resolution -- finer than readsb's own
actual-range-outline (360 one-degree bearing buckets, per-bucket farthest
received position), deliberately, since this runs centrally and aggregates
*every* receiver and external feed, not one antenna. Requires a configured
center (`MAP_CENTER_LATITUDE`/`LONGITUDE`) -- there's no origin to measure
from otherwise, and the endpoint returns an empty `FeatureCollection`.

**Accumulation.** For each accepted `position`, the service computes the
great-circle bearing and distance from center and, if that distance beats
the farthest yet seen in that `{bearing, altitude-band}` bucket, records
the actual aircraft coordinate. Buckets live in one Redis hash
(`map:range:outline`, `map/range_outline.py`). Altitude bands:
`0-2000`, `2000-5000`, `5000-10000`, `10000-20000`, `20000-30000`,
`30000-40000`, `40000+` feet. An outlier guard (the map feed carries no
CPR reliability flags) drops anything past `325` nm, and drops a jump more
than `50` nm beyond a bucket's current maximum unless a bearing bucket
within 3° already reaches close to that distance -- the first detection
into an empty bucket is always taken.

**Daily lifecycle.** The outline accumulates from empty at 00:00 UTC. It
carries its UTC date; the first `position` after midnight (or a 1/min
tick, for a quiet midnight) finalises the finished day's snapshot, clears
the Redis hash, and starts the new day empty.

**Disk snapshots.** `./data/map/range-outline/{YYYY-MM-DD}.json` -- raw
bucket data as JSON (not GeoJSON; the API builds GeoJSON on read so the
banding/envelope logic can change without rewriting old files). Written
once a minute when the outline has changed and once at the rollover.
Host-persisted (unlike `map-redis` itself), so the 30-day history and the
in-progress day survive a container restart. On boot the service reloads
`{today}.json` if present (a mid-day crash); a reboot that spanned
midnight finds no `{today}.json`, starts the new day empty, and leaves
`{yesterday}.json` as its record. Files older than
`MAP_RANGE_OUTLINE_TTL_SECONDS` (30 days) are deleted on each write. If
`map-redis` restarts while this process stays up, the next 1/min tick
reloads the current day from disk.

Snapshots written before the bearing bucket resolution was doubled from
360 (whole-degree) to 720 (half-degree) buckets store bucket keys under
the old integer-degree scheme. A syntactically valid old key like `"47"`
is silently reinterpreted under the new scheme as half-degree index 47
(23.5°), not 47°. This is a known, accepted limitation rather than
something migrated: a historical `?date=` lookup against a pre-upgrade
snapshot may render with shifted bearings for the remainder of that
snapshot's 30-day retention window, after which it's deleted and the
issue disappears on its own. The live in-progress day is unaffected --
it clears and rebuilds fresh at every UTC rollover regardless.

**API.**

`GET /api/range-outline` -- a GeoJSON `FeatureCollection`: one 3-D
`Polygon` Feature per altitude band that has at least three bearing points
(vertices `[lon, lat, alt_ft]`, closed ring, ascending bearing), plus an
`envelope` Feature (farthest per bearing across all bands). Top-level
`properties`: `date`, `generated_at`, `point_count`, `max_range_nm`,
`center`. No `date` parameter -> today's live outline; `date=YYYY-MM-DD` ->
that day's finalised snapshot (`HTTP 404` past the retention window,
`HTTP 400` for a malformed date). `band=<label>` narrows to one band;
`band=envelope` returns only the envelope.

`GET /api/range-outline/dates` -- `{"dates": ["today", "2026-09-10", …]}`,
newest first.

`DELETE /api/range-outline` -- reset the in-progress day (drops the Redis
hash and today's snapshot file). Finalised past days are untouched.

## WebSocket API

`WS /ws` -- one connection per browser. Carries `position`, `metadata`,
`stale`, `hide`, and `remove` messages only; **never a snapshot** (that's
`GET /api/flights`'s job -- a client is expected to call it once on
connect, then open the WebSocket for live updates).

| Type | When | Payload |
|---|---|---|
| `position` | A `position` UDP packet was applied | The aircraft's current merged position-relevant fields only (`icao_hex` plus whichever of `lat`/`lon`/`alt`/`velocity`/`hdg`/`vs` are currently known) -- a field the aircraft has never reported is omitted, never sent as `null` |
| `metadata` | A `metadata` UDP packet was applied | The **full** merged current-state -- both position and metadata fields -- matching `GET /api/flights`' per-aircraft shape exactly. A client that only just connected (and so missed any earlier `position` events) still has everything needed to place and label the aircraft the first time it hears about it |
| `stale` | `flight:live:{icao_hex}` expired | `{"type": "stale", "icao_hex": ...}` -- fade signal |
| `hide` | `flight:visible:{icao_hex}` expired | `{"type": "hide", "icao_hex": ...}` -- drop-from-view signal. The client must keep the aircraft's record and trail (see `src/lib/aircraftState.ts`), only omitting it from what's drawn, so a resumed flight bridges the gap as one continuous trail |
| `remove` | `flight:detail:{icao_hex}` expired | `{"type": "remove", "icao_hex": ...}` -- hard-delete signal. The service tells the browser to delete rather than the browser inferring eviction from its own timers |

### Batching

Events arriving within a `MAP_WS_BATCH_INTERVAL_SECONDS` (~250ms) window
are batched into a single WebSocket frame per connected browser -- a JSON
array of event objects -- rather than one frame per update. See
`map/broadcaster.py`'s `ConnectionManager`.

### Compression

`permessage-deflate` is negotiated automatically whenever a connecting
client offers it. This requires no extra code: `map/main.py`'s
`uvicorn.run(...)` call sets no `ws`/`ws_per_message_deflate` kwargs at
all, so it rides uvicorn's own default -- `ws_per_message_deflate=True` --
which wraps every WebSocket connection in a `ServerPerMessageDeflateFactory`
extension unconditionally, regardless of which of uvicorn's WebSocket
implementations `ws="auto"` happens to select (currently
`websockets-sansio` for the pinned `uvicorn`/`websockets` versions in
`map/requirements.txt` -- both it and the older `websockets` implementation
read the same `ws_per_message_deflate` config flag). Confirmed directly
against those pinned versions: a real WebSocket handshake against this
service's `/ws` endpoint returns an HTTP 101 response whose
`Sec-WebSocket-Extensions` header includes `permessage-deflate`.

**Verifying this**: don't trust browser DevTools' Network/WS frame
inspector -- it shows the *decompressed* payload regardless of whether
`permessage-deflate` was actually negotiated on the wire, since the
browser's own WebSocket implementation transparently decompresses before
handing frames to DevTools. To actually confirm compression is
negotiating, inspect the HTTP 101 upgrade response's headers directly --
e.g. in a browser, DevTools' Network panel still shows the raw response
*headers* for the `/ws` request (separate from the frame inspector) and
one of them will be `Sec-WebSocket-Extensions: permessage-deflate; ...`;
or from the command line/a script, open a raw WebSocket handshake (e.g.
Python's `websockets` client) and read `.response.headers` for the same
header.

## Fault Tolerance

This Redis instance has no persistence -- killing and restarting the map
service (or its Redis) loses all state, and the state rebuilds correctly
from live UDP traffic within one `MAP_EVICT_SECONDS` window. There is no
backup file and no migration path, matching the design intent: this
service caches, it does not store.

A slow, unreachable, or misconfigured `MAP_REDIS_HOST` blocks this
service's own startup (unlike message-processor's fire-and-forget UDP
*send* path, which never blocks on a down destination) -- Redis is this
service's only state store, so there's nothing useful to do without it.
The eviction listener reconnects (re-enabling keyspace notifications each
time) on any pubsub error rather than exiting.

## Frontend (`frontend/`)

A standalone React (TypeScript) + Vite + Tailwind CSS + MapLibre GL JS
project -- its own `package.json`, not a view inside `management-ui/frontend`
(that frontend has its own separate backend/purpose; this one exists purely
to render this service's live feed). It sits alongside this directory's
Python backend the same way `management-ui/frontend` sits alongside
`management-ui/backend`, but unlike that pairing (frontend built into an
nginx html root, served on its own port), this frontend is built and
served by the `map` image itself: `map/Dockerfile`'s `frontend-build`
stage runs `npm ci && npm run build`, and the resulting `frontend/dist/`
is copied into the final Python-stage image, where `map/main.py` mounts it
at `/map` (FastAPI/Starlette `StaticFiles`, with SPA-fallback routing so a
deep link/refresh under `/map/*` doesn't 404). `GET /api/flights` and
`WS /ws` stay unprefixed -- `/map` is only the SPA's own mount point.
Vite's `base: '/map/'` (`vite.config.ts`) keeps the built bundle's own
asset URLs rooted at that same sub-path.

Full-viewport live map, no persistent side panel: a symbol layer for
tracked aircraft (a **type-specific top-down silhouette** per aircraft --
see [Aircraft silhouettes](#aircraft-silhouettes) below; rotates via
`icon-rotate` bound to `heading`; an SDF icon so its fill can be recolored
per feature via `icon-color`, driven by altitude -- ported from
`management-ui/frontend/src/lib/flightView.ts`'s `altitudeColor()`), an
ATC-style floating info box per aircraft (hidden by
default, shown for a selected or hovered aircraft, or for every aircraft
via the "Labels: All" toggle -- boxes overlap freely with no leader lines,
stacked by altitude so a higher-altitude aircraft's box always draws on
top of a cluster), a live trail per aircraft (built client-side from
`position` events as they arrive; when an aircraft is *selected* the
frontend also fetches `GET /api/flights/{icao_hex}` and reseeds that
aircraft's trail from the server's own accumulation, so a selected
aircraft's trail covers the whole flight and survives a page reload -- see
[REST API](#rest-api) above; both the trail line and the Aircraft Detail
Panel's separate Trace Points buffer are capped independently, see [Trail
History Caps](#trail-history-caps) above), a fixed "center" marker/recenter button from
the backend's `GET /api/config` (see [REST API](#rest-api) above), and a
"Range Outline" toggle that draws today's reception range outline (the
`envelope` band from `GET /api/range-outline`, see [Range
Outline](#range-outline) above) as a `#196363` line, twice the width of the
static range rings -- disabled when no center is configured, since the
backend has no origin to measure from in that case either.

- `src/lib/altitudeColor.ts` -- verbatim port of `flightView.ts`'s
  `altitudeColor()`; used for both the icon fill and the live trail color.
- `src/lib/aircraftIcon.ts` -- fills one silhouette path to an SDF
  `ImageData` for `map.addImage(..., { sdf: true })`.
- `src/lib/aircraftIconResolver.ts` -- picks a silhouette for an aircraft:
  exact ICAO type designator, then an alias table for designators with no
  dedicated art, then the ICAO Doc 8643 `description_code` (± wake
  turbulence category), then `UNIDENTIFIED`.
- `src/lib/aircraftShapes.generated.ts` -- **generated** (gitignored) by
  `scripts/generate-aircraft-shapes.mjs` on every `predev`/`prebuild` from
  the vendored SVGs; the outline path + bounding box + relative size per
  shape. See [Aircraft silhouettes](#aircraft-silhouettes).
- `src/lib/infoBox.ts` -- info-box field formatting/omission rules (ident,
  altitude+trend-arrow+groundspeed, registration+type), each independently
  omitted (never a `?`/`N/A` placeholder) when its underlying field is
  unknown.
- `src/lib/labelStackOrder.ts` -- maps altitude to a bounded z-index so
  overlapping info boxes stack with the higher-altitude aircraft on top;
  unknown-altitude aircraft sort to the bottom, ties broken by icao_hex.
- `src/lib/aircraftState.ts` -- client-side per-aircraft state: applies the
  REST snapshot, then every WebSocket event, with the same
  merge-never-overwrite semantics as `state_store.py`'s `apply_update`. A
  `hide` event sets a `hidden` flag without deleting the record or its
  trail; any `position`/`metadata` event clears both `stale` and `hidden`.
- `src/lib/featureCollections.ts` -- builds the MapLibre aircraft-icon and
  per-segment trail GeoJSON feature collections from the live `AircraftMap`,
  filtering out `hidden` aircraft from both.
- `src/hooks/useMapFlights.ts` -- owns the WebSocket connection + REST
  snapshot fetch, deliberately sequenced: the WebSocket connects *first*
  (buffering whatever arrives), then `GET /api/flights` is called, then
  every buffered WS message is applied on top of that snapshot before the
  first `aircraft` state is ever published -- closing the gap between
  "snapshot fetched" and "WS live" a snapshot-then-connect order would
  leave open.
- `src/hooks/useProcessorRoster.ts` -- polls `GET /api/processors` every
  5s (see [Processor Roster](#processor-roster) above for why this must be
  a poll, not a WS push) for `ControlsPanel`'s connection indicator.
- `src/api/rangeOutline.ts` / `src/hooks/useRangeOutline.ts` -- fetches
  `GET /api/range-outline?band=envelope`; the hook polls every 60s (matching
  the backend's own `MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS`,
  `shared/timing.py`) only while the "Range Outline" toggle is on, and
  reports an empty `FeatureCollection` (no polling) while it's off.
- `src/lib/processorStatus.ts` -- the connection indicator's presentation
  logic: overall color (red whenever the WebSocket itself is down,
  regardless of the last-known roster) and the per-processor hover
  tooltip text. Kept out of `ControlsPanel.tsx` so it's unit-testable
  without a DOM-rendering dependency, same convention as
  `labelStackOrder.ts`/`infoBox.ts`.
- `src/components/MapView.tsx` -- map construction, aircraft/trail
  sources+layers (via `src/lib/featureCollections.ts`),
  click-to-toggle-trail, and the info-box overlay.
- `src/components/ControlsPanel.tsx` -- the top-right status
  panel/toggles/recenter button. The connection dot is green/amber/red
  (amber = "at least one processor reconnecting/disconnected, but at
  least one still connected"), and hovering it lists every rostered
  processor by `processor_id` with its own status label. Its "Range
  Outline" toggle is disabled whenever no center is configured, matching
  the recenter button's own disabled state.

```bash
cd map/frontend
npm install
npm run dev       # Vite dev server on :5173, serving the app under /map/ (base: '/map/'
                   # in vite.config.ts, matching production) and proxying /api and /ws
                   # to localhost:8080 -- run the dev backend with MAP_HTTP_PORT=8080
                   # (plain HTTP, unprivileged), not the production 443 default
npm run build     # type-checks (tsc -b) then builds the static bundle to dist/
npm test          # vitest -- aircraftState/featureCollections lifecycle rules, altitudeColor,
                   # info-box formatting, label stack-order, icon-shape resolution, processor status
```

Every one of the four scripts above (`dev`, `build`, `typecheck`, `test`)
first runs `generate:shapes` -- see below.

### Aircraft silhouettes

Each aircraft draws as a top-down silhouette of its actual type rather than
one generic marker.

- **Source art:** `src/assets/aircraft-shapes/*.svg` -- 181 SVGs vendored
  from [RexKramer1/AircraftShapesSVG](https://github.com/RexKramer1/AircraftShapesSVG)
  (GPL-3.0; see that directory's `LICENSE`, the repo's
  `THIRD-PARTY-NOTICES.md`, and the on-map attribution control). Filenames
  are ICAO type designators (`A320.svg`, `H60.svg`); four swing-wing
  variants have a hyphen where the source had a space (`B1-slow.svg`).
- **Build step:** `scripts/generate-aircraft-shapes.mjs` (run by the
  `predev`/`prebuild`/`pretypecheck`/`pretest` npm hooks) parses each SVG's
  outer outline path, rounds its coordinates, computes its bounding box,
  and derives a clamped relative size from the drawn dimensions (the art is
  drawn to a consistent real-world scale in an 80-unit box). Output:
  `src/lib/aircraftShapes.generated.ts` -- gitignored, the vendored SVGs
  are the source of truth.
- **Resolver:** `src/lib/aircraftIconResolver.ts` maps an aircraft's
  `type_designator` / `description_code` / `wake_turbulence_category` (all
  present in the `metadata` payload) to a shape key, falling back to
  `UNIDENTIFIED`. Non-enriched aircraft with no type at all get the
  fallback -- forwarding the raw ADS-B emitter category for a better guess
  there is a separate enhancement.
- **Rendering:** `MapView.tsx` registers each shape's SDF image with
  MapLibre lazily, the first time an aircraft needs it. The symbol layer's
  `icon-image` is `["concat", "sf-ac-", ["get", "shape"]]`; `icon-color`
  (altitude), `icon-halo` (selection) and `icon-opacity` (stale) are
  unchanged from the single-icon version; `icon-size` multiplies a base by
  the shape's relative size.

### Frontend Configuration

`src/lib/config.ts`'s `loadConfig()` fetches `GET /api/config` (see
[REST API](#rest-api) above) once at page load for the "center" reference
point -- a runtime value read from this service's own `MAP_CENTER_LATITUDE`/
`MAP_CENTER_LONGITUDE` (see [Configuration](#configuration) above), not a
Vite build-time value. Changing it on the backend takes effect on the next
page load; no frontend rebuild required.

`VITE_MAP_API_BASE_URL` is still Vite build-time only (baked into the
bundle at `npm run build` time -- see `.env.example`):

| Variable | Required | Default | Description |
|---|---|---|---|
| `VITE_MAP_API_BASE_URL` | ❌ | same-origin | Base URL of this service's REST/WebSocket API. Leave unset when the built frontend is served from the same host:port as this service |

`VITE_CENTER_LATITUDE`/`VITE_CENTER_LONGITUDE` still exist, but only as an
`npm run dev` fallback (Vite's dev server has no backend at the same
origin to serve `GET /api/config` unless a proxy is set up) -- see
`.env.example`. They have no effect on a production build or the
published image.
