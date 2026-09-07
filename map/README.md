# Map Service

Backend for the live-map frontend (`frontend/`, see [Frontend](#frontend-frontend)
below). Three jobs in one process:

1. A UDP listener that receives `position`/`metadata` datagrams from any/all
   `message-processor` instances (see
   [message-processor/README.md](../message-processor/README.md)'s "Map UDP
   Publisher" section for the wire format) and merges each into per-aircraft
   current-state held in a dedicated Redis instance.
2. A Redis keyspace-notification listener that turns key expiry into
   `stale`/`remove` WebSocket events -- eviction is driven entirely by that
   signal, with no app-level timer loop scanning for expired aircraft.
3. A FastAPI app exposing `GET /api/flights` (a snapshot) and `WS /ws` (a
   live, batched relay of `position`/`metadata`/`stale`/`remove` events),
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

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("map_redis", "map")`, interpolated by Compose from this host's
`.env`.

| Variable | Required | Default | Description |
|---|---|---|---|
| `MAP_LISTEN_HOST` | ❌ | `0.0.0.0` | Bind address for the UDP listener |
| `MAP_LISTEN_PORT` | ✅ | — | Bind port for the UDP listener. Must match whatever port a message processor's `MAP_UDP_PORT` sends datagrams to -- see [Deliberately distinct variable names](#deliberately-distinct-variable-names) below |
| `MAP_HTTP_HOST` | ❌ | `0.0.0.0` | Bind address for the REST/WebSocket API |
| `MAP_HTTP_PORT` | ❌ | `80` | Bind port for the REST/WebSocket API |
| `MAP_REDIS_HOST` | ✅ | — | Dedicated Redis instance for this service's own live aircraft state -- **not** core Redis (see the repo root docs' Redis Key Schema for core's schema; this service never reads or writes any of those keys) |
| `MAP_REDIS_PORT` | ❌ | `6379` | |
| `MAP_REDIS_PASSWORD` | ❌ | — | Optional, unlike core's `REDIS_PASSWORD` -- see [Why `MAP_REDIS_PASSWORD` is optional](#why-map_redis_password-is-optional) below |
| `MAP_STALE_SECONDS` | ❌ | `30` | TTL on `flight:live:{icao_hex}`; expiry fades an aircraft client-side (a `stale` WebSocket event) without removing it |
| `MAP_EVICT_SECONDS` | ❌ | `300` | TTL on `flight:detail:{icao_hex}` (and its `flight:trail:{icao_hex}`); expiry hard-removes the aircraft (a `remove` WebSocket event). Should stay clearly longer than `MAP_STALE_SECONDS` |
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

## UDP Listener

Receives `position` and `metadata` JSON datagrams -- one object per UDP
packet, disambiguated by a `"type"` field -- from any/all message-processor
instances. No subscriber registration, no 1:1 pairing: this service just
listens on `MAP_LISTEN_HOST:MAP_LISTEN_PORT` and processes whatever arrives
from whoever sends it. See message-processor/README.md's "Map UDP
Publisher" section for the exact payload shapes
(`message-processor/main.py`'s `_publish_map_position`/
`_maybe_publish_map_metadata`).

### Out-of-order protection

Each incoming packet carries the source ADS-B message's original
timestamp (`position`'s `timestamp` field directly; `metadata`'s
`last_message` field, since it has no dedicated `timestamp` key -- both
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
routinely carry only a subset of `{latitude, longitude, altitude, velocity,
heading, vertical_speed}`.

Every incoming field is merged into the aircraft's existing current-state
via a field-level Redis `HSET` on `flight:detail:{icao_hex}` -- never a
read-modify-write of a single serialized JSON blob, and never a wholesale
overwrite. A heading-only update does not blank out a previously known
position, and a position-only update does not blank out a previously known
heading; it's expected and normal for an aircraft's displayed position to
sit still while its heading updates on its own, or vice versa. `metadata`
fields merge into the same per-aircraft hash the same way.

## Redis State

Dedicated Redis instance (`MAP_REDIS_*`), no persistence -- pure in-memory,
fully reconstructible from live UDP traffic. Three keys per tracked
aircraft, all TTL'd in seconds and refreshed on every UDP update for that
aircraft:

| Key | TTL | Contents |
|---|---|---|
| `flight:live:{icao_hex}` | `MAP_STALE_SECONDS` | Lightweight sentinel, no meaningful value. Expiry → `stale` |
| `flight:detail:{icao_hex}` | `MAP_EVICT_SECONDS` | A Redis **hash** holding the aircraft's actual merged current-state -- every known field from both `position` and `metadata` messages. This is what `GET /api/flights` and the WebSocket relay read from. Expiry → `remove` |
| `flight:trail:{icao_hex}` | `MAP_EVICT_SECONDS` | A Redis **list** of JSON `{latitude, longitude, altitude}` snapshots, one `RPUSH` per accepted `position` update (once latitude/longitude are actually known), no length/point cap. Refreshed onto the same TTL/lifecycle as `flight:detail` -- it lives and dies alongside the aircraft's detail record |

These three key families are local to this service and are not part of
`shared/redis_keys.py`'s schema, which documents *core* Redis's keys --
this service's Redis is a second, separate instance it alone owns, so its
key namespace has no reason to be centralized alongside core's.

### Eviction

Driven entirely by Redis keyspace notifications (`notify-keyspace-events
Ex`, enabled by this service itself on every connect/reconnect -- it's a
runtime `CONFIG SET`, not persisted by this no-persistence Redis instance
across its own restart). A key's expiry *is* the signal: there is no
app-level timer loop scanning for expired aircraft. See
`map/state_store.py`'s `FlightStateStore.handle_expired_key()` and
`map/main.py`'s `_eviction_loop()`.

`flight:detail:{icao_hex}` expiring (`remove`) also proactively deletes
`flight:trail:{icao_hex}` -- it's refreshed onto the same TTL on every
update so it would expire on its own moments later regardless, but this
guarantees no leftover trail key can survive a detail-key eviction even if
the two TTLs ever drift apart.

## REST API

`GET /api/flights` -- a JSON list, one object per currently-tracked
aircraft (i.e. one per `flight:detail:{icao_hex}` hash that currently
exists). Each object is the same shape a WebSocket `metadata` message
carries: the full merged current-state, combining both position fields and
metadata fields into one object per aircraft, not two separate lists.

## WebSocket API

`WS /ws` -- one connection per browser. Carries `position`, `metadata`,
`stale`, and `remove` messages only; **never a snapshot** (that's
`GET /api/flights`'s job -- a client is expected to call it once on
connect, then open the WebSocket for live updates).

| Type | When | Payload |
|---|---|---|
| `position` | A `position` UDP packet was applied | The aircraft's current merged position-relevant fields only (`icao_hex` plus whichever of `latitude`/`longitude`/`altitude`/`velocity`/`heading`/`vertical_speed` are currently known) -- a field the aircraft has never reported is omitted, never sent as `null` |
| `metadata` | A `metadata` UDP packet was applied | The **full** merged current-state -- both position and metadata fields -- matching `GET /api/flights`' per-aircraft shape exactly. A client that only just connected (and so missed any earlier `position` events) still has everything needed to place and label the aircraft the first time it hears about it |
| `stale` | `flight:live:{icao_hex}` expired | `{"type": "stale", "icao_hex": ...}` -- fade signal |
| `remove` | `flight:detail:{icao_hex}` expired | `{"type": "remove", "icao_hex": ...}` -- hard-delete signal. The service tells the browser to delete rather than the browser inferring eviction from its own timers |

### Batching

Events arriving within a `MAP_WS_BATCH_INTERVAL_SECONDS` (~250ms) window
are batched into a single WebSocket frame per connected browser -- a JSON
array of event objects -- rather than one frame per update. See
`map/broadcaster.py`'s `ConnectionManager`.

### Compression

`permessage-deflate` is negotiated automatically whenever a connecting
client offers it. This requires no extra code: uvicorn's `websockets`
ASGI implementation (selected automatically once the `websockets` package
is installed -- see `map/requirements.txt`) defaults its
`ws_per_message_deflate` config option to `True`, wrapping every
WebSocket connection in a `ServerPerMessageDeflateFactory` extension
unconditionally.

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
tracked aircraft (icon rotates via `icon-rotate` bound to `heading`; an SDF
icon so its fill can be recolored per feature via `icon-color`, driven by
altitude -- ported from `management-ui/frontend/src/lib/flightView.ts`'s
`altitudeColor()`), an ATC-style floating info box per aircraft (custom
collision/nudge placement so every box stays visible, never MapLibre's
`text-allow-overlap: false` collision-hiding), a client-accumulated live
trail per aircraft (this service has no trail/history endpoint -- see REST
API above -- so the frontend builds each aircraft's trail itself, purely
from `position` events observed after the page loaded), and a fixed "home"
marker/recenter button from build-time config.

- `src/lib/altitudeColor.ts` -- verbatim port of `flightView.ts`'s
  `altitudeColor()`; used for both the icon fill and the live trail color.
- `src/lib/aircraftIcon.ts` -- the custom-authored top-down dart/chevron
  aircraft silhouette, rendered as an SDF image.
- `src/lib/infoBox.ts` -- info-box field formatting/omission rules (ident,
  altitude+trend-arrow+groundspeed, registration+type), each independently
  omitted (never a `?`/`N/A` placeholder) when its underlying field is
  unknown.
- `src/lib/placement.ts` -- the info-box overlap/nudge placement algorithm.
- `src/lib/aircraftState.ts` -- client-side per-aircraft state: applies the
  REST snapshot, then every WebSocket event, with the same
  merge-never-overwrite semantics as `state_store.py`'s `apply_update`.
- `src/hooks/useMapFlights.ts` -- owns the WebSocket connection + REST
  snapshot fetch, deliberately sequenced: the WebSocket connects *first*
  (buffering whatever arrives), then `GET /api/flights` is called, then
  every buffered WS message is applied on top of that snapshot before the
  first `aircraft` state is ever published -- closing the gap between
  "snapshot fetched" and "WS live" a snapshot-then-connect order would
  leave open.
- `src/components/MapView.tsx` -- map construction, aircraft/trail
  sources+layers, click-to-toggle-trail, and the info-box overlay.

```bash
cd map/frontend
npm install
npm run dev       # Vite dev server on :5173, serving the app under /map/ (base: '/map/'
                   # in vite.config.ts, matching production) and proxying /api and /ws
                   # to localhost:80
npm run build     # type-checks (tsc -b) then builds the static bundle to dist/
npm test          # vitest -- altitudeColor, info-box formatting, and overlap-placement unit tests
```

### Frontend Configuration

Build-time only (Vite's `import.meta.env.VITE_*`, baked into the bundle at
`npm run build` time, not read at container runtime -- see `.env.example`):

| Variable | Required | Default | Description |
|---|---|---|---|
| `VITE_MAP_API_BASE_URL` | ❌ | same-origin | Base URL of this service's REST/WebSocket API. Leave unset when the built frontend is served from the same host:port as this service |
| `VITE_HOME_LATITUDE` | ❌ | — | Centered reference point for the "home" marker and initial camera. Both required together, or neither -- without them the map still renders, just without a home marker/recenter target |
| `VITE_HOME_LONGITUDE` | ❌ | — | |
