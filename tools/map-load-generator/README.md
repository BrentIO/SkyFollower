# Map Load Generator

A standalone command-line tool that fires synthetic UDP datagrams directly at
a `map` service instance, in the exact wire format `message-processor`
actually sends (`position`/`metadata`/`heartbeat`, verified against
`map/main.py`'s `_handle_packet()`/`_extract_timestamp()` and
`message-processor/main.py`'s `_publish_map_position`/
`_maybe_publish_map_metadata`/`_map_heartbeat_loop`). It lets update
frequency and simultaneous-aircraft count be pushed well past normal live
traffic to find where `map`'s backend ingestion and WebSocket relay actually
start to strain — without a real ADS-B feed, receiver, or message processor
in the loop.

Beyond raw throughput, the simulated fleet is a believable scenario: real
straight-line traffic lanes at FAA-hemispheric-correct altitudes, a varied
fleet (airliners, GA singles, business jets, helicopters, a balloon,
military, special livery), a VFR/IFR mix, migrating emergency squawks, and a
staggered startup ramp-up — so a load run also exercises `map`'s
altitude-layering, icon-shape-by-type resolution, and emergency-squawk
handling, not just packet throughput.

It is a plain Python script, not a container — there is no Dockerfile or
Compose service for it. Run it from any host that can reach the target
`map` instance's UDP listener (`MAP_LISTEN_HOST:MAP_LISTEN_PORT`).

**Scope**: this tool is a backend + WebSocket-relay load generator only. It
never opens a browser, so it says nothing about actual frontend rendering
cost (trail redraw, `GeoJSONSource.setData()` rebuild cost, etc.) — only
about whether `map`'s UDP ingestion, its Redis writes, and its WebSocket
broadcast keep up. A headless-browser rendering benchmark is a separate,
meaningfully bigger tool, not something bolted onto a UDP packet sender.

## Usage

```bash
pip install -r requirements.txt

python main.py --host 192.168.1.20 --port 5566 \
    --aircraft-count 200 --position-rate 2 --duration 300

python main.py --host 192.168.1.20 --port 5566 \
    --aircraft-count 50 --mode stress
```

`--port` defaults to `30500` (the installer's suggested
`MAP_LISTEN_PORT`/`MAP_UDP_PORT`) and can be omitted if the target `map`
instance uses it.

There are no third-party runtime dependencies — `requirements.txt` exists
only so this tool's tests are picked up by CI's per-component discovery.

Stop early at any time with `Ctrl+C` (or `SIGTERM`); a final summary
(datagrams sent per type, average packet rate) prints on exit either way.
Progress (datagrams sent per type, current rate) prints every 5 seconds
while running.

**No `--seed`**: start/finish points along each lane, squawk-code selection,
and emergency-squawk migration are all true per-run randomness by design —
this tool's older "fully reproducible, zero randomness" property was
intentionally given up in favor of a fleet that looks and behaves
differently, and realistically, on every run.

## Simulated Fleet

Aircraft fly straight, parallel traffic **lanes** instead of a shared
circular orbit — one E-W lane per fixed latitude, one N-S lane per fixed
longitude, each spanning `2 * --area-radius-nm` nautical miles across the
test area centered on `--center-lat`/`--center-lon`. Within a lane,
aircraft are strung 7 nautical miles apart in sequence; each aircraft's
start/finish point along its lane is randomized per run (see "No `--seed`"
above).

`--aircraft-count` sizes the fleet; one aircraft is always the balloon (see
below), and the rest occupy lane **seats** — persistent slots that keep
their lane and role (see "Fleet variety" below) for the life of the run.
When a seat's current occupant reaches the end of its lane, it simply stops
sending — no synthetic "eviction" packet exists on the real wire protocol —
and a fresh, never-reused synthetic `icao_hex` immediately takes the same
seat, keeping the concurrently-active fleet near `--aircraft-count`.

### Altitude and direction — the FAA hemispheric rule

Per 14 CFR 91.159 (VFR cruising altitude) and 91.179 (IFR cruising
altitude/flight level): magnetic course 000–179 ("eastbound") flies odd
thousands (VFR adds 500ft); course 180–359 ("westbound") flies even
thousands (VFR +500). At/above FL180 (Class A — IFR only), the same
odd/even split continues in flight levels, with no VFR variant:

| Below FL180 | Eastbound (000–179°) | Westbound (180–359°) |
|---|---|---|
| IFR | 3,000 / 5,000 / 7,000 / 9,000 / 11,000 / 13,000 / 15,000 / 17,000 | 4,000 / 6,000 / 8,000 / 10,000 / 12,000 / 14,000 / 16,000 |
| VFR (+500) | 3,500 / 5,500 / 7,500 / 9,500 / 11,500 / 13,500 / 15,500 / 17,500 | 4,500 / 6,500 / 8,500 / 10,500 / 12,500 / 14,500 / 16,500 |

| At/above FL180 (IFR only) | Eastbound | Westbound |
|---|---|---|
| Flight levels | FL190 / FL210 / FL230 / FL250 / FL270 / FL290 | FL200 / FL220 / FL240 / FL260 / FL280 |

Every lane's altitude comes directly from this table, so every aircraft's
displayed altitude is always hemispherically correct for its heading —
except while a climbing/descending aircraft (see below) is transiting
between two such altitudes. There is no separate "flight rules" field: a
VFR aircraft is identified the real-world way, by squawking the standard
US VFR code `1200` (see "Squawk assignment" below). An aircraft above
FL180 is always IFR and always carries an operator (Class A airspace
requires an IFR clearance); below FL180, VFR (no operator, no
origin/destination) and IFR (operator, one fixed shared origin/destination
pair) are both represented.

### Speed and vertical rate

Vertical speed is clamped to ±800ft/min. Groundspeed is a function of
altitude, linearly interpolated between `100ft → 60kt` and
`40,000ft → 500kt` (clamped at both ends) — so low-altitude traffic (light
GA, helicopters) comes out around real-world light-aircraft speeds, and
high-altitude lanes come out around real airliner cruise speeds, with no
per-type speed table. The balloon is the one exception — see below.

A small subset (`--climbing-count`, default `2`) transitions between two
altitudes over the run — its lane's own altitude and the next
hemispherically-valid altitude up in the same direction — climbing then
descending at a steady rate comfortably inside the ±800ft/min clamp,
instead of holding level the whole time.

### Squawk assignment

- **VFR** aircraft (below-FL180, +500 altitude) always squawk `1200`, and
  never carry an operator or origin/destination.
- **IFR** aircraft squawk a synthetic discrete-looking code instead, and
  always carry an operator and the one fixed origin/destination pair shared
  by every IFR aircraft in the run.
- A small subset (2, fixed) instead squawk one of the real emergency codes
  `7500`/`7600`/`7700`/`7777`, chosen randomly, overriding whatever squawk
  they'd otherwise have. When an aircraft holding an emergency code exits
  its lane, a **different**, randomly chosen active aircraft picks up a
  (also randomly chosen) emergency code next — so there's always roughly
  the same small number of "emergency" flights active, just never the same
  tail number for long.

### Wake turbulence, receiver sources

`wake_turbulence_category` is sent for every aircraft with metadata (not
the no-metadata subset — see below), randomly one of `light`/`medium`/
`heavy`. `receiver_sources` is a randomly chosen, randomly sized (1–3)
subset of `978`/`1090`/`EXTERNAL` per aircraft.

### Fleet variety

Each lane seat is assigned one role for the life of the run (only the
occupant — hex/ident/squawk/etc. — changes on a lane-exit replacement):

- **Regular** (the majority): a real ICAO type designator cycled from a
  pool spanning airliners, GA singles, and business jets.
- **Helicopter** (3, fixed): one each of three distinct real helicopter
  type designators — `R44` (piston GA), `EC35` (light-twin EMS/H135), `S76`
  (medium-twin offshore/EMS).
- **Climbing** (`--climbing-count`, default `2`): see "Speed and vertical
  rate" above.
- **No-metadata** (2, fixed): simulates an enrichment-database miss — the
  `aircraft` dict carries only `icao_hex` and `emitter_category` (the raw
  ADS-B-broadcast category), nothing else.
- **Military** (1, fixed): `military: true`, with a real military type
  designator (`F16`/`F15`).
- **Livery** (1, fixed): `special_livery: "Overclocked Osprey"` on a V-22
  Osprey (`type_designator: "V22"`, tiltrotor description code `"T"`).

One further aircraft, outside the lane seat pool entirely, is the
**balloon**: a diagonal heading off the E-W/N-S lane grid, centered around
60,000ft at a slow ~15–25kt drift, with a gentle organic wander in both
heading and altitude — not part of the hemispheric-rule lane grid, and not
governed by the altitude-interpolation groundspeed formula above.

Every type designator used is one `map/frontend/src/lib/
aircraftIconResolver.ts` actually resolves to a dedicated icon shape,
either directly or through its `TYPE_ALIASES`/`DESCRIPTION_SHAPES` tables.

### Startup ramp-up

The fleet joins staggered over `--ramp-up-seconds` (default `60`) instead
of all appearing at tick 0 — aircraft `i` doesn't start sending until
`i * (ramp_up_seconds / aircraft_count)` seconds into the run, matching how
a real `message-processor` discovers aircraft one at a time rather than all
at once. `--ramp-up-seconds 0` disables this explicitly (today's
instant-appearance behavior). Ramp-up is disabled outright (not scaled
down) whenever `--duration` is nonzero and shorter than `--ramp-up-seconds`
— a short, deliberate run shouldn't spend its whole duration still ramping
up. This is a one-time run-start behavior only; a lane-exit replacement
always spawns immediately regardless.

### Second processor (roster staleness)

A second, one-shot `processor_id` (`--processor-id` + `-secondary`) sends
exactly one `heartbeat` packet at the very start of the run, then nothing
further — its roster entry ages from green to amber to red purely from real
wall-clock silence over a long-enough run, exercising a code path the
primary (continuously healthy) `--processor-id` never touches.

Metadata fields (operator/origin/destination/squawk/receiver_sources) are
randomized per the rules above; `--matched-rule`, if given, is stamped into
every aircraft's `matched_rules` list unchanged, exercising the map's
rule-match indicator too.

## Wire Format

Each UDP datagram is a single UTF-8 JSON object, no framing — matching what
`message-processor` actually sends and what `map/main.py`'s `_udp_loop()`
expects.

### `position`

Sent `--position-rate` times per second, per aircraft:

```json
{
  "type": "position",
  "icao_hex": "FF0000",
  "ts": 1717200000.123,
  "processor_id": "load-gen-1",
  "lat": 39.8283,
  "lon": -98.7203,
  "alt": 5000.0,
  "velocity": 140.6,
  "hdg": 90.0,
  "vs": 0.0
}
```

`ts` is the send-time Unix epoch float — `map`'s out-of-order guard
(`_extract_timestamp`) requires a numeric `ts` for this packet type.

### `metadata`

Sent once per aircraft at startup, then again every `--metadata-interval`
seconds (real message-processor sends this on first-known-metadata and
again only on change, plus an unconditional 60-second resend loop — this
tool always resends on a fixed interval, since there's no real "change" to
detect against synthetic data):

```json
{
  "type": "metadata",
  "aircraft": {
    "icao_hex": "FF0000",
    "type_designator": "B738",
    "category": "Land",
    "wake_turbulence_category": "medium"
  },
  "ident": "LOAD0000",
  "processor_id": "load-gen-1",
  "last_message": "2024-06-01T12:00:00.000Z",
  "operator": {"airline_designator": "LG1", "name": "Load Generator One", "callsign": "LOADGEN"},
  "origin": {"icao_code": "ZZ00", "name": "Load Test Airport ZZ00"},
  "destination": {"icao_code": "ZZ01", "name": "Load Test Airport ZZ01"},
  "squawk": "4703",
  "matched_rules": [],
  "receiver_sources": ["1090", "EXTERNAL"]
}
```

A VFR aircraft carries `"squawk": "1200"` and omits `operator`/`origin`/
`destination` entirely; the no-metadata subset's `aircraft` dict carries
only `icao_hex` and `emitter_category`; the military/livery seats add
`military: true` / `special_livery`/`description_code` alongside the fields
above.

Note `icao_hex` is nested under `aircraft`, never top-level — this packet
type is shaped like message-processor's `CompletedFlight` notification
payload (`_build_flight_notification_payload()`) with `"type": "metadata"`
added. It carries no `ts`; `map`'s out-of-order guard uses `last_message`
(an ISO-8601 timestamp) as this packet type's clock instead — verified
directly against `map/main.py`'s `_extract_timestamp()`. Optional fields
are omitted entirely (not sent as null/empty) when there's nothing to
report.

### `heartbeat`

Sent independently of aircraft traffic, on the same cadence as
message-processor's own `_map_heartbeat_loop` (`MAP_HEARTBEAT_INTERVAL_SECONDS`,
5s) for the primary `--processor-id`, plus exactly once at run start for the
second, one-shot `processor_id` (see "Second processor" above):

```json
{"type": "heartbeat", "processor_id": "load-gen-1", "ts": 1717200000.123}
```

`processor_id` is stamped on every packet type above, not just
`heartbeat` — `map` updates its per-processor liveness roster from any of
the three, matching the real traffic-reduction design (see
`map/README.md`'s "Processor Roster" section).

## Arguments

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--host` | Yes | — | Target `map` instance's UDP listen host. |
| `--port` | No | `30500` | Target `map` instance's UDP listen port (`MAP_LISTEN_PORT`). |
| `--aircraft-count` | No | `10` | Number of simulated aircraft, including the one balloon. |
| `--position-rate` | No | `2.0` | Position datagrams per second, **per aircraft**. Deliberately allowed to exceed the real `MAP_UDP_MIN_POSITION_INTERVAL_SECONDS` throttle (default 1/s in message-processor) — finding where that breaks is the point. |
| `--metadata-interval` | No | `30.0` | Seconds between metadata resends per aircraft. |
| `--processor-id` | No | `load-gen-1` | Fake `processor_id` stamped on every packet. A second, one-shot `processor_id` (this value + `-secondary`) sends exactly one heartbeat at run start. |
| `--duration` | No | `0` | Run length in seconds. `0` (or omitted) runs indefinitely until `Ctrl+C`/`SIGTERM`. |
| `--mode` | No | `steady` | `steady`: hold `--position-rate` to wall-clock time. `stress`: send as fast as the socket accepts, no rate limiting at all — for finding an absolute ceiling rather than a sustained-rate one. Simulated motion still advances one tick per round in `stress` mode, so aircraft keep moving realistically even when massively outrunning real time. |
| `--center-lat` | No | `39.8283` | Lane grid center latitude. |
| `--center-lon` | No | `-98.5795` | Lane grid center longitude. |
| `--area-radius-nm` | No | `15.0` | Lane grid size — half the length of every lane, nautical miles. |
| `--climbing-count` | No | `2` | Number of aircraft that transition between two altitudes over their run instead of holding level. |
| `--ramp-up-seconds` | No | `60.0` | Seconds over which the fleet joins staggered instead of all appearing at tick 0. `0` disables ramp-up explicitly. Disabled outright (not scaled down) if `--duration` is nonzero and shorter than this. |
| `--matched-rule` | No | none | If given, every simulated aircraft's metadata reports this identifier in `matched_rules`. |

## Modes

- **`steady`** — the primary use case. Holds `--position-rate`
  datagrams/sec/aircraft to wall-clock time, for sustained-rate soak
  testing (does `map` keep up indefinitely at a given rate?). If a round
  falls behind schedule (e.g. slow local socket send under a very high
  aircraft count), later rounds send immediately rather than sleeping
  further — this tool never tries to "catch up" by bursting.
- **`stress`** — sends every datagram back-to-back as fast as the local UDP
  socket accepts, with no delay at all. Use this to find an absolute
  ingestion ceiling rather than a sustained-rate one. `--position-rate`
  still governs how fast simulated motion advances per round (so aircraft
  positions remain realistic), it just no longer paces wall-clock sends.

## Tests

```bash
python -m pytest tools/map-load-generator/tests/
```

The lane-motion, altitude/hemispheric, speed/vertical-rate, fleet-variety,
squawk-migration, and packet-construction logic is pure (no socket I/O) and
covered directly — including `FleetSimulator`, the piece that decides which
aircraft are active and where, independent of real sockets or wall-clock
time. Because start/finish points, squawk selection, and emergency-squawk
migration are true per-run randomness (see "No `--seed`" above), tests
check *bounds and rules* — altitude is always one of the valid hemispheric
values for a lane's direction, `vs` never exceeds ±800ft/min, VFR aircraft
always squawk `1200`, a lane-exit replacement gets a never-reused
`icao_hex`, the emergency-squawk count stays constant across many exits —
rather than exact values.

`run()`/`main()` (the live-send loop, socket creation, argument parsing) is
intentionally not covered by a separate unit test — a real UDP send isn't
meaningfully unit-testable, and this glue has no branching logic beyond
what's already exercised above. It was instead verified against a real
local UDP listener during development (packet counts, types, and field
values, both `steady` and `stress` mode).

## Documentation Site

Tool directories under `tools/` are picked up automatically by the
[documentation site](https://brentio.github.io/SkyFollower/)'s
`docs/scripts/discover.mjs` `tools/*` scan, which generates a page per tool
from this README with no change to `discover.mjs` needed. The hand-authored
`docs/tools/index.md` overview page lists each tool by name and is updated
alongside this file.
