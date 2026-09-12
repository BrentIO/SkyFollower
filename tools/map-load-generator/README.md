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

There are no third-party runtime dependencies — `requirements.txt` exists
only so this tool's tests are picked up by CI's per-component discovery.

Stop early at any time with `Ctrl+C` (or `SIGTERM`); a final summary
(datagrams sent per type, average packet rate) prints on exit either way.
Progress (datagrams sent per type, current rate) prints every 5 seconds
while running.

## Simulated Fleet

Each simulated aircraft gets a distinct, deterministic, obviously-synthetic
identity, indexed `0` through `--aircraft-count - 1`:

- `icao_hex`: a fixed `FF` prefix (an unallocated ICAO address block) plus a
  zero-padded hex counter, e.g. `FF0000`, `FF0001`, ... — re-running the tool
  with the same `--aircraft-count` always reproduces the same set of
  addresses.
- `ident`: `LOAD0000`, `LOAD0001`, ...

Each aircraft moves along a simple circular orbit around a configurable
center point (`--center-lat`/`--center-lon`/`--radius-nm`), completing one
revolution every `--orbit-period-seconds`, recomputed every send — a frozen
position would understate the real cost of `map`'s trail accumulation and
the frontend's re-render work. Aircraft are evenly phase-offset around the
circle so `--aircraft-count` of them spread out rather than stacking on top
of each other. Altitude and vertical speed oscillate gently on the same
orbit angle, so those fields are genuinely non-static too. The whole motion
model is a pure function of `(aircraft_index, elapsed_seconds)` — no hidden
state, no randomness — so a run is fully reproducible given the same flags.

Metadata fields (operator/origin/destination/squawk/receiver_sources) are
also deterministic and cycle through a small pool of obviously-fake values
by aircraft index, so a multi-aircraft run exercises varied panel content
instead of every aircraft looking identical. `--matched-rule`, if given, is
stamped into every aircraft's `matched_rules` list, exercising the map's
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
  "lon": -98.5795,
  "alt": 35000.0,
  "velocity": 450.0,
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
  "aircraft": {"icao_hex": "FF0000"},
  "ident": "LOAD0000",
  "processor_id": "load-gen-1",
  "last_message": "2024-06-01T12:00:00.000Z",
  "operator": {"airline_designator": "LG1", "name": "Load Generator One", "callsign": "LOADGEN"},
  "origin": {"icao_code": "ZZ00", "name": "Load Test Airport ZZ00"},
  "destination": {"icao_code": "ZZ01", "name": "Load Test Airport ZZ01"},
  "squawk": "0000",
  "matched_rules": [],
  "receiver_sources": ["1090"]
}
```

Note `icao_hex` is nested under `aircraft`, never top-level — this packet
type is shaped like message-processor's `CompletedFlight` notification
payload (`_build_flight_notification_payload()`) with `"type": "metadata"`
added. It carries no `ts`; `map`'s out-of-order guard uses `last_message`
(an ISO-8601 timestamp) as this packet type's clock instead — verified
directly against `map/main.py`'s `_extract_timestamp()`.
`operator`/`origin`/`destination`/`squawk`/`matched_rules`/`receiver_sources`
are all omitted entirely (not sent as null/empty) when there's nothing to
report — this tool always has synthetic values for them, so in practice
they're always present except `matched_rules`, which is only included when
`--matched-rule` is given.

### `heartbeat`

Sent independently of aircraft traffic, on the same cadence as
message-processor's own `_map_heartbeat_loop` (`MAP_HEARTBEAT_INTERVAL_SECONDS`,
5s):

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
| `--port` | Yes | — | Target `map` instance's UDP listen port (`MAP_LISTEN_PORT`). |
| `--aircraft-count` | No | `10` | Number of simulated aircraft. |
| `--position-rate` | No | `2.0` | Position datagrams per second, **per aircraft**. Deliberately allowed to exceed the real `MAP_UDP_MIN_POSITION_INTERVAL_SECONDS` throttle (default 1/s in message-processor) — finding where that breaks is the point. |
| `--metadata-interval` | No | `30.0` | Seconds between metadata resends per aircraft. |
| `--processor-id` | No | `load-gen-1` | Fake `processor_id` stamped on every packet. |
| `--duration` | No | `0` | Run length in seconds. `0` (or omitted) runs indefinitely until `Ctrl+C`/`SIGTERM`. |
| `--mode` | No | `steady` | `steady`: hold `--position-rate` to wall-clock time. `stress`: send as fast as the socket accepts, no rate limiting at all — for finding an absolute ceiling rather than a sustained-rate one. Simulated motion still advances one tick per round in `stress` mode, so aircraft keep moving realistically even when massively outrunning real time. |
| `--center-lat` | No | `39.8283` | Orbit center latitude. |
| `--center-lon` | No | `-98.5795` | Orbit center longitude. |
| `--radius-nm` | No | `15.0` | Orbit radius, nautical miles. |
| `--orbit-period-seconds` | No | `300.0` | Simulated time for one full orbit revolution. |
| `--altitude-ft` | No | `35000.0` | Base cruise altitude, feet. |
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

The synthetic-motion and packet-construction logic — `synthetic_icao_hex`,
`synthetic_ident`, `synthetic_squawk`, `aircraft_position`,
`tick_elapsed_seconds`, `build_position_packet`, `build_metadata_packet`,
`build_heartbeat_packet`, and `synthetic_metadata_fields` — is pure (no
socket I/O) and covered directly: determinism, that positions genuinely
change over time and are phase-offset per aircraft, that a full orbit
returns to its starting point, and that each packet type's shape matches
the real wire protocol documented above (`icao_hex` nested under `aircraft`
for `metadata` but top-level for `position`, no `ts` on `metadata`, optional
fields omitted rather than sent empty).

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
