# Message Processor

The message processor consumes raw ADS-B and UAT messages from a RabbitMQ
queue, maintains per-aircraft flight state in a file-backed (WAL-mode) SQLite
database so it survives a process restart, enriches each flight with
registration and operator data from Redis, evaluates
the configured rules engine, publishes MQTT notifications when rules match, and
routes completed flights to the archive queue (or a local SQLite fallback when
RabbitMQ is unavailable). One container equals one processor instance; scale
horizontally by adding processor containers on separate hosts.

![Processor architecture](./processor.svg)

## Configuration (`settings.json`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `rabbitmq.host` | string | — | RabbitMQ hostname or IP |
| `rabbitmq.port` | integer | `5672` | RabbitMQ AMQP port |
| `rabbitmq.username` | string | — | RabbitMQ username |
| `rabbitmq.password` | string | — | RabbitMQ password |
| `redis.host` | string | — | Redis hostname or IP |
| `redis.port` | integer | `6379` | Redis port |
| `mqtt.host` | string | — | MQTT broker hostname (omit key to disable MQTT) |
| `mqtt.port` | integer | `1883` | MQTT broker port |
| `mqtt.username` | string | — | MQTT username. Optional — omit both `username` and `password` to connect anonymously. |
| `mqtt.password` | string | — | MQTT password |
| `rule_notification_max_lag_seconds` | integer | `30` | Maximum age (seconds, message `received_at` vs. wall-clock time) of a message whose rule match still gets published to MQTT. Older matches (replayed from a RabbitMQ backlog after a restart) still fire and are recorded in `matched_rules`, just not pushed to MQTT — prevents flooding MQTT with backlogged notifications the instant a processor reconnects. |
| `telemetry_interval_seconds` | integer | `30` | How often (seconds) the processor publishes MQTT statistic messages and refreshes its Redis heartbeat key. |
| `latitude` | float | — | Receiver location latitude (decimal degrees). Required for single-message CPR airborne position decoding. Omit if position decoding is not needed. |
| `longitude` | float | — | Receiver location longitude (decimal degrees). |
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `active_flights.db` (the durable active flight store) and `completed_flights.db` (the RabbitMQ offline fallback) are written. |
| `log_level` | string | `"info"` | Log verbosity. Set to `"debug"` for verbose output. |

### `PROCESSOR_ID` Environment Variable

`PROCESSOR_ID` is a required integer environment variable set in the Docker
Compose service definition. It must match one of the queue names declared by
the receiver (`adsb-0`, `adsb-1`, …). The processor consumes from
`adsb-{PROCESSOR_ID}`.

On startup the processor attempts to claim a Redis key
`processor:{PROCESSOR_ID}:heartbeat` using `SET NX`. If the key already exists
(i.e., another instance with the same ID is running), the process exits
immediately to prevent duplicate-ID conflicts.

Example:

```yaml
environment:
  PROCESSOR_ID: "0"
```

## Decoding

Raw Mode-S/ADS-B frames are decoded via pyModeS 3.x's single unified
`decode()` call, which returns every decodable field for a message in one
dict. The processor extracts fields purely by presence — if a field is in
the result, it's used; there's no downlink-format or typecode dispatch.
Message types that don't populate any field the processor cares about
(e.g. ACAS RA broadcasts) simply produce nothing and are dropped, with
nothing to explicitly filter.

**This is a wider scope than before**: earlier versions filtered to DF
5/17/21 only. Any downlink format pyModeS can extract a relevant field from
(e.g. DF4 surveillance altitude replies) now contributes to flight state.

**Any message pyModeS flags as CRC-invalid is rejected outright** — but
this only provides real protection for DF17/18 (extended squitter), where
`crc_valid` reflects an actual CRC-remainder-equals-zero check. For
DF0/4/5/11/16/20/21 (including squawk, DF5/21), pyModeS hardcodes
`crc_valid=True` unconditionally, since their CRC field encodes the ICAO
address itself rather than providing an independent integrity check —
there's no single-message corruption signal available for those message
types at all. A squawk value is trusted once decoded; there's nothing
further to verify it against outside of multi-message/pipe-mode decoding,
which this processor doesn't use.

**`wake_turbulence_category`** is sourced directly from pyModeS's own
computed `wake_vortex` field, which is aware of which identification
sub-type (typecode) a category code came from — TC=4 aircraft categories,
TC=3 gliders/UAVs, and TC=2 surface vehicles are all encoded on the same
numeric scale but mean different things, and pyModeS's mapping accounts for
that. Its wording differs slightly in casing from earlier versions of this
processor (e.g. `"High vortex aircraft"` instead of `"High Vortex
Aircraft"`) — existing `wake_turbulence_category` rule conditions matching
the old exact casing need updating.

## Redis Key Dependencies

### Keys read

| Key pattern | Purpose |
|-------------|---------|
| `EVALSHA` → `shared/lua/merge_aircraft.lua` | Aircraft registration and type enrichment (read once per new flight). Not a direct key read: the processor calls this script (`SCRIPT LOAD`ed once at startup) with `icao_hex` as its sole argument and has no visibility into what it reads. The script itself performs the underlying `JSON.GET`s against `aircraft:mictronics:{icao_hex}` and `aircraft:registry:{icao_hex}` server-side and returns the deep-merged result (registry winning on any field overlap) in a single round-trip. |
| `operator:{DESIGNATOR}` | Airline operator enrichment (read once per flight when ident is first seen) |
| `flight:{IDENT}` | Origin/destination enrichment (read once per flight when ident is first seen) |
| `config:rules:version` | SHA-256 hash polled every 5 s; triggers rule reload when changed |
| `config:rules` | JSON rules array; loaded when version changes |
| `config:areas:version` | SHA-256 hash polled every 5 s; triggers area reload when changed |
| `config:areas` | GeoJSON FeatureCollection; loaded when version changes |
| `config:flight_ttl_seconds` | Plain scalar, read once at startup and cached (not hot-reloaded — restart to pick up a changed value); defaults to `300` if unset. Shared with the archive processor, which uses the same value to detect flights split by a processor-count resize. |

### Keys written

| Key pattern | Purpose |
|-------------|---------|
| `processor:{ID}:heartbeat` | Liveness key; claimed with `NX` on startup, TTL refreshed every `telemetry_interval_seconds × 2` |
| `registration:{REGISTRATION}` | Reverse-lookup index (registration → ICAO hex); written `NX` when aircraft enrichment is found and a registration exists |
| `metrics:processor:{ID}:registration_misses:{hour\|today\|lifetime}` | Incremented each time an `icao_hex:` or `operator:` lookup returns no result. The `_hour` key has a 3600 s TTL; `_today` expires at the next UTC midnight. Both are set on first write via `INCR` + `EXPIREAT`/`EXPIRE`. `_lifetime` has no TTL. |
| `metrics:processor:{ID}:aircraft_type_misses:{hour\|today\|lifetime}` | Incremented each time an aircraft type lookup returns no result. Same TTL scheme as above. |

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/processor/{ID}/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/processor/{ID}/statistics` | JSON stats payload (see fields below) | Yes |
| `SkyFollower/rule/{IDENTIFIER}` | JSON flight snapshot (no positions/velocities) with `rule` key | No |

**Statistics payload fields:**

| Field | Type | Description |
|-------|------|-------------|
| `started_at` | string | UTC ISO-8601 timestamp of process start |
| `messages_per_second` | float | Rolling 30-second average message rate |
| `processing_time_hwm_ms` | float | End-to-end processing time high-water mark since last publish; resets on publish |
| `rules_engine_hwm_ms` | integer | Rules engine duration high-water mark since last publish; resets on publish |
| `rabbitmq_input_queue_depth` | integer | Current input queue depth (`-1` on error) |
| `local_archive_queue_depth` | integer | Completed flights queued in `completed_flights.db` fallback |
| `registration_misses_hour` | integer | Aircraft Redis cache misses this hour |
| `registration_misses_today` | integer | Aircraft Redis cache misses today (UTC) |
| `aircraft_type_misses_hour` | integer | Aircraft type lookup misses this hour |
| `aircraft_type_misses_today` | integer | Aircraft type lookup misses today (UTC) |
| `active_flights` | integer | Flights currently tracked in the active store |

All statistics are published as a single retained JSON payload every `telemetry_interval_seconds`.
Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_processor_{ID}_{field}/config` on MQTT connect,
each using `value_template` to extract its field from the shared statistics topic.

## Fault Tolerance

When RabbitMQ is unavailable at startup or during operation, completed flights
are written to `completed_flights.db` (SQLite WAL mode) in `data_dir`. On the next
successful RabbitMQ reconnect, the fallback queue is drained oldest-first
before new messages are consumed. Redis and MQTT failures are handled
gracefully and logged; enrichment lookups that fail leave the flight partially
enriched rather than dropping it.

### Active flight store durability & crash recovery

`active_flights.db` (SQLite, WAL mode, `data_dir`) holds every currently
tracked flight and is committed to disk after every message. A process end —
whether a deliberate stop (`SIGTERM`/`SIGINT`) or an ungraceful one (OOM-kill,
`docker kill`, host crash) — is recovered identically on the next startup;
there is no special "flush everything" shutdown path, since nothing needs
force-archiving when the store already survives on its own.

On startup, the processor reopens `active_flights.db` and recovers whatever
flights were still tracked. Recovery is driven by message timestamps, not by
how long the container was down: an internal clock is floored at the most
recent `last_message` among recovered flights (not wall-clock "now"), and
only advances as RabbitMQ messages are actually consumed. This means a
recovered flight is **not** archived just because real time passed while the
container was stopped — if a continuation message for that aircraft is
sitting in the RabbitMQ backlog, it resumes the same flight once the
processor reconnects and drains the backlog. A genuine gap longer than
`flight_ttl_seconds` — whether it happens live or is discovered while
replaying a backlog — still correctly splits the flight into two records.

MQTT rule notifications for messages older than
`rule_notification_max_lag_seconds` are suppressed during backlog replay
(logged at debug) to avoid flooding MQTT the instant a processor reconnects
after downtime; the rule still fires and is still recorded in
`matched_rules`/the eventual archived flight.

Because `active_flights.db` only depends on `PROCESSOR_ID` (which determines
the RabbitMQ queue consumed, `adsb-{PROCESSOR_ID}`) and not on container
identity, moving the file to a replacement container with the same
`PROCESSOR_ID` resumes tracking the same way a restart does. One caveat: the
Redis heartbeat key (`processor:{PROCESSOR_ID}:heartbeat`, `SET NX` with a
TTL of `2 × telemetry_interval_seconds`) must expire — or be deleted
manually — before a replacement container can claim the same ID.

## Rules Engine

Rules and areas are loaded from Redis (`config:rules` / `config:areas`) and
hot-reloaded every 5 seconds when the corresponding version hash keys change.
The rules engine is implemented in `processor/rules_engine.py`.

Each rule must have a unique `identifier`, an `enabled` boolean, and a
non-empty `conditions` array. All conditions within a rule are ANDed together.
A rule fires at most once per flight per identifier (the identifier is added to
`flight.matched_rules` on first match, and subsequent evaluations skip it).

Conditions are sorted by evaluation cost before each evaluation pass — cheap
field comparisons run before expensive geographic checks. See
[Rules & Areas](https://brentio.github.io/SkyFollower/rules-and-areas/) for
the full condition type reference.
