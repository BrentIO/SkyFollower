# SkyFollower Message Processor

The message processor consumes raw ADS-B and UAT messages from a RabbitMQ
queue, maintains per-aircraft flight state in an in-memory SQLite database,
enriches each flight with registration and operator data from Redis, evaluates
the configured rules engine, publishes MQTT notifications when rules match, and
routes completed flights to the archive queue (or a local SQLite fallback when
RabbitMQ is unavailable). One container equals one processor instance; scale
horizontally by adding processor containers on separate hosts.

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
| `flight_ttl_seconds` | integer | `300` | Seconds of silence before a flight is considered complete and sent to the archive queue. Too short fragments flights; too long merges back-to-back flights on quick-turn aircraft. |
| `telemetry_interval_seconds` | integer | `30` | How often (seconds) the processor publishes MQTT statistic messages and refreshes its Redis heartbeat key. |
| `home_latitude` | float | — | Receiver home latitude (decimal degrees). Required for single-message CPR position decoding (DF 17, TC 5–18). Omit if position decoding is not needed. |
| `home_longitude` | float | — | Receiver home longitude (decimal degrees). |
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `archive.db` (the RabbitMQ offline fallback) is written. |
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

## Redis Key Dependencies

### Keys read

| Key pattern | Purpose |
|-------------|---------|
| `icao_hex:{ICAO_HEX}` | Aircraft registration and type enrichment (read once per new flight) |
| `operator:{DESIGNATOR}` | Airline operator enrichment (read once per flight when ident is first seen) |
| `flight:{IDENT}` | Origin/destination enrichment (read once per flight when ident is first seen) |
| `config:rules:version` | SHA-256 hash polled every 5 s; triggers rule reload when changed |
| `config:rules` | JSON rules array; loaded when version changes |
| `config:areas:version` | SHA-256 hash polled every 5 s; triggers area reload when changed |
| `config:areas` | GeoJSON FeatureCollection; loaded when version changes |

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
| `local_archive_queue_depth` | integer | Completed flights queued in `archive.db` fallback |
| `registration_misses_hour` | integer | Aircraft Redis cache misses this hour |
| `registration_misses_today` | integer | Aircraft Redis cache misses today (UTC) |
| `aircraft_type_misses_hour` | integer | Aircraft type lookup misses this hour |
| `aircraft_type_misses_today` | integer | Aircraft type lookup misses today (UTC) |
| `active_flights` | integer | Flights currently tracked in the in-memory store |

All statistics are published as a single retained JSON payload every `telemetry_interval_seconds`.
Home Assistant autodiscovery payloads are published to
`homeassistant/sensor/SkyFollower_processor_{ID}_{field}/config` on MQTT connect,
each using `value_template` to extract its field from the shared statistics topic.

## Fault Tolerance

When RabbitMQ is unavailable at startup or during operation, completed flights
are written to `archive.db` (SQLite WAL mode) in `data_dir`. On the next
successful RabbitMQ reconnect, the fallback queue is drained oldest-first
before new messages are consumed. Redis and MQTT failures are handled
gracefully and logged; enrichment lookups that fail leave the flight partially
enriched rather than dropping it.

## Rules Engine

Rules and areas are loaded from Redis (`config:rules` / `config:areas`) and
hot-reloaded every 5 seconds when the corresponding version hash keys change.
The rules engine is implemented in `processor/rules_engine.py`.

Each rule must have a unique `identifier`, an `enabled` boolean, and a
non-empty `conditions` array. All conditions within a rule are ANDed together.
A rule fires at most once per flight per identifier (the identifier is added to
`flight.matched_rules` on first match, and subsequent evaluations skip it).

Conditions are sorted by evaluation cost before each evaluation pass — cheap
field comparisons run before expensive geographic checks. See the top-level
README for the full condition type reference.
