# Archive Processor

The archive processor consumes completed flight records from the RabbitMQ
`archive` queue, builds a 3D GeoJSON `LineString` of the flight path
(interpolating missing altitude from adjacent position reports), writes each
flight as gzip-compressed JSON to AWS S3, and appends a row to a local Parquet
metadata index for fast lookups without needing to scan S3. When S3 is
unavailable, completed flights are queued locally and drained automatically
once S3 reconnects.

![Archive processor architecture](./archive-processor.svg)

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
| `s3.access_key_id` | string | — | AWS access key ID |
| `s3.secret_access_key` | string | — | AWS secret access key |
| `s3.region` | string | `"us-east-1"` | AWS region for the S3 bucket |
| `s3.bucket` | string | — | S3 bucket name flights are written to |
| `flight_ttl_seconds` | integer | `300` | Used to detect flights artificially split by a processor-count resize (see "Split-Flight Stitching" below). Should match the processors' own `flight_ttl_seconds`. |
| `telemetry_interval_seconds` | integer | `30` | How often (seconds) the archive processor publishes MQTT statistic messages |
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `s3.db` (the S3 offline fallback) and `flight_index.parquet` (the metadata index) are written |
| `log_level` | string | `"info"` | Log verbosity. Set to `"debug"` for verbose output. |

## Consuming from RabbitMQ

The archive processor declares and consumes from a single durable queue
named `archive` (`prefetch_count=1`, manual ack). This is the queue the
message processor publishes completed flights to — see
[processor/README.md](../processor/README.md).
A message that fails to process is not requeued; instead it is written to
the local fallback queue and acknowledged, to avoid poison-message retry
loops.

## S3 Object Format

Each flight is written to:

```
flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz
```

- `{YYYY}/{MM}/{DD}` — UTC date of the flight's last message
- `{ident}` — non-alphanumeric characters stripped; `unknown` if absent
- `{uuid}` — the flight's `_id` (UUID-v7)

The object body is the completed flight record (see
[shared/README.md](../shared/README.md)
for `CompletedFlight`) with one addition: a `flight_path` GeoJSON `Feature`
built from `positions`. Each
coordinate is `[lon, lat, alt_ft]` when altitude is known (interpolated
linearly from the nearest preceding/following position with an altitude) or
`[lon, lat]` when no altitude is known anywhere nearby. Flights with fewer
than two positions have no `flight_path`. The payload is gzip-compressed
before upload, with `ContentType: application/json` and
`ContentEncoding: gzip`.

## Split-Flight Stitching

Resizing a deployment's processor count reshuffles which processor an
aircraft routes to, which can force a flight to be archived early even
though the aircraft keeps flying — the continuation shows up as a second,
separate flight on whichever processor it's now routed to. The archive
processor detects and merges this after the fact:

- After archiving a flight, it writes a small pointer to Redis —
  `archive:last_segment:{icao_hex}` (see `shared/redis_keys.py`), containing
  the flight's `_id`, `first_message`, `last_message`, and S3 key. This
  expires after 1 day.
- Before archiving the *next* flight for that aircraft, it checks for a
  pointer. If the new flight's `first_message` is within
  `flight_ttl_seconds` of the pointer's `last_message`, this is treated as
  a continuation rather than a new flight: the archive processor fetches
  the previous S3 object, merges the two segments — concatenated and
  re-sorted `positions`/`velocities`, a recomputed `flight_path`, a deduped
  union of `matched_rules`, and summed `total_messages` — and overwrites
  the *original* S3 object under its original `_id`. The new segment's own
  S3 object is never created.
- A gap beyond `flight_ttl_seconds` (or no pointer at all) means this is a
  genuinely new flight; it's archived normally and a fresh pointer is
  written. Three or more consecutive artificial splits chain correctly —
  each stitches into the same original segment, not a new one each time.
- This is a purely archive-side concern: it doesn't touch the message
  processor or affect live MQTT rule notifications, which are published in
  real time as each segment is tracked, before the archive processor ever
  sees a completed flight.
- If the previous segment can't be fetched (S3 error, object deleted, etc.),
  the current flight falls back to being archived as its own new object
  rather than blocking or dropping data — the Redis pointer still advances
  to point at whatever was actually written, so a later segment can still
  stitch onto it going forward, even though this specific pair missed the
  merge.

![Split-flight stitching](./split-flight-stitching-sequence.svg)

## Parquet Metadata Index

Every successful S3 write also appends a row to `flight_index.parquet` (in
`data_dir`) via DuckDB, so flights can be looked up without scanning S3:

| Column | Type | Source |
|--------|------|--------|
| `_id` | VARCHAR | Flight UUID-v7 |
| `icao_hex` | VARCHAR | Aircraft ICAO hex |
| `registration` | VARCHAR | Aircraft registration, if known |
| `ident` | VARCHAR | Flight ident/callsign, if known |
| `first_message` | TIMESTAMP WITH TIME ZONE | Timestamp of the flight's first message |
| `last_message` | TIMESTAMP WITH TIME ZONE | Timestamp of the flight's last message |
| `operator_designator` | VARCHAR | Operator ICAO designator, if known |
| `s3_key` | VARCHAR | The S3 object key the flight was written to |

This index is local to each archive processor instance and is not itself
replicated to S3 (see the Parquet/Athena open item in the top-level
`CLAUDE.md` for the longer-term cross-instance query strategy).

## Fault Tolerance

When S3 is unavailable — at startup or during operation — completed flights
are written to `s3.db` (SQLite, in `data_dir`) instead. A background
thread checks S3 connectivity every 10 seconds; once it reconnects, the
fallback queue is drained oldest-first, with each flight written to S3 and
indexed exactly as it would have been on the normal path. RabbitMQ
connection failures are retried every 10 seconds independently of the S3
fallback logic.

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/archive/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/archive/statistics` | JSON stats payload (see fields below) | Yes |

**Statistics payload fields:**

| Field | Type | Description |
|-------|------|-------------|
| `started_at` | string | UTC ISO-8601 timestamp of process start |
| `flights_archived_hour` | integer | Flights successfully written to S3 this hour |
| `flights_archived_today` | integer | Flights successfully written to S3 today (UTC) |
| `s3_connected` | boolean | Current S3 connectivity state |
| `local_queue_depth` | integer | Flights currently queued in `s3.db` fallback |

All statistics are published as a single retained JSON payload every
`telemetry_interval_seconds`. Home Assistant autodiscovery payloads are
published to `homeassistant/sensor/SkyFollower_archive_{field}/config` on
MQTT connect, each using `value_template` to extract its field from the
shared statistics topic.
