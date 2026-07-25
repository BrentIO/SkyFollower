# Archive Processor

The archive processor consumes completed flight records from the RabbitMQ
`archive` queue, builds a 3D GeoJSON `LineString` of the flight path
(interpolating missing altitude from adjacent position reports), writes each
flight as gzip-compressed JSON to AWS S3, and writes a small per-flight
Parquet index row alongside it in the same bucket, queryable via AWS
Athena/Glue without needing to scan S3. When S3 is unavailable, completed
flights are queued locally and drained automatically once S3 reconnects.

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
| `telemetry_interval_seconds` | integer | `30` | How often (seconds) the archive processor publishes MQTT statistic messages |
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `s3.db` (the S3 offline fallback, and the Parquet-index write retry queue — see [Fault Tolerance](#fault-tolerance)) is written |
| `log_level` | string | `"info"` | Log verbosity. Set to `"debug"` for verbose output. |

`flight_ttl_seconds` is not a local setting — it's read from `config:flight_ttl_seconds`
in Redis (shared with the processor) once at startup and cached; not
hot-reloaded, restart the container to pick up a changed value. Defaults to
`300` if unset. See [Split-Flight Stitching](#split-flight-stitching) below
for how it's used.

## Consuming from RabbitMQ

The archive processor declares and consumes from a single durable queue
named `archive` (`prefetch_count=1`, manual ack). This is the queue the
message processor publishes completed flights to — see
[processor/README.md](../processor/README.md).
A message that fails to process is not requeued; instead it is written to
the local fallback queue and acknowledged, to avoid poison-message retry
loops.

## MLAT-Only Flight Skip

Flights whose `receiver_sources` is exactly `["MLAT"]` are dropped instead
of archived — not written to S3, and not queued to the local fallback
either, since the drop is deliberate rather than deferred. MLAT can
produce a very large number of tracked aircraft the user has no interest
in, and S3 storage cost scales with what gets archived. Any mix that
includes a non-MLAT source (e.g. `["1090", "MLAT"]`) archives normally,
since the aircraft was independently seen on a real receive path at some
point.

`force_archive: true` on the flight (set by the processor when any matched
rule in `config:rules` carries a `force_archive: true` property — see
[processor/README.md](../processor/README.md)) overrides the skip for
MLAT-only flights the user does care about, without having to archive
every MLAT contact indiscriminately. Skipped flights increment
`flights_skipped_hour`/`flights_skipped_today` (see Statistics below).

## S3 Object Format

Each flight is written to:

```
flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz
```

- `{YYYY}/{MM}/{DD}` — UTC date of the flight's *first* message, not its last. This key is only ever computed once, at first archive — a later [split-flight stitch](#split-flight-stitching) overwrites the object in place under this same key rather than recomputing it, so the date has to come from whichever timestamp stitching never changes. `first_message` is exactly that (`_merge_segments` always preserves the original segment's `first_message`; only `last_message` advances with each stitched segment) — using it keeps the key stable across any number of stitches, even ones that happen to straddle a UTC day boundary. The [Parquet index row](#parquet-index)'s key follows the same rule, for the same reason (it's rebuilt on every stitch, unlike this object's key).
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

## Parquet Index

After a successful flight write, the archive processor also writes a
single-row Parquet file for that flight to the same S3 bucket, alongside
the flight object:

```
index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet
```

- `year=`/`month=`/`day=` — Hive-style partition segments (UTC date of the
  flight's *first* message, matching the flight object's own key — see
  [S3 Object Format](#s3-object-format) for why first_message, not
  last_message, is what both keys have to agree on) — Athena partition
  projection assumes this layout by default, with no
  `storage.location.template` table property required.
- `{uuid}` — the flight's `_id` (UUID-v7), matching the flight object's own
  key.

| Column | Type | Description |
|--------|------|--------------|
| `icao_hex` | string | Aircraft ICAO hex |
| `registration` | string | Aircraft registration, if known |
| `type_designator` | string | ICAO aircraft type designator (e.g. `B763`), if known |
| `military` | boolean | Non-nullable — absent on the source record normalizes to `false` |
| `operator_designator` | string | Operator ICAO designator, if known |
| `ident` | string | Flight ident/callsign, if known |
| `first_message` | timestamp (UTC) | Timestamp of the flight's first message |
| `last_message` | timestamp (UTC) | Timestamp of the flight's last message |
| `s3_key` | string | The S3 object key of the matching flight record |

There's no `_id`/UUID column — the uuid lives in the S3 key itself. This
schema is the authoritative source in
[specs/data-dictionary.yaml](../specs/data-dictionary.yaml)'s
`archive_parquet_index` record.

Writing one small file per flight (rather than appending to one shared
file) is deliberate: it's what makes this index queryable directly from S3
via Athena/Glue partition projection, with no local-only, single-instance
state to lose or rebuild. Daily compaction of these small per-flight files
into one file per partition, and the Glue table/partition projection setup
itself, are separate, not-yet-built pieces of work.

## Fault Tolerance

When S3 is unavailable — at startup or during operation — completed flights
are written to `s3.db` (SQLite, in `data_dir`) instead. RabbitMQ connection
failures are retried every 10 seconds independently of the S3 fallback
logic.

There are actually two fallback queues in that same `s3.db` file (separate
tables): the main one for flights that couldn't be written at all, and a
second (`index_queue`) for flights whose object write succeeded but whose
Parquet index write failed — the flight object write and the Parquet index
write are two separate S3 calls, so it's possible for the first to succeed
while the second fails (a transient error, a permissions issue scoped to
the `index/` prefix, etc.). Unlike the flight object itself, a lost index
row has no self-healing recovery path: nothing ever rescans S3 to notice a
flight is missing from the index. So a failed index write is queued — as
`{flight_json, s3_key}` — and retried without re-archiving the flight
object itself.

Both queues are drained the same way, on two triggers: whenever the
background S3-connectivity thread detects a reconnect (checked every 10
seconds), and independently, once per `telemetry_interval_seconds`. Only
the index queue strictly needs the second trigger — it can fill even while
S3 never registers as fully disconnected, so the reconnect-based trigger
alone wouldn't be enough for it. The flight queue only ever fills while S3
*is* known to be down, so the reconnect trigger is sufficient for it in
theory — but it gets the periodic trigger too, since checking an empty
queue costs nothing and a periodic sweep is a strictly stronger guarantee
than relying solely on an edge-triggered "was down, now up" detection.
Retrying an already-drained row twice (if both triggers race) is harmless
either way — reprocessing the same flight, or rewriting the same Parquet
key, just overwrites with identical content.

![Flight & Parquet index write, with retry](./archive-write-sequence.svg)

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
| `flights_skipped_hour` | integer | MLAT-only flights dropped instead of archived this hour |
| `flights_skipped_today` | integer | MLAT-only flights dropped instead of archived today, UTC |
| `s3_connected` | boolean | Current S3 connectivity state |
| `local_queue_depth` | integer | Flights currently queued in `s3.db` fallback |
| `local_index_queue_depth` | integer | Parquet index rows currently queued for retry (`index_queue` table in `s3.db`) |
| `rabbitmq_archive_queue_depth_hwm` | integer | High-water mark of the RabbitMQ `archive` queue's depth since the last publish; sampled at most once every 10 seconds, resets on publish (`-1` if no valid sample landed this window) |

All statistics are published as a single retained JSON payload every
`telemetry_interval_seconds`. Home Assistant autodiscovery payloads are
published to `homeassistant/sensor/SkyFollower_archive_{field}/config` on
MQTT connect, each using `value_template` to extract its field from the
shared statistics topic.

`rabbitmq_archive_queue_depth_hwm` is sampled by a dedicated background
loop capped at once every 10 seconds, independent of how low
`telemetry_interval_seconds` is configured, and tracked as a high-water
mark that resets each time telemetry is published.

![RabbitMQ queue-depth high-water mark](./rmq-queue-depth-hwm-sequence.svg)
