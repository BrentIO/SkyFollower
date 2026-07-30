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
in Redis (shared with the message processor) once at startup and cached; not
hot-reloaded, restart the container to pick up a changed value. Defaults to
`300` if unset. See [Split-Flight Stitching](#split-flight-stitching) below
for how it's used.

## Consuming from RabbitMQ

The archive processor declares and consumes from a single durable queue
named `archive` (`prefetch_count=1`, manual ack). This is the queue the
message processor publishes completed flights to — see
[message-processor/README.md](../message-processor/README.md).
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

`force_archive: true` on the flight (set by the message processor when any matched
rule in `config:rules` carries a `force_archive: true` property — see
[message-processor/README.md](../message-processor/README.md)) overrides the skip for
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

Resizing a deployment's message processor count reshuffles which message
processor an aircraft routes to, which can force a flight to be archived
early even though the aircraft keeps flying — the continuation shows up as
a second, separate flight on whichever message processor it's now routed
to. The archive processor detects and merges this after the fact:

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
- A gap beyond `flight_ttl_seconds`, a *negative* gap, or no pointer at
  all means this is treated as a genuinely new flight; it's archived
  normally and a fresh pointer is written. Three or more consecutive
  artificial splits chain correctly — each stitches into the same
  original segment, not a new one each time.
- A negative gap means the pointer found is for a segment that actually
  started *after* the flight being archived now — this flight arrived out
  of order (e.g. it failed on its first write attempt and sat in the
  local retry queue while its own continuation raced ahead and archived
  normally in the meantime). Merging always takes `first_message` from
  the pointed-to segment and leaves `last_message` from the segment being
  processed, which assumes the pointed-to segment is chronologically
  earlier — merging backwards would silently produce (and write to S3,
  overwriting the correct object already there) a record with
  `last_message` before `first_message`, so this case is rejected the
  same way a too-large gap already is, rather than attempting the merge.
  This is a known, accepted limitation: an out-of-order arrival like this
  is rare enough (it needs an S3 write failure that survives the AWS SDK's
  own internal retries, *and* a same-aircraft continuation landing within
  one `telemetry_interval_seconds` tick of that failure) that the result —
  two separate archived records instead of one merged one — is treated as
  an acceptable outcome rather than something worth adding cross-thread
  coordination between the live write path and the local retry queue to
  prevent.
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

Both queues are retried on the same two triggers — whenever the background
S3-connectivity thread detects a reconnect (checked every 10 seconds), and
independently, once per `telemetry_interval_seconds` — but the *flight*
queue's reconnect-triggered drain works differently from every other
drain, on purpose.

**The flight queue, on reconnect, drains synchronously before
`s3_connected` is set.** A live flight is only ever routed directly to S3
once `s3_connected` is `True`; a continuation of a still-archiving flight
(a resize-induced split, see [Split-Flight
Stitching](#split-flight-stitching) below) has to look up a pointer written
by the segment it continues. If the reconnect-triggered drain ran in the
background the way every other drain does, a continuation segment could
arrive live and go straight to S3 — missing that pointer, since the
segment it continues might still be sitting undrained in the backlog — and
archive as an independent flight instead of stitching, silently splitting
one flight into two S3 objects. Running this specific drain synchronously,
and only flipping `s3_connected` to `True` once it's fully empty, closes
that race by construction: any flight arriving on the RabbitMQ consumer
thread while the drain is still running still sees `s3_connected == False`
and queues behind the backlog rather than going live — and since the queue
drains strictly oldest-first, a continuation can never be processed before
whatever it continues. No per-aircraft locking or queue scanning needed,
and RabbitMQ consumption itself never stalls (a queued-not-drained flight
is still just a fast local SQLite insert). If the drain stops early (S3
goes down again mid-drain), `s3_connected` stays `False` and the whole
sequence — reconnect, then this drain — retries on the next 10-second tick,
picking up wherever the queue was left. This same gate also covers a
leftover backlog found at startup (e.g. after a crash mid-outage), not just
a live reconnect, since `start()` runs it before consuming anything.

Every other drain — the flight queue's periodic `telemetry_interval_seconds`
safety sweep, and the index queue on *either* trigger — still runs the way
it always has: `drain_in_background()` spawns the actual drain on a
background thread and returns immediately, so a slow drain never delays
that cycle's telemetry publish. The index queue never participates in the
stitch race above (it only retries a Parquet index row for a flight object
that already wrote successfully), so it's unaffected either way. By the
time `s3_connected` first becomes `True`, the flight queue is already
guaranteed empty (that's the point of the gate above) — so its periodic
sweep only ever finds something to do for a flight that failed and was
re-queued by a single transient write error while otherwise connected — a
narrower race than the reconnect-window one above, tracked separately
since closing it needs its own design discussion (whether that's worth a
similar gate, given it'd have to be per-aircraft rather than a simple
global flip, since `s3_connected` never goes false in that scenario).

Each queue has its own single-flight guard (a non-blocking lock,
independent per queue), ensuring at most one drain is ever in progress
*for that queue* regardless of which trigger started it — draining the
flight queue never blocks a concurrent drain of the index queue, but two
overlapping drains of the *same* queue (e.g. the periodic tick firing while
a background drain is still working through a backlog) would otherwise
both select the same oldest row before either deletes it, genuinely
duplicate-processing that row — a duplicate archived flight or a duplicate
Parquet index write, not just a harmless retry. The synchronous
reconnect-triggered flight-queue drain doesn't use this guard at all — it
runs inline on the S3-reconnect thread itself, before `s3_connected` (and
therefore the periodic sweep's own trigger condition) ever becomes `True`,
so there's nothing for it to overlap with.

![Flight & Parquet index write, with retry](./archive-write-sequence.svg)

## MQTT Topics Published

All topics use the root `SkyFollower`.

| Topic | Payload | Retained |
|-------|---------|----------|
| `SkyFollower/archive/status` | `ONLINE` or `OFFLINE` | Yes |
| `SkyFollower/archive/statistic/{name}` | One retained topic per stat (see fields below) | Yes |

**Statistic topic suffixes (`{name}`):**

| Field | Format | Description |
|-------|--------|-------------|
| `started_at` | UTC ISO-8601 timestamp | Process start time |
| `flights_archived_hour` | Integer as string | Flights successfully written to S3 this hour |
| `flights_archived_today` | Integer as string | Flights successfully written to S3 today (UTC) |
| `flights_skipped_hour` | Integer as string | MLAT-only flights dropped instead of archived this hour |
| `flights_skipped_today` | Integer as string | MLAT-only flights dropped instead of archived today, UTC |
| `s3_connected` | `True` or `False` | Current S3 connectivity state |
| `local_queue_depth` | Integer as string | Flights currently queued in `s3.db` fallback |
| `local_index_queue_depth` | Integer as string | Parquet index rows currently queued for retry (`index_queue` table in `s3.db`) |
| `rabbitmq_archive_queue_depth_hwm` | Integer as string | High-water mark of the RabbitMQ `archive` queue's depth since the last publish; sampled at most once every 10 seconds, resets on publish (`-1` if no valid sample landed this window) |

Each stat is published as its own retained topic (not a combined JSON
payload) every `telemetry_interval_seconds`. Home Assistant autodiscovery
payloads are published to
`homeassistant/sensor/SkyFollower_archive_{field}/config` on MQTT connect;
each sensor's `state_topic` points directly at its own
`SkyFollower/archive/statistic/{field}` topic — no `value_template` needed.

`rabbitmq_archive_queue_depth_hwm` is sampled by a dedicated background
loop capped at once every 10 seconds, independent of how low
`telemetry_interval_seconds` is configured, and tracked as a high-water
mark that resets each time telemetry is published.

![RabbitMQ queue-depth high-water mark](./rmq-queue-depth-hwm-sequence.svg)
