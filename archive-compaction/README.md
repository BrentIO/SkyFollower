# archive-compaction

| | |
|---|---|
| **Purpose** | Daily consolidation of the archive's per-flight Parquet index files into one file per day, so Athena/Glue partition projection isn't scanning thousands of tiny files per partition indefinitely |
| **Run frequency** | Daily, scheduled by `ofelia` |
| **Reads/writes** | AWS S3 only — no Redis, no RabbitMQ |

## How it works

The archive processor writes one small Parquet index row per flight to
`index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet` (see
`archive-processor/README.md` and `specs/data-dictionary.yaml`'s
`archive_parquet_index` record). Each run of this job:

1. Computes the target partition: **the day before yesterday**, in UTC —
   not yesterday — so `flight_ttl_seconds` archival delay and any lag from
   the archive processor's local `s3.db` offline-fallback queue draining
   late don't cause a flight to be missed.
2. Lists every object under that day's `index/year=/month=/day=/` prefix,
   filtering out any file already produced by a previous compaction run
   (identified by a `compacted-` filename prefix — a per-flight file is
   always a bare UUID, so this can never collide with one).
3. Reads and merges the remaining per-flight files into a single Arrow
   table, and writes it as one new `compacted-{uuid}.parquet` file under
   the same partition.
4. Deletes only the source files that were actually read into that output
   — never a file that failed to read, and never a file that arrived under
   the prefix after the initial listing (a late straggler). Both cases are
   left in place: an extra small file in the partition, still queryable on
   its own via Glue's partition projection (which reads every file under a
   partition as one table), with no duplication risk.

A file that's left behind — whether a late straggler or one whose delete
call failed after being included in a compacted output — is not retried by
a later run targeting a different date. This is an accepted, self-healing
edge case (see `specs/data-dictionary.yaml`), not a bug: it costs one extra
small file in that day's partition, not incorrect query results, except in
the narrow case of a post-inclusion delete failure, where the row is
present twice until someone manually removes the lingering source file —
logged clearly via `files_delete_failed` when it happens.

## Configuration (`settings.json`)

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `s3.access_key_id` | string | — | AWS access key ID |
| `s3.secret_access_key` | string | — | AWS secret access key |
| `s3.region` | string | `"us-east-1"` | AWS region for the S3 bucket |
| `s3.bucket` | string | — | S3 bucket the archive processor writes flights/index rows to |
| `mqtt.host` | string | — | MQTT broker hostname (omit the whole `mqtt` block to skip completion-stats publishing entirely) |
| `mqtt.port` | integer | `1883` | |
| `mqtt.username` | string | — | Optional MQTT auth; omit for an anonymous broker |
| `mqtt.password` | string | — | |
| `log_level` | string | `"info"` | Set to `"debug"` for verbose output |

The settings file path defaults to `/app/settings.json` and can be
overridden with the `SETTINGS_PATH` environment variable.

## MQTT

Published once, at the end of a run, to
`SkyFollower/runner/archive-compaction/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `files_compacted` | e.g. `142` | Integer as string — per-flight files merged into this run's compacted output |
| `files_delete_failed` | e.g. `0` | Integer as string — files that were included in the compacted output but whose delete call failed, and therefore still linger as duplicates |
| `last_run_at` | e.g. `2026-07-25T04:50:03.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `success` or `failure` | String |

Home Assistant autodiscovery configs are also published (retained) to
`homeassistant/sensor/SkyFollower_archive_compaction_{name}/config` for
each of the four stats above.

## Deployment

This job needs the same S3 credentials as `archive-processor` and nothing
else — no Redis, no RabbitMQ — so it's scheduled by a dedicated `ofelia`
instance on the archive host (`docker-compose.archive.yaml`) rather than
the central server's `ofelia` (which only has Redis-facing data runners in
its `depends_on` list, and no AWS credentials configured anywhere on that
host today). See `config/archive/ofelia-config.ini.example` for the
schedule.
