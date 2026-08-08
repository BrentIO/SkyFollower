# archive-compaction

| | |
|---|---|
| **Purpose** | Daily consolidation of the archive's per-flight Parquet index files into one file per day, so Athena/Glue partition projection isn't scanning thousands of tiny files per partition indefinitely |
| **Run frequency** | Daily, scheduled by `ofelia` |
| **Reads/writes** | AWS S3 only — no Redis, no RabbitMQ |

## How it works

The archive processor writes one small Parquet index row per flight to
`index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet` alongside the
flight's own `flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz`
object (see `archive-processor/README.md` and
`specs/data-dictionary.yaml`'s `archive_parquet_index` record). Each run of
this job:

1. Reads the last successfully compacted date — the **watermark** — from
   `_compaction_state/watermark.json` in S3 (a sibling prefix to `flights/`
   and `index/`, never nested inside either, so Glue's partition projection
   template never mistakes it for a partition file). A missing watermark
   (first run ever) is treated as "nothing compacted yet."
2. Walks forward one date at a time, from the day after the watermark up to
   **the day before yesterday** (UTC, not yesterday — this absorbs
   `flight_ttl_seconds` archival delay and any lag from the archive
   processor's local `s3.db` offline-fallback queue draining late). A
   single run can therefore clear a multi-day backlog once whatever stalled
   it is fixed, rather than crawling forward one day per scheduled run.
3. Before compacting each date, verifies parity: every flight object under
   that date's `flights/` prefix must have a matching Parquet index row
   under its `index/` prefix, matched by the UUID embedded in each key. A
   mismatch — a flight archived with no index row ever landing for it —
   **stops the loop at that date**; nothing later is attempted either, and
   the watermark stays exactly where it was. This is deliberately different
   from the late-straggler case below: a late straggler is a file this run
   simply hasn't seen yet and will pick up on its own once seen; a parity
   mismatch means a date was actually checked and is still missing a row,
   which won't fix itself by moving on to the next date.
4. For a date that passes the parity check: lists every object under that
   day's `index/year=/month=/day=/` prefix, filtering out any file already
   produced by a previous compaction run (identified by a `compacted-`
   filename prefix — a per-flight file is always a bare UUID, so this can
   never collide with one), reads and merges the remaining per-flight files
   into a single Arrow table, and writes it as one new
   `compacted-{uuid}.parquet` file under the same partition.
5. Deletes only the source files that were actually read into that output
   — never a file that failed to read, and never a file that arrived under
   the prefix after the initial listing (a late straggler). Both cases are
   left in place: an extra small file in the partition, still queryable on
   its own via Glue's partition projection (which reads every file under a
   partition as one table), with no duplication risk. The watermark only
   advances past a date once its compaction step actually completes.

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
| `data_dir` | string | `"/app/data"` | Host-mounted directory where `aws-setup/iam-policy.json` (resolved AWS reference file — see [AWS Setup](#aws-setup)) is written |
| `log_level` | string | `"info"` | Set to `"debug"` for verbose output |

The settings file path defaults to `/app/settings.json` and can be
overridden with the `SETTINGS_PATH` environment variable.

## AWS Setup

**No IAM identity for this job exists yet in AWS.** It only ever prepares
a *local reference file* an operator uses to create one by hand; it never
calls a Glue, IAM, or Athena provisioning API itself, and identity
creation is not something this project automates anywhere. On every run,
it resolves its own `__BUCKET_NAME__`-templated IAM policy (baked into its
image from `specs/aws/iam-policies/archive-compaction.json`) against its
configured `s3.bucket` and writes it to `{data_dir}/aws-setup/iam-policy.json`,
for pasting directly into the console's JSON policy editor. This is a
deliberately separate identity from `archive-processor`'s — it needs
`Delete` and bucket-level `List` permissions archive-processor doesn't,
and no access to `flights/*` object content at all (it only lists
`flights/*` keys for the parity check, never reads them). See
[docs/aws-setup.md](../docs/aws-setup.md) for the full console click-path
setup guide — including the Glue database/table and Athena workgroup this
job's own permissions assume already exist, none of which this job (or
any other component) provisions either.

## MQTT

Published once, at the end of a run, to
`SkyFollower/archive-compaction/statistic/{name}` (all retained):

| Topic suffix | Value | Format |
|---|---|---|
| `files_compacted` | e.g. `142` | Integer as string — per-flight files merged into this run's compacted output(s), summed across every date compacted this run |
| `files_delete_failed` | e.g. `0` | Integer as string — files that were included in a compacted output but whose delete call failed, and therefore still linger as duplicates, summed across every date compacted this run |
| `days_compacted` | e.g. `1` | Integer as string — number of date partitions successfully compacted this run (more than one during catch-up after a gap) |
| `last_compacted_date` | e.g. `2026-07-23` | The watermark after this run — the most recent date whose partition has been fully compacted |
| `mismatch_date` | e.g. `2026-07-24`, or empty | The date this run stopped at due to a flight/index parity mismatch; empty when the run wasn't stopped by one |
| `mismatch_uuids` | e.g. `0198abcd-...,0198abce-...`, or empty | Comma-separated flight UUIDs missing their index row on `mismatch_date` — a starting point for manual investigation. Check `local_index_queue_depth` on the archive processor's own stats: nonzero means the row is likely still draining locally and will resolve on its own; zero means it's genuinely lost from the archive processor's perspective |
| `last_run_at` | e.g. `2026-07-25T04:50:03.123456+00:00` | ISO 8601 UTC |
| `last_run_status` | `Success`, `Failure`, or `Mismatch` | String — `Mismatch` means the run completed without error but stopped early on a parity mismatch (see `mismatch_date`/`mismatch_uuids`); `Failure` means an actual exception (S3 error, etc.) |

Home Assistant autodiscovery configs are also published (retained) to
`homeassistant/sensor/SkyFollower_archive_compaction_{name}/config` for
each of the eight stats above.

## Deployment

This job needs its own S3 credentials — no Redis, no RabbitMQ — so it's
scheduled by a dedicated `ofelia` instance on the archive host
(`docker-compose.archive.yaml`) rather than the core's `ofelia`
(which only schedules Redis-facing data runners, and has no AWS
credentials configured anywhere on that host today). Its own IAM
identity is deliberately separate from `archive-processor`'s, not a shared
or widened one — see [AWS Setup](#aws-setup) above for exactly what it
needs and why (`Delete` and bucket-level `List`, which `archive-processor`
doesn't need; no access to `flights/*` content at all). See the `ofelia`
service's labels in `docker-compose.archive.yaml` for the schedule.
