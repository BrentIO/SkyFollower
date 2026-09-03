# Archive Compaction

| | |
|---|---|
| **Purpose** | Daily consolidation of the archive's per-flight Parquet index files into one file per day, so Athena/Glue partition projection isn't scanning thousands of tiny files per partition indefinitely |
| **Run frequency** | Daily, scheduled by `ofelia` |
| **Reads/writes** | AWS S3, plus a local index-cache volume shared with `archive-processor` (see [Local Index Cache](#local-index-cache)) — no Redis, no RabbitMQ |

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
   day's `index/year=/month=/day=/` prefix (via S3, always — parity
   checking and listing never consult the local cache), filtering out any
   file already produced by a previous compaction run (identified by a
   `compacted-` filename prefix — a per-flight file is always a bare UUID,
   so this can never collide with one), reads and merges the remaining
   per-flight files into a single Arrow table, and writes it as one new
   `compacted-{uuid}.parquet` file under the same partition. Each
   per-flight file's *content* is read from the local index cache when
   present, falling back to a real S3 `GetObject` only when it's missing —
   see [Local Index Cache](#local-index-cache).
5. Deletes only the source files that were actually read into that output
   — never a file that failed to read, and never a file that arrived under
   the prefix after the initial listing (a late straggler). Both cases are
   left in place: an extra small file in the partition, still queryable on
   its own via Glue's partition projection (which reads every file under a
   partition as one table), with no duplication risk. The watermark only
   advances past a date once its compaction step actually completes. A
   file's local index-cache copy (if any) is removed the moment its S3
   counterpart is confirmed deleted — see [Local Index
   Cache](#local-index-cache).

A file that's left behind — whether a late straggler or one whose delete
call failed after being included in a compacted output — is not retried by
a later run targeting a different date. This is an accepted, self-healing
edge case (see `specs/data-dictionary.yaml`), not a bug: it costs one extra
small file in that day's partition, not incorrect query results, except in
the narrow case of a post-inclusion delete failure, where the row is
present twice until someone manually removes the lingering source file —
logged clearly via `files_delete_failed` when it happens.

## Local Index Cache

`archive-processor` and `archive-compaction` run on the same host
(`docker-compose.archive.yaml`) and both bind-mount the same host
directory at `/app/index-cache`. `archive-processor` writes a local copy
of each per-flight Parquet index row there right after uploading it to S3;
this job reads from that local copy instead of downloading the row again,
issuing a real S3 `GetObject` only when the local copy is unexpectedly
missing (a partial/failed write on `archive-processor`'s side, or a row
written before this cache existed) — see
[archive-processor/README.md](../archive-processor/README.md#local-index-cache)
for the write side.

The local path mirrors the S3 key's own layout, minus the `index/` prefix:
`/app/index-cache/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet` — so a
multi-day catch-up run (see step 2 above) naturally finds each backlogged
date's local files under its own subdirectory, with no extra bookkeeping
for how many days are outstanding.

Only the *read* path (step 4 above) consults the local cache.
`check_date_parity` (step 3) and the post-compaction delete (step 5) are
both S3-only, exactly as before — the local cache is purely a faster way
to get bytes this job would otherwise download, never a second source of
truth about what exists. A file's local copy is removed the moment its S3
counterpart is confirmed deleted, so the shared volume only ever holds the
not-yet-compacted backlog rather than growing without bound; if the S3
delete itself fails (see `files_delete_failed` above), the local copy is
left in place along with the lingering S3 source.

## Configuration

Reads its configuration from environment variables via `shared/config.py`'s
`load_config("mqtt", "s3")`, interpolated by Compose from this host's `.env`
(written by `scripts/install.sh`). No Redis, no RabbitMQ -- see the table
at the top of this page.

| Variable | Required | Default | Description |
|---|---|---|---|
| `S3_BUCKET` | ✅ | — | S3 bucket the archive processor writes flights/index rows to |
| `AWS_DEFAULT_REGION` | ✅ | — | boto3's own variable name -- no credentials are ever passed in code, so every client picks up the default credential chain. Shared with `archive-processor` |
| `AWS_ACCESS_KEY_ID` | ✅ | — | boto3's own variable name. `docker-compose.archive.yaml` maps this from the `.env` key `ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID` -- a credential pair distinct from `archive-processor`'s, so this job runs under its own least-privilege IAM identity (see [AWS Setup](#aws-setup)). The Ofelia scheduled-run label maps the same key |
| `AWS_SECRET_ACCESS_KEY` | ✅ | — | boto3's own variable name. Mapped from the `.env` key `ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY` |
| `MQTT_HOST` | ❌ | — | Leave unset to disable MQTT entirely |
| `MQTT_PORT` | ❌ | `1883` | |
| `MQTT_USERNAME` | ❌ | — | Optional MQTT auth; leave unset for an anonymous broker |
| `MQTT_PASSWORD` | ❌ | — | |
| `LOG_LEVEL` | ❌ | `info` | `"debug"` for verbose output |

## AWS Setup

**No IAM identity for this job exists yet in a fresh AWS account** — nor
the bucket, Glue database/table, or Athena workgroup. All of it is created
by the one-shot `aws-setup` container, which deploys a CloudFormation stack
from `specs/aws/cloudformation.yaml`; `scripts/install.sh` runs it when you
install the `archive` role. See [docs/aws-configuration.md](../docs/aws-configuration.md).

This job's identity is deliberately separate from `archive-processor`'s: it
needs `s3:DeleteObject` on `index/*` and bucket-level `s3:ListBucket` that
archive-processor doesn't, and it has **no access to `flights/*` object
content at all** — it only lists `flights/*` keys for the parity check,
never reads them. It also gets `s3:GetObject`/`s3:PutObject` on
`_compaction_state/*` for the watermark. That split is only actually
reachable now that the archive host accepts two credential pairs (the
`ARCHIVE_PROCESSOR_AWS_*` / `ARCHIVE_COMPACTION_AWS_*` keys); before, both
services shared one credential and one of them was always over- or
under-privileged.

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

Ofelia's scheduled run creates its own container directly through the
Docker Engine API rather than reusing this file's `archive-compaction`
service definition, so the shared index-cache mount (see [Local Index
Cache](#local-index-cache)) is declared a second time as its own
`ofelia.job-run.archive-compaction.volume` label, resolving to the same
host directory as `archive-processor`'s `/app/index-cache` mount. The
manual `docker compose run --rm archive-compaction` path uses the service
definition's own `volumes:` entry instead — both paths land on the same
host directory either way.
