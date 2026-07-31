#!/usr/bin/env python3
"""
SkyFollower Archive Compaction

Daily job that consolidates each day's small per-flight Parquet index files
(index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet, written one per
flight by archive-processor) into one file per partition, so Athena/Glue
partition projection isn't scanning thousands of tiny files per day
indefinitely.

Tracks a `_compaction_state/watermark.json` "last compacted date" in S3 and
walks forward one date at a time from watermark+1 up to today-2 (UTC) --
absorbing flight_ttl_seconds archival delay and any lag from the archive
processor's offline s3.db fallback draining late -- so a single run can
clear a multi-day backlog once whatever stalled it is fixed, rather than
advancing one day per scheduled run regardless of how far behind it is.

Before compacting each date, verifies every flight object under that date's
flights/ prefix has a matching Parquet index row under its index/ prefix.
A mismatch stops the loop at that date (nothing later is attempted either)
and leaves the watermark exactly where it was, rather than silently
compacting an index that's missing rows.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import date, datetime, timedelta, timezone

import boto3
import paho.mqtt.client as mqtt
import pyarrow as pa
import pyarrow.parquet as pq

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-compaction or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.aws_setup import write_aws_setup_files
from shared.logging_setup import configure_logging
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("archive-compaction")

MQTT_ROOT = "SkyFollower/archive-compaction"

# Template resolved (__BUCKET_NAME__ substitution only, no AWS API calls)
# and written to {data_dir}/aws-setup/ on every run -- see
# shared/aws_setup.py and docs/aws-setup.md.
_IAM_POLICY_TEMPLATE = os.path.join(
    os.path.dirname(__file__), "..", "specs", "aws", "iam-policies", "archive-compaction.json"
)

# Matches archive-processor's _PARQUET_INDEX_SCHEMA exactly (see
# archive-processor/main.py) -- every per-flight file this job reads was
# written with this schema, and the consolidated output preserves it.
_PARQUET_INDEX_SCHEMA = pa.schema([
    pa.field("icao_hex", pa.string()),
    pa.field("registration", pa.string()),
    pa.field("type_designator", pa.string()),
    pa.field("military", pa.bool_(), nullable=False),
    pa.field("operator_designator", pa.string()),
    pa.field("ident", pa.string()),
    pa.field("first_message", pa.timestamp("us", tz="UTC")),
    pa.field("last_message", pa.timestamp("us", tz="UTC")),
    pa.field("s3_key", pa.string()),
])

# Per-flight files are named "{uuid}.parquet" (a bare UUID-v7, no other
# prefix -- see archive-processor's build_index_s3_key()). Consolidated
# output from this job always uses this prefix instead, so a later run
# never mistakes a previous run's output for a per-flight file and re-reads
# rows that have already been compacted and had their sources deleted.
_COMPACTED_PREFIX = "compacted-"

# Sibling to flights/ and index/, not nested inside either -- so Glue's
# year=/month=/day= partition projection template never mistakes this for
# a partition file.
_WATERMARK_KEY = "_compaction_state/watermark.json"


# ---------------------------------------------------------------------------
# Partition targeting
# ---------------------------------------------------------------------------

def _utc_today(now: datetime | None = None) -> date:
    return (now or datetime.now(timezone.utc)).astimezone(timezone.utc).date()


def _cutoff_date(now: datetime | None = None) -> date:
    """
    Latest date this job will ever compact: today - 2, UTC. Not yesterday
    -- this absorbs flight_ttl_seconds archival delay and any lag from the
    archive processor's local s3.db offline-fallback queue draining late,
    so a flight that logically belongs to that day but was archived a bit
    late is still present before compaction runs.
    """
    return _utc_today(now) - timedelta(days=2)


def index_prefix_for_date(d: date) -> str:
    return (
        f"index/year={d.strftime('%Y')}/"
        f"month={d.strftime('%m')}/"
        f"day={d.strftime('%d')}/"
    )


def flights_prefix_for_date(d: date) -> str:
    return f"flights/{d.strftime('%Y')}/{d.strftime('%m')}/{d.strftime('%d')}/"


def target_partition_prefix(now: datetime | None = None) -> str:
    """
    The index/ prefix for the cutoff date (today - 2, UTC) -- the single
    date this job used to always target before the watermark-driven
    catch-up loop (run_compaction) existed. Kept as a thin wrapper around
    index_prefix_for_date/_cutoff_date since it's still the right prefix
    for "the latest date we'd ever compact right now."
    """
    return index_prefix_for_date(_cutoff_date(now))


def is_per_flight_file(key: str) -> bool:
    """True for a small per-flight index file (bare-UUID basename), False
    for a previous run's compacted output (compacted-* basename)."""
    basename = key.rsplit("/", 1)[-1]
    return not basename.startswith(_COMPACTED_PREFIX)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def connect_s3(s3_cfg: dict):
    session = boto3.Session(
        aws_access_key_id=s3_cfg.get("access_key_id"),
        aws_secret_access_key=s3_cfg.get("secret_access_key"),
        region_name=s3_cfg.get("region", "us-east-1"),
    )
    return session.client("s3")


def list_partition_objects(s3_client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_parquet_table(s3_client, bucket: str, key: str) -> pa.Table:
    response = s3_client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return pq.read_table(io.BytesIO(body))


def build_compacted_key(prefix: str) -> str:
    return f"{prefix}{_COMPACTED_PREFIX}{uuid.uuid4()}.parquet"


def delete_keys(s3_client, bucket: str, keys: list[str]) -> int:
    """Batch-delete `keys` (up to 1000 per API call, the S3 limit). Returns
    the count of individual failures reported in the response's Errors
    list, plus every key in a chunk whose whole request call raised."""
    failed = 0
    for i in range(0, len(keys), 1000):
        chunk = keys[i:i + 1000]
        try:
            response = s3_client.delete_objects(
                Bucket=bucket,
                Delete={"Objects": [{"Key": k} for k in chunk], "Quiet": True},
            )
        except Exception as exc:
            logger.warning("Batch delete failed for %d keys: %s", len(chunk), exc)
            failed += len(chunk)
            continue
        errors = response.get("Errors", [])
        for err in errors:
            logger.warning("Failed to delete %s: %s", err.get("Key"), err.get("Message"))
        failed += len(errors)
    return failed


# ---------------------------------------------------------------------------
# Flight / index parity
# ---------------------------------------------------------------------------

def _uuid_from_flight_key(key: str) -> str | None:
    """
    Extract the flight UUID from a flights/ object key
    (flights/{YYYY}/{MM}/{DD}/{icao_hex}_{ident}_{uuid}.json.gz). icao_hex
    and ident never contain underscores themselves (icao_hex is hex
    digits; ident is sanitized to alnum-only in archive-processor's
    build_s3_key), so splitting the basename on "_" always yields exactly
    three segments. Returns None for a key that doesn't match this shape.
    """
    basename = key.rsplit("/", 1)[-1]
    if not basename.endswith(".json.gz"):
        return None
    parts = basename[: -len(".json.gz")].split("_")
    if len(parts) != 3:
        return None
    return parts[2]


def _uuid_from_index_key(key: str) -> str | None:
    """
    Extract the flight UUID from a per-flight index/ object key
    (index/year=/month=/day=/{uuid}.parquet). Returns None for an
    already-compacted file (compacted-* basename) or a key that otherwise
    doesn't match this shape.
    """
    if not is_per_flight_file(key):
        return None
    basename = key.rsplit("/", 1)[-1]
    if not basename.endswith(".parquet"):
        return None
    return basename[: -len(".parquet")]


def check_date_parity(s3_client, bucket: str, d: date) -> set[str]:
    """
    Return the set of flight UUIDs present under `d`'s flights/ prefix with
    no matching Parquet index row under its index/ prefix -- exactly the
    flights that would be missing from the index forever if this date were
    compacted as-is. An empty set means a clean match (safe to compact).

    Not checked in the other direction: an index row with no matching
    flight object doesn't lose any data when compacted, so it isn't a
    reason to block compaction here.
    """
    flight_keys = list_partition_objects(s3_client, bucket, flights_prefix_for_date(d))
    index_keys = list_partition_objects(s3_client, bucket, index_prefix_for_date(d))

    flight_uuids = {u for k in flight_keys if (u := _uuid_from_flight_key(k))}
    index_uuids = {u for k in index_keys if (u := _uuid_from_index_key(k))}

    return flight_uuids - index_uuids


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def read_watermark(s3_client, bucket: str) -> date | None:
    """
    Read the last successfully compacted date from
    _compaction_state/watermark.json. Returns None if the object doesn't
    exist yet (first run ever) or can't be read/parsed for any other
    reason -- either way, the caller treats an absent watermark as
    "nothing compacted yet" rather than failing the whole run over it.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=_WATERMARK_KEY)
        data = json.loads(response["Body"].read())
        return datetime.strptime(data["last_compacted_date"], "%Y-%m-%d").date()
    except Exception:
        return None


def write_watermark(s3_client, bucket: str, d: date) -> None:
    body = json.dumps({"last_compacted_date": d.strftime("%Y-%m-%d")}).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=_WATERMARK_KEY,
        Body=body,
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

def compact_partition(s3_client, bucket: str, prefix: str) -> dict:
    """
    Compact one day's partition: read every per-flight Parquet file under
    `prefix`, write one consolidated file, then delete only the source
    files that were actually read into it.

    Write-then-delete, and only ever delete a key that was successfully
    read into the compacted output -- a file that fails to read is left in
    place (absent from the compacted output, so deleting it would lose
    data), and a file that lands under this prefix after the initial
    listing (a late straggler) is simply never seen by this run. Both
    cases are the same accepted, self-healing shape: an extra small file
    left in the partition, queryable on its own, no duplication risk.
    """
    all_keys = list_partition_objects(s3_client, bucket, prefix)
    source_keys = [k for k in all_keys if is_per_flight_file(k)]

    if not source_keys:
        logger.info("No per-flight files to compact under %s.", prefix)
        return {"files_compacted": 0, "files_delete_failed": 0}

    tables = []
    included_keys = []
    for key in source_keys:
        try:
            tables.append(read_parquet_table(s3_client, bucket, key))
            included_keys.append(key)
        except Exception as exc:
            logger.warning("Skipping unreadable object %s: %s", key, exc)

    if not tables:
        logger.warning("No readable per-flight files under %s; nothing compacted.", prefix)
        return {"files_compacted": 0, "files_delete_failed": 0}

    combined = pa.concat_tables(tables)
    sink = io.BytesIO()
    pq.write_table(combined, sink)

    compacted_key = build_compacted_key(prefix)
    s3_client.put_object(
        Bucket=bucket,
        Key=compacted_key,
        Body=sink.getvalue(),
        ContentType="application/octet-stream",
    )
    logger.info(
        "Wrote %s (%d rows from %d source files).",
        compacted_key, combined.num_rows, len(included_keys),
    )

    files_delete_failed = delete_keys(s3_client, bucket, included_keys)

    return {
        "files_compacted": len(included_keys),
        "files_delete_failed": files_delete_failed,
    }


def run_compaction(s3_client, bucket: str, now: datetime | None = None) -> dict:
    """
    Catch-up loop: starting the day after the watermark (or cutoff - 1 day
    if no watermark exists yet, matching the old fixed single-date
    behavior on a first run), compact one date at a time up to the cutoff
    (today - 2, UTC). Each date is gated by check_date_parity first -- a
    mismatch stops the loop immediately, leaving that date and every later
    one uncompacted and the watermark exactly where it was, so a later run
    resumes at the same stuck date once whatever caused the mismatch
    resolves (the row lands late, or drains from index_queue) instead of
    silently skipping past it.
    """
    cutoff = _cutoff_date(now)
    watermark = read_watermark(s3_client, bucket)
    if watermark is None:
        watermark = cutoff - timedelta(days=1)

    files_compacted = 0
    files_delete_failed = 0
    days_compacted = 0
    mismatch_date: date | None = None
    mismatch_uuids: set[str] = set()

    target = watermark + timedelta(days=1)
    while target <= cutoff:
        missing = check_date_parity(s3_client, bucket, target)
        if missing:
            mismatch_date = target
            mismatch_uuids = missing
            logger.error(
                "Parity mismatch for %s: %d flight(s) missing their index row; "
                "stopping catch-up here. UUIDs: %s",
                target.isoformat(), len(missing), ", ".join(sorted(missing)),
            )
            break

        prefix = index_prefix_for_date(target)
        logger.info("Compacting partition %s", prefix)
        result = compact_partition(s3_client, bucket, prefix)
        files_compacted += result["files_compacted"]
        files_delete_failed += result["files_delete_failed"]
        days_compacted += 1

        watermark = target
        write_watermark(s3_client, bucket, watermark)
        target += timedelta(days=1)

    return {
        "files_compacted": files_compacted,
        "files_delete_failed": files_delete_failed,
        "days_compacted": days_compacted,
        "last_compacted_date": watermark,
        "mismatch_date": mismatch_date,
        "mismatch_uuids": mismatch_uuids,
    }


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(
    cfg: dict,
    result: dict,
    status: str,
) -> None:
    """Publish completion statistics to MQTT, one retained topic per stat.

    `result` is the dict returned by run_compaction() (or the all-zero/None
    default used when the run failed before compaction even started)."""
    mc = cfg.get("mqtt")
    if not mc:
        logger.info("No MQTT config; skipping stats publish.")
        return

    run_at = datetime.now(timezone.utc).isoformat()
    files_compacted = result.get("files_compacted", 0)
    files_delete_failed = result.get("files_delete_failed", 0)
    days_compacted = result.get("days_compacted", 0)
    last_compacted_date = result.get("last_compacted_date")
    mismatch_date = result.get("mismatch_date")
    mismatch_uuids = result.get("mismatch_uuids") or set()

    client = build_mqtt_client(mc)
    connected = False

    def _on_connect(c, userdata, flags, reason_code, properties):
        nonlocal connected
        connected = True

    client.on_connect = _on_connect

    try:
        client.connect(mc["host"], port=mc.get("port", 1883), keepalive=60)
        client.loop_start()

        deadline = time.monotonic() + 5
        while not connected and time.monotonic() < deadline:
            time.sleep(0.05)

        if not connected:
            logger.warning("MQTT connect timed out; skipping stats publish.")
            client.loop_stop()
            return

        base = MQTT_ROOT + "/statistic"
        client.publish(f"{base}/files_compacted", str(files_compacted), retain=True)
        client.publish(f"{base}/files_delete_failed", str(files_delete_failed), retain=True)
        client.publish(f"{base}/days_compacted", str(days_compacted), retain=True)
        client.publish(
            f"{base}/last_compacted_date",
            last_compacted_date.strftime("%Y-%m-%d") if last_compacted_date else "",
            retain=True,
        )
        client.publish(
            f"{base}/mismatch_date",
            mismatch_date.strftime("%Y-%m-%d") if mismatch_date else "",
            retain=True,
        )
        client.publish(
            f"{base}/mismatch_uuids",
            ",".join(sorted(mismatch_uuids)),
            retain=True,
        )
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status, retain=True)

        _publish_ha_autodiscovery(client)

        time.sleep(0.5)
        client.loop_stop()
        client.disconnect()
        logger.info(
            "MQTT stats published (status=%s, files_compacted=%d, files_delete_failed=%d).",
            status, files_compacted, files_delete_failed,
        )

    except Exception as exc:
        logger.warning("MQTT publish failed: %s", exc)
        try:
            client.loop_stop()
        except Exception:
            pass


def _publish_ha_autodiscovery(client: mqtt.Client) -> None:
    device = {
        "ids": "SkyFollower_archive_compaction",
        "name": "SkyFollower Archive Compaction",
        "manufacturer": "P5Software, LLC",
    }
    stats = [
        ("files_compacted", "Archive Compaction Files Compacted", "mdi:file-multiple", "total_increasing", None),
        ("files_delete_failed", "Archive Compaction Delete Failures", "mdi:alert", "total_increasing", None),
        ("days_compacted", "Archive Compaction Days Compacted", "mdi:calendar-check", "measurement", None),
        ("last_compacted_date", "Archive Compaction Last Compacted Date", "mdi:calendar", None, None),
        ("mismatch_date", "Archive Compaction Mismatch Date", "mdi:calendar-alert", None, None),
        ("mismatch_uuids", "Archive Compaction Mismatch Flight UUIDs", "mdi:alert-circle", None, None),
        ("last_run_at", "Archive Compaction Last Run At", "mdi:clock", None, None),
        ("last_run_status", "Archive Compaction Last Run Status", "mdi:check-circle", None, None),
    ]
    for name, friendly_name, icon, state_class, unit in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "unique_id": f"SkyFollower_archive_compaction_{name}",
            "object_id": f"SkyFollower_archive_compaction_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        client.publish(
            f"homeassistant/sensor/SkyFollower_archive_compaction_{name}/config",
            json.dumps(payload),
            retain=True,
        )


# ---------------------------------------------------------------------------
# Config / entry point
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    path = os.environ.get("SETTINGS_PATH", "/app/settings.json")
    with open(path) as f:
        return json.load(f)


def main() -> None:
    try:
        cfg = _load_config()
    except FileNotFoundError as exc:
        configure_logging()
        logger.critical("Settings file not found: %s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

    status = "failure"
    result: dict = {
        "files_compacted": 0,
        "files_delete_failed": 0,
        "days_compacted": 0,
        "last_compacted_date": None,
        "mismatch_date": None,
        "mismatch_uuids": set(),
    }

    try:
        s3_cfg = cfg["s3"]
        bucket = s3_cfg["bucket"]

        write_aws_setup_files(
            cfg.get("data_dir", "/app/data"), bucket,
            {_IAM_POLICY_TEMPLATE: "iam-policy.json"},
        )

        s3_client = connect_s3(s3_cfg)

        result = run_compaction(s3_client, bucket)

        if result["mismatch_uuids"]:
            status = "mismatch"
            logger.warning(
                "Archive compaction stopped early at %s due to a parity "
                "mismatch (%d day(s) compacted this run before stopping).",
                result["mismatch_date"], result["days_compacted"],
            )
        else:
            status = "success"
            logger.info(
                "Archive compaction completed successfully. Days compacted: "
                "%d, files compacted: %d",
                result["days_compacted"], result["files_compacted"],
            )

    except Exception as exc:
        logger.error("Archive compaction failed: %s", exc, exc_info=True)
        status = "failure"

    finally:
        try:
            publish_completion_stats(cfg, result, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
