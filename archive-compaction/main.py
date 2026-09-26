#!/usr/bin/env python3
"""
SkyFollower Archive Compaction

Daily job that consolidates each day's small per-flight Parquet index files
(index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet, written one per
flight by archive-processor) into one file per partition, so Athena/Glue
isn't scanning thousands of tiny files per day indefinitely.

Tracks a `_compaction_state/watermark.json` "last compacted date" in S3 and
walks forward one date at a time from watermark+1 up to today-2 (UTC),
absorbing archival delay so a single run can clear a multi-day backlog
once whatever stalled it is fixed.

Before compacting each date, verifies every flight object has a matching
Parquet index row; a mismatch stops the loop there, leaves the watermark
unchanged, tracks consecutive blocked runs (published over MQTT), and
exits with a distinct status code so it's distinguishable from a genuine
failure.

Each per-flight row is read from the shared local index cache archive-
processor populates (see read_parquet_table()), falling back to S3 only
when the local copy is missing.
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
from botocore.exceptions import ClientError

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-compaction or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.config import ConfigError, load_config
from shared.ha_discovery import build_ha_device
from shared.index_cache import INDEX_CACHE_DIR, delete_local_index, local_index_path
from shared.logging_setup import configure_logging
from shared.mqtt import build_mqtt_client
from shared.mqtt_register import publish_register

logger = logging.getLogger("archive-compaction")

MQTT_ROOT = "SkyFollower/archive-compaction"

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

# Per-flight files are named "{uuid}.parquet" with no other prefix.
# Consolidated output always uses this prefix instead, so a later run never
# mistakes it for a per-flight file and re-reads already-compacted rows.
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
    """Latest date this job will ever compact: today - 2, UTC. Not
    yesterday -- absorbs flight_ttl_seconds archival delay and fallback-
    drain lag, so a late-archived flight is still present before
    compaction runs."""
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
    """The index/ prefix for the cutoff date (today - 2, UTC) -- the
    latest date this job would ever compact right now."""
    return index_prefix_for_date(_cutoff_date(now))


def is_per_flight_file(key: str) -> bool:
    """True for a small per-flight index file (bare-UUID basename), False
    for a previous run's compacted output (compacted-* basename)."""
    basename = key.rsplit("/", 1)[-1]
    return not basename.startswith(_COMPACTED_PREFIX)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

def connect_s3():
    """No credential arguments: boto3 reads AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION from its own default
    credential chain, which an instance role can also satisfy."""
    return boto3.Session().client("s3")


def list_partition_objects(s3_client, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def read_parquet_table(
    s3_client, bucket: str, key: str, local_dir: str = INDEX_CACHE_DIR
) -> pa.Table:
    """Read a per-flight Parquet index row, preferring the local copy
    archive-processor already wrote to the shared index cache, to avoid
    a GetObject call for bytes already on local disk. Falls back to S3
    only when the local copy is missing."""
    local_path = local_index_path(key, local_dir)
    try:
        with open(local_path, "rb") as f:
            body = f.read()
    except FileNotFoundError:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response["Body"].read()
    return pq.read_table(io.BytesIO(body))


def build_compacted_key(prefix: str) -> str:
    return f"{prefix}{_COMPACTED_PREFIX}{uuid.uuid4()}.parquet"


def delete_keys(s3_client, bucket: str, keys: list[str]) -> tuple[int, list[str]]:
    """Batch-delete `keys` (up to 1000 per API call, the S3 limit).
    Returns the failure count and the list of keys actually confirmed
    deleted, so the caller knows which local index-cache copies are safe
    to remove too."""
    failed = 0
    deleted: list[str] = []
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
        failed_keys = {err.get("Key") for err in errors}
        deleted.extend(k for k in chunk if k not in failed_keys)
    return failed, deleted


# ---------------------------------------------------------------------------
# Flight / index parity
# ---------------------------------------------------------------------------

def _uuid_from_flight_key(key: str) -> str | None:
    """Extract the flight UUID from a flights/ object key
    (flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz), or None if it doesn't
    match this shape."""
    basename = key.rsplit("/", 1)[-1]
    if not basename.endswith(".json.gz"):
        return None
    uuid = basename[: -len(".json.gz")]
    if not uuid or "_" in uuid:
        return None
    return uuid


def _uuid_from_index_key(key: str) -> str | None:
    """Extract the flight UUID from a per-flight index/ object key
    (index/year=/month=/day=/{uuid}.parquet), or None for an
    already-compacted file or otherwise non-matching key."""
    if not is_per_flight_file(key):
        return None
    basename = key.rsplit("/", 1)[-1]
    if not basename.endswith(".parquet"):
        return None
    return basename[: -len(".parquet")]


def check_date_parity(s3_client, bucket: str, d: date) -> set[str]:
    """Return the set of flight UUIDs present under `d`'s flights/ prefix
    with no matching Parquet index row -- flights that would be missing
    from the index forever if this date were compacted as-is. An empty
    set means safe to compact. Not checked the other direction: an index
    row with no matching flight object loses no data when compacted."""
    flight_keys = list_partition_objects(s3_client, bucket, flights_prefix_for_date(d))
    index_keys = list_partition_objects(s3_client, bucket, index_prefix_for_date(d))

    flight_uuids = {u for k in flight_keys if (u := _uuid_from_flight_key(k))}
    index_uuids = {u for k in index_keys if (u := _uuid_from_index_key(k))}

    return flight_uuids - index_uuids


# ---------------------------------------------------------------------------
# Watermark
# ---------------------------------------------------------------------------

def _parse_date_field(data: dict, key: str) -> date | None:
    value = data.get(key)
    return datetime.strptime(value, "%Y-%m-%d").date() if value else None


def read_watermark(s3_client, bucket: str) -> dict | None:
    """Read compaction state (watermark plus mismatch tracking) from
    _compaction_state/watermark.json.

    Returns None only when the object genuinely doesn't exist yet (a real
    first run, via S3's NoSuchKey/404). Any other failure re-raises rather
    than being treated as "absent" -- silently skipping every date before
    the cutoff would be worse than failing loudly.

    On success, returns a dict with `last_compacted_date`, `mismatch_date`,
    and `mismatch_runs`. Missing keys (a pre-mismatch-tracking watermark
    object) default to None / 0.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=_WATERMARK_KEY)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            return None
        raise

    data = json.loads(response["Body"].read())
    return {
        "last_compacted_date": _parse_date_field(data, "last_compacted_date"),
        "mismatch_date": _parse_date_field(data, "mismatch_date"),
        "mismatch_runs": data.get("mismatch_runs", 0),
    }


def write_watermark(
    s3_client,
    bucket: str,
    last_compacted_date: date | None,
    mismatch_date: date | None = None,
    mismatch_runs: int = 0,
) -> None:
    body = json.dumps({
        "last_compacted_date": (
            last_compacted_date.strftime("%Y-%m-%d") if last_compacted_date else None
        ),
        "mismatch_date": mismatch_date.strftime("%Y-%m-%d") if mismatch_date else None,
        "mismatch_runs": mismatch_runs,
    }).encode("utf-8")
    s3_client.put_object(
        Bucket=bucket,
        Key=_WATERMARK_KEY,
        Body=body,
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------

def compact_partition(
    s3_client, bucket: str, prefix: str, local_dir: str = INDEX_CACHE_DIR
) -> dict:
    """Compact one day's partition: read every per-flight Parquet file
    under `prefix`, write one consolidated file, then delete only the
    source files that were actually read into it.

    Write-then-delete, and only delete a key successfully read into the
    output -- an unreadable file is left in place (deleting it would lose
    data) and a late straggler that lands after the initial listing is
    simply never seen by this run. Both are the same self-healing shape:
    an extra small file left in the partition, queryable on its own.

    Each read prefers the local index cache over S3 (see
    read_parquet_table); once a source key's S3 object is confirmed
    deleted, its local cache copy is removed too.
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
            tables.append(read_parquet_table(s3_client, bucket, key, local_dir))
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

    files_delete_failed, deleted_keys = delete_keys(s3_client, bucket, included_keys)
    for key in deleted_keys:
        delete_local_index(key, local_dir)

    return {
        "files_compacted": len(included_keys),
        "files_delete_failed": files_delete_failed,
    }


def run_compaction(
    s3_client, bucket: str, now: datetime | None = None, local_dir: str = INDEX_CACHE_DIR
) -> dict:
    """Catch-up loop: starting the day after the watermark (or
    cutoff - 1 day on a first run), compact one date at a time up to the
    cutoff (today - 2, UTC). Each date is gated by check_date_parity
    first -- a mismatch stops the loop immediately, leaving that date and
    every later one uncompacted and the watermark unchanged, so a later
    run resumes at the same stuck date once the mismatch resolves.

    A mismatch on the same date across consecutive runs increments
    `mismatch_runs` (persisted alongside the watermark), resetting to 1
    when the blocked date changes and to 0 once a run gets past it.

    `local_dir` is the shared index-cache root each date's reads and
    post-delete cleanup use.
    """
    cutoff = _cutoff_date(now)
    state = read_watermark(s3_client, bucket)
    if state is None:
        watermark = cutoff - timedelta(days=1)
        prior_mismatch_date: date | None = None
        prior_mismatch_runs = 0
    else:
        watermark = state["last_compacted_date"] or (cutoff - timedelta(days=1))
        prior_mismatch_date = state["mismatch_date"]
        prior_mismatch_runs = state["mismatch_runs"]

    files_compacted = 0
    files_delete_failed = 0
    days_compacted = 0
    mismatch_date: date | None = None
    mismatch_uuids: set[str] = set()
    mismatch_runs = 0

    target = watermark + timedelta(days=1)
    while target <= cutoff:
        missing = check_date_parity(s3_client, bucket, target)
        if missing:
            mismatch_date = target
            mismatch_uuids = missing
            mismatch_runs = (
                prior_mismatch_runs + 1 if prior_mismatch_date == target else 1
            )
            logger.error(
                "Parity mismatch for %s: %d flight(s) missing their index row; "
                "stopping catch-up here (blocked for %d consecutive run(s)). "
                "UUIDs: %s",
                target.isoformat(), len(missing), mismatch_runs, ", ".join(sorted(missing)),
            )
            write_watermark(
                s3_client, bucket, watermark,
                mismatch_date=mismatch_date, mismatch_runs=mismatch_runs,
            )
            break

        prefix = index_prefix_for_date(target)
        logger.info("Compacting partition %s", prefix)
        result = compact_partition(s3_client, bucket, prefix, local_dir)
        files_compacted += result["files_compacted"]
        files_delete_failed += result["files_delete_failed"]
        days_compacted += 1

        watermark = target
        write_watermark(s3_client, bucket, watermark, mismatch_date=None, mismatch_runs=0)
        target += timedelta(days=1)

    return {
        "files_compacted": files_compacted,
        "files_delete_failed": files_delete_failed,
        "days_compacted": days_compacted,
        "last_compacted_date": watermark,
        "mismatch_date": mismatch_date,
        "mismatch_uuids": mismatch_uuids,
        "mismatch_runs": mismatch_runs,
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
    mismatch_runs = result.get("mismatch_runs", 0)

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
            last_compacted_date.strftime("%Y-%m-%d") if last_compacted_date else "None",
            retain=True,
        )
        client.publish(
            f"{base}/mismatch_date",
            mismatch_date.strftime("%Y-%m-%d") if mismatch_date else "None",
            retain=True,
        )
        client.publish(
            f"{base}/mismatch_uuids",
            json.dumps({"uuids": sorted(mismatch_uuids)}),
            retain=True,
        )
        client.publish(f"{base}/mismatch_uuid_count", str(len(mismatch_uuids)), retain=True)
        client.publish(f"{base}/mismatch_runs", str(mismatch_runs), retain=True)
        client.publish(f"{base}/last_run_at", run_at, retain=True)
        client.publish(f"{base}/last_run_status", status.capitalize(), retain=True)

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
    device = build_ha_device(
        identifier="SkyFollower_archive_compaction",
        name="SkyFollower Archive Compaction",
        model="Archive Compaction",
    )
    publish_register(client, device)
    stats = [
        ("files_compacted", "Files Compacted", "mdi:file-multiple", "total_increasing", None, None),
        ("files_delete_failed", "Delete Failures", "mdi:alert", "total_increasing", None, None),
        ("days_compacted", "Days Compacted", "mdi:calendar-check", "measurement", None, None),
        ("last_compacted_date", "Last Compacted Date", "mdi:calendar", None, None, None),
        ("mismatch_date", "Mismatch Date", "mdi:calendar-alert", None, None, None),
        ("mismatch_uuid_count", "Mismatch Flight Count", "mdi:alert-circle", "measurement", None,
         f"{MQTT_ROOT}/statistic/mismatch_uuids"),
        ("mismatch_runs", "Mismatch Consecutive Runs", "mdi:counter", "measurement", None, None),
        ("last_run_at", "Last Run At", "mdi:clock", None, None, None),
        ("last_run_status", "Last Run Status", "mdi:check-circle", None, None, None),
    ]
    for name, friendly_name, icon, state_class, unit, json_attributes_topic in stats:
        payload: dict = {
            "state_topic": f"{MQTT_ROOT}/statistic/{name}",
            "name": friendly_name,
            "has_entity_name": True,
            "unique_id": f"SkyFollower_archive_compaction_{name}",
            "object_id": f"SkyFollower_archive_compaction_{name}",
            "device": device,
            "icon": icon,
        }
        if state_class:
            payload["state_class"] = state_class
        if unit:
            payload["unit_of_measurement"] = unit
        if json_attributes_topic:
            payload["json_attributes_topic"] = json_attributes_topic
        if name == "last_run_at":
            payload["device_class"] = "timestamp"
        client.publish(
            f"homeassistant/sensor/SkyFollower_archive_compaction_{name}/config",
            json.dumps(payload),
            retain=True,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    try:
        cfg = load_config("mqtt", "s3")
    except ConfigError as exc:
        configure_logging()
        logger.critical("%s", exc)
        sys.exit(1)

    configure_logging(cfg.get("log_level"))

    version = os.environ.get("VERSION", "dev")
    logger.info("Starting SkyFollower Archive Compaction %s", version)

    status = "failure"
    result: dict = {
        "files_compacted": 0,
        "files_delete_failed": 0,
        "days_compacted": 0,
        "last_compacted_date": None,
        "mismatch_date": None,
        "mismatch_uuids": set(),
        "mismatch_runs": 0,
    }

    try:
        bucket = cfg["s3"]["bucket"]

        s3_client = connect_s3()

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

    if status == "mismatch":
        # Distinct non-zero code so exit-status monitoring can tell a
        # parity mismatch apart from a genuine failure.
        sys.exit(2)
    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
