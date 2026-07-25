#!/usr/bin/env python3
"""
SkyFollower Archive Compaction

Daily one-shot job that consolidates a single day's small per-flight
Parquet index files (index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet,
written one per flight by archive-processor) into one file per partition,
so Athena/Glue partition projection isn't scanning thousands of tiny files
per day indefinitely.

Targets "the day before yesterday" (UTC) to absorb flight_ttl_seconds
archival delay and any lag from the offline s3.db fallback draining late.
"""

from __future__ import annotations

import io
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import paho.mqtt.client as mqtt
import pyarrow as pa
import pyarrow.parquet as pq

# Add /app to sys.path so shared/ is importable whether running from
# /app/archive-compaction or /app.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shared.logging_setup import configure_logging
from shared.mqtt import build_mqtt_client

logger = logging.getLogger("archive-compaction")

MQTT_ROOT = "SkyFollower/runner/archive-compaction"

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


# ---------------------------------------------------------------------------
# Partition targeting
# ---------------------------------------------------------------------------

def target_partition_prefix(now: datetime | None = None) -> str:
    """
    Return the S3 prefix for the partition this run should compact: "the
    day before yesterday" in UTC, not yesterday -- this absorbs
    flight_ttl_seconds archival delay and any lag from the local s3.db
    offline-fallback queue draining late, so a flight that logically
    belongs to that day but was archived a bit late is still present
    before compaction runs.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    target = now - timedelta(days=2)
    return (
        f"index/year={target.strftime('%Y')}/"
        f"month={target.strftime('%m')}/"
        f"day={target.strftime('%d')}/"
    )


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


# ---------------------------------------------------------------------------
# MQTT
# ---------------------------------------------------------------------------

def publish_completion_stats(
    cfg: dict,
    files_compacted: int,
    files_delete_failed: int,
    status: str,
) -> None:
    """Publish completion statistics to MQTT, one retained topic per stat."""
    mc = cfg.get("mqtt")
    if not mc:
        logger.info("No MQTT config; skipping stats publish.")
        return

    run_at = datetime.now(timezone.utc).isoformat()

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
    files_compacted = 0
    files_delete_failed = 0

    try:
        s3_cfg = cfg["s3"]
        bucket = s3_cfg["bucket"]
        s3_client = connect_s3(s3_cfg)

        prefix = target_partition_prefix()
        logger.info("Compacting partition %s", prefix)
        result = compact_partition(s3_client, bucket, prefix)
        files_compacted = result["files_compacted"]
        files_delete_failed = result["files_delete_failed"]

        status = "success"
        logger.info(
            "Archive compaction completed successfully. Files compacted: %d",
            files_compacted,
        )

    except Exception as exc:
        logger.error("Archive compaction failed: %s", exc, exc_info=True)
        status = "failure"

    finally:
        try:
            publish_completion_stats(cfg, files_compacted, files_delete_failed, status)
        except Exception as exc:
            logger.warning("Failed to publish MQTT stats: %s", exc)

    if status != "success":
        sys.exit(1)


if __name__ == "__main__":
    main()
