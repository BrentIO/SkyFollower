"""
S3 key/Parquet index construction for completed flights.

Shared between archive-processor (one row per flight, written at archive
time) and tools/legacy-migration (many rows compacted into one Parquet file
per day, written once per calendar day of backfilled history) so there is
exactly one implementation of the destination key format and the index
column layout, never a second one drifting from
specs/data-dictionary.yaml's archive_parquet_index record.
"""

from __future__ import annotations

import io
from datetime import timezone

import pyarrow as pa
import pyarrow.parquet as pq

from shared.models import CompletedFlight

PARQUET_INDEX_SCHEMA = pa.schema([
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


def build_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight.
    Format: flights/{YYYY}/{MM}/{DD}/{uuid}.json.gz

    Dated by first_message, not last_message: this key is computed once
    and frozen (a stitch overwrites the object in place, never
    recomputing the key), and first_message -- unlike last_message -- is
    invariant across stitching, so the frozen date stays correct even if
    a stitch straddles a UTC day boundary.
    """
    dt = flight.first_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")

    uuid = flight.id  # alias for _id field

    return f"flights/{yyyy}/{mm}/{dd}/{uuid}.json.gz"


def build_index_s3_key(flight: CompletedFlight) -> str:
    """
    Build the S3 object key for a completed flight's single-row Parquet
    index file. Format: index/year={YYYY}/month={MM}/day={DD}/{uuid}.parquet

    Hive-style partition segments so Athena partition projection needs no
    explicit storage.location.template. Dated by first_message: unlike
    build_s3_key()'s frozen key, this index row is rebuilt on every
    stitch, so it must derive its date from a field stitching never
    changes -- using last_message would orphan the row under the wrong
    day's partition whenever a stitch straddles a UTC day boundary.
    """
    dt = flight.first_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    return f"index/year={yyyy}/month={mm}/day={dd}/{flight.id}.parquet"


def flight_index_row(flight: CompletedFlight, s3_key: str) -> dict:
    """
    Build one Parquet index row (as a plain dict matching
    PARQUET_INDEX_SCHEMA's column set/order) for a completed flight.
    s3_key is the flight object's own key (from build_s3_key), copied in
    so a search hit can be resolved to its full flight record.
    """
    return {
        "icao_hex": flight.aircraft.get("icao_hex", "") or "",
        "registration": flight.aircraft.get("registration", "") or "",
        "type_designator": flight.aircraft.get("type_designator", "") or "",
        # military is present-and-true or absent, never explicit False;
        # normalize absent to False for a clean non-nullable column.
        "military": bool(flight.aircraft.get("military") or False),
        "operator_designator": (flight.operator or {}).get("airline_designator", "") or "",
        "ident": flight.ident or "",
        "first_message": flight.first_message,
        "last_message": flight.last_message,
        "s3_key": s3_key,
    }


def build_parquet_index_row(flight: CompletedFlight, s3_key: str) -> bytes:
    """
    Build the single-row Parquet file (in-memory bytes) for one completed
    flight's index entry. tools/legacy-migration instead accumulates
    flight_index_row() dicts across a day and writes one compacted table,
    calling flight_index_row() directly rather than this function.
    """
    table = pa.Table.from_pylist([flight_index_row(flight, s3_key)], schema=PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()
