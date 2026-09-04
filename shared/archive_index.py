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

    Dated by first_message, not last_message: split-flight stitching
    (_merge_segments) always preserves the *original* segment's
    first_message across every stitch, while last_message keeps advancing
    to whichever segment most recently continued the flight. This key is
    only ever computed once per flight (a stitch overwrites the object in
    place under its original key, never recomputing it) — first_message
    is what keeps that frozen key's date consistent with the value the
    Parquet index (build_index_s3_key, same rationale) recomputes on every
    stitch, even when a stitch happens to straddle a UTC day boundary.

    Also the key format tools/legacy-migration copies legacy flights under
    -- first_message there comes from the legacy Mongo stub rather than a
    live message processor, but the field means the same thing.
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

    Hive-style partition segments (year=/month=/day=) so Athena partition
    projection can use its default location-template behavior with no
    explicit storage.location.template table property required. Dated by
    first_message, matching build_s3_key() — unlike the flight object's
    key (computed once, then frozen across any later stitch), this index
    row IS rebuilt on every stitch, so it must derive its date from
    something stitching never changes. last_message advances with every
    stitched segment; first_message is always the original segment's,
    invariant across the whole chain (see archive-processor's
    _merge_segments). Using last_message here would silently orphan a
    stale index row under the original day's partition — and create a
    second, live one elsewhere — the moment a stitch happened to straddle
    a UTC day boundary.

    Not used by tools/legacy-migration, which writes one file per *day*
    (many flights already compacted) rather than one file per flight.
    """
    dt = flight.first_message.astimezone(timezone.utc)
    yyyy = dt.strftime("%Y")
    mm = dt.strftime("%m")
    dd = dt.strftime("%d")
    return f"index/year={yyyy}/month={mm}/day={dd}/{flight.id}.parquet"


def flight_index_row(flight: CompletedFlight, s3_key: str) -> dict:
    """
    Build one Parquet index row (as a plain dict, matching
    PARQUET_INDEX_SCHEMA's column set/order) for a completed flight.
    s3_key is the flight object's own key (from build_s3_key), copied into
    the row so a search hit can be resolved to its full flight record.
    Column set/order matches specs/data-dictionary.yaml's
    archive_parquet_index record exactly.
    """
    return {
        "icao_hex": flight.aircraft.get("icao_hex", "") or "",
        "registration": flight.aircraft.get("registration", "") or "",
        "type_designator": flight.aircraft.get("type_designator", "") or "",
        # The merged aircraft record only ever has military present-and-true
        # or absent (to_completed_flight() strips an explicit False for
        # legacy compatibility) — normalize absent to False here so the
        # column is a clean non-nullable boolean rather than tri-state.
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
    flight's index entry. Used by archive-processor, which writes one
    index file per flight; tools/legacy-migration instead accumulates
    flight_index_row() dicts across a whole day and writes one compacted
    Parquet table, so it calls flight_index_row() directly rather than
    this function.
    """
    table = pa.Table.from_pylist([flight_index_row(flight, s3_key)], schema=PARQUET_INDEX_SCHEMA)
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()
