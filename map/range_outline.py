"""
Daily reception range outline for the map service.

"How far can this system hear, per compass bearing, per altitude band" --
built from the same `position` UDP stream the live map runs on. Buckets at
720 half-degree bearing resolution -- finer than readsb's own
actual-range-outline (360 one-degree bearing buckets, per-bucket farthest
received position), deliberately, since this runs centrally and aggregates
*every* receiver / external feed rather than one antenna.

Lifecycle:

- The current UTC day's outline lives in one Redis hash
  (``map:range:outline``): field ``"{bearing_index}:{band}"``, value JSON
  ``{nm, lat, lon, alt, ts}`` = the single farthest position received in
  that bucket today. ``bearing_index`` is an integer half-degree index
  (``0``-``719``; real bearing = ``bearing_index / 2``), kept as a plain
  integer string rather than a float so downstream `int()`/`.isdigit()`
  parsing of the field name is unaffected. It accumulates from empty at
  00:00 UTC.
- Snapshotted to ``{snapshot_dir}/{YYYY-MM-DD}.json`` -- raw buckets, not
  GeoJSON -- once every MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS when
  it has changed, and once at the UTC-date rollover (which also clears the
  Redis hash so the new day starts empty).
- On boot the in-progress day is reloaded from ``{today}.json`` if present
  (mid-day crash recovery). A reboot spanning midnight finds no
  ``{today}.json`` and starts the new day empty, leaving ``{yesterday}.json``
  as its record.
- Snapshot files older than MAP_RANGE_OUTLINE_RETENTION_DAYS are deleted on
  each snapshot write.

The Redis hash carries a short safety TTL so a dead process can't leave
stale data in this no-persistence Redis forever; the disk files are the
real 30-day store, and a live process refreshes the TTL on every write.

Disabled entirely when no "center" reference point is configured
(MAP_CENTER_LATITUDE/LONGITUDE) -- there is no origin to measure bearing and
distance from, same as the frontend's range rings.
"""

from __future__ import annotations

import json
import logging
import os
import pathlib
import tempfile
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from map.geo import great_circle_nm, initial_bearing

logger = logging.getLogger("map.range_outline")

OUTLINE_KEY = "map:range:outline"

# A bucket whose max isn't re-confirmed within a day naturally drops when
# that day's hash is cleared at rollover. This TTL is only a safety net for
# "the process died and never rolled over" -- two days of slack past the
# longest a single day's hash should ever live.
_SAFETY_TTL_SECONDS = 2 * 86400

# readsb rejects positions beyond --max-range (default 300 nm) for decoding;
# anything past this from center is a bad decode or a relayed position from a
# far-away feed that doesn't belong in *this* system's range outline.
MAX_RANGE_NM = 325.0

# Once a bearing bucket has a maximum, a new maximum more than this far
# beyond it is only accepted if a bearing bucket within _OUTLIER_NEIGHBOUR_DEG
# already reaches close to the same distance -- otherwise it's treated as a
# one-off bad decode rather than genuine propagation. (The map UDP feed
# carries no CPR reliability flags, so this stands in for readsb's
# odd/even-count check.) The first-ever detection in a direction, into an
# empty bucket, is always accepted up to MAX_RANGE_NM.
#
# This constant is in real degrees. Bearing buckets are keyed by half-degree
# index (see module docstring), so anywhere this drives a bucket-index
# offset it must be doubled (_OUTLIER_NEIGHBOUR_DEG * 2) to keep the real
# angular tolerance at +/-_OUTLIER_NEIGHBOUR_DEG -- doubling the bucket
# count without doubling the index-space window would silently halve it.
_OUTLIER_JUMP_NM = 50.0
_OUTLIER_NEIGHBOUR_DEG = 3

# Lower edge (feet) of each altitude band; the last band has no upper edge.
# A position with unknown altitude goes in the lowest band.
_ALTITUDE_BAND_EDGES = (0, 2000, 5000, 10000, 20000, 30000, 40000)


def _band_label(lower: int, upper: Optional[int]) -> str:
    return f"{lower}+" if upper is None else f"{lower}-{upper}"


ALTITUDE_BANDS: tuple[tuple[int, Optional[int], str], ...] = tuple(
    (
        _ALTITUDE_BAND_EDGES[i],
        (_ALTITUDE_BAND_EDGES[i + 1] if i + 1 < len(_ALTITUDE_BAND_EDGES) else None),
        _band_label(
            _ALTITUDE_BAND_EDGES[i],
            (_ALTITUDE_BAND_EDGES[i + 1] if i + 1 < len(_ALTITUDE_BAND_EDGES) else None),
        ),
    )
    for i in range(len(_ALTITUDE_BAND_EDGES))
)

_BAND_LABELS = tuple(label for _lo, _hi, label in ALTITUDE_BANDS)

_DATE_FMT = "%Y-%m-%d"


def _utc_today() -> str:
    return datetime.now(timezone.utc).strftime(_DATE_FMT)


def altitude_band(alt: Optional[float]) -> str:
    """The band label for an altitude in feet. Unknown/None -> lowest band."""
    if alt is None:
        return _BAND_LABELS[0]
    for lower, upper, label in ALTITUDE_BANDS:
        if alt < lower:
            # Below the lowest edge (a negative/near-ground altitude).
            return _BAND_LABELS[0]
        if upper is None or alt < upper:
            return label
    return _BAND_LABELS[-1]


class RangeOutlineStore:
    """Owns the Redis hash and the on-disk daily snapshots. All public
    methods are safe to call from the UDP listener thread, the snapshot
    thread and FastAPI's request threadpool concurrently -- a lock guards
    the date-rollover and snapshot critical sections; individual Redis
    commands are atomic."""

    def __init__(
        self,
        redis_client,
        center_latitude: Optional[float],
        center_longitude: Optional[float],
        snapshot_dir,
        retention_seconds: int,
    ) -> None:
        self._redis = redis_client
        self._center = (
            (center_latitude, center_longitude)
            if center_latitude is not None and center_longitude is not None
            else None
        )
        self._dir = pathlib.Path(snapshot_dir)
        self._retention_days = max(1, round(retention_seconds / 86400))
        self._date = _utc_today()
        self._dirty = False
        self._lock = threading.Lock()

    @property
    def enabled(self) -> bool:
        return self._center is not None

    # -- boot ------------------------------------------------------------

    def load_from_disk(self) -> None:
        """Called once at startup. Restores the in-progress day from
        ``{today}.json`` if it exists (a mid-day crash), then prunes files
        past the retention window. A reboot that spanned midnight finds no
        ``{today}.json`` and simply starts the new day empty."""
        if not self.enabled:
            return
        with self._lock:
            self._date = _utc_today()
            path = self._dir / f"{self._date}.json"
            buckets = self._read_snapshot_file(path)
            if buckets:
                self._write_buckets_to_redis(buckets)
                logger.info(
                    "Range outline: restored %d buckets for %s from disk.",
                    len(buckets), self._date,
                )
            self._dirty = False
        self._cleanup_old_snapshots()

    # -- hot path ------------------------------------------------------------

    def record_position(self, lat: Optional[float], lon: Optional[float], alt: Optional[float]) -> None:
        """Fold one received position into the outline. No-op when the
        outline is disabled (no center) or the position has no lat/lon."""
        if not self.enabled or lat is None or lon is None:
            return

        today = _utc_today()
        if today != self._date:
            self._rollover(today)

        center_lat, center_lon = self._center
        nm = great_circle_nm(center_lat, center_lon, lat, lon)
        if nm > MAX_RANGE_NM:
            return

        # Half-degree index, 0-719 (real bearing = bearing_index / 2).
        bearing_index = round(initial_bearing(center_lat, center_lon, lat, lon) * 2) % 720
        band = altitude_band(alt)
        field = f"{bearing_index}:{band}"

        # +/-_OUTLIER_NEIGHBOUR_DEG real degrees -> +/-(_OUTLIER_NEIGHBOUR_DEG * 2)
        # in index units, since each index step is 0.5 real degrees.
        neighbour_window = _OUTLIER_NEIGHBOUR_DEG * 2
        neighbour_fields = [
            f"{(bearing_index + d) % 720}:{band}"
            for d in range(-neighbour_window, neighbour_window + 1)
            if d != 0
        ]
        current_raw, *neighbours_raw = self._redis.hmget(OUTLINE_KEY, field, *neighbour_fields)

        current_nm = self._point_nm(current_raw)
        if nm <= current_nm:
            return

        if nm > current_nm + _OUTLIER_JUMP_NM and current_nm > 0:
            neighbour_max = max((self._point_nm(r) for r in neighbours_raw), default=0.0)
            if neighbour_max < nm - _OUTLIER_JUMP_NM:
                logger.debug(
                    "Range outline: rejecting outlier %.0f nm at bearing %.1f (current %.0f, neighbours %.0f)",
                    nm, bearing_index / 2, current_nm, neighbour_max,
                )
                return

        point = json.dumps(
            {"nm": round(nm, 2), "lat": round(lat, 5), "lon": round(lon, 5),
             "alt": (int(alt) if alt is not None else None), "ts": int(time.time())}
        )
        pipe = self._redis.pipeline()
        pipe.hset(OUTLINE_KEY, field, point)
        pipe.expire(OUTLINE_KEY, _SAFETY_TTL_SECONDS)
        pipe.execute()
        self._dirty = True

    # -- snapshot loop -----------------------------------------------------

    def tick(self) -> None:
        """Called every MAP_RANGE_OUTLINE_SNAPSHOT_INTERVAL_SECONDS from
        the snapshot thread: rolls the date over if midnight has passed
        with no traffic, recovers from a map-redis restart, and writes the
        current day's snapshot if it has changed."""
        if not self.enabled:
            return
        today = _utc_today()
        if today != self._date:
            self._rollover(today)
        self._reload_if_redis_emptied()
        self._snapshot_if_dirty()
        self._cleanup_old_snapshots()

    def snapshot_now(self) -> None:
        """Best-effort final write on shutdown."""
        self._snapshot_if_dirty()

    def _snapshot_if_dirty(self) -> None:
        if not self.enabled:
            return
        with self._lock:
            if not self._dirty:
                return
            # Cleared *before* the write, not after: a record_position()
            # that lands between the HGETALL below and this method returning
            # re-sets the flag, so its point is picked up by the next tick
            # rather than silently dropped.
            self._dirty = False
            date = self._date
        self._write_snapshot(date)

    # -- API ------------------------------------------------------------

    def get_outline(self, date: Optional[str] = None, band: Optional[str] = None) -> dict:
        """A GeoJSON FeatureCollection: one Polygon per altitude band that
        has at least three bearing points, plus an ``envelope`` Polygon
        (the farthest point per bearing across all bands). `date` None ->
        today's live outline from Redis; otherwise that day's finalised
        snapshot from disk (raises FileNotFoundError if it's gone)."""
        if not self.enabled:
            return self._feature_collection([], date or self._date, center=None)

        if date is None:
            date = self._date
            buckets = self._read_buckets_from_redis()
        else:
            _validate_date(date)
            buckets = self._read_snapshot_file(self._dir / f"{date}.json")
            if buckets is None:
                raise FileNotFoundError(date)

        return self._build_geojson(buckets, date, band)

    def available_dates(self) -> list[str]:
        """Snapshot dates present on disk (newest first), plus ``"today"``
        for the live outline."""
        dates = sorted(
            (p.stem for p in self._dir.glob("*.json") if _is_date(p.stem)),
            reverse=True,
        ) if self._dir.is_dir() else []
        return ["today", *dates]

    def clear_today(self) -> None:
        """Reset the in-progress day -- drops the Redis hash and today's
        snapshot file. Finalised past days are untouched."""
        with self._lock:
            self._redis.delete(OUTLINE_KEY)
            try:
                (self._dir / f"{self._date}.json").unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("Range outline: could not delete today's snapshot: %r", exc)
            self._dirty = False

    # -- internals ------------------------------------------------------------

    def _rollover(self, new_date: str) -> None:
        with self._lock:
            if new_date == self._date:
                return  # Another thread beat us to it.
            finished = self._date
            if self._redis.hlen(OUTLINE_KEY):
                self._write_snapshot(finished)
            self._redis.delete(OUTLINE_KEY)
            self._date = new_date
            self._dirty = False
        logger.info("Range outline: rolled over %s -> %s.", finished, new_date)

    def _reload_if_redis_emptied(self) -> None:
        """Recover the current day's outline if map-redis restarted (it has
        no persistence) while this process stayed up."""
        try:
            if self._redis.exists(OUTLINE_KEY):
                return
        except Exception:
            return
        buckets = self._read_snapshot_file(self._dir / f"{self._date}.json")
        if buckets:
            with self._lock:
                if not self._redis.exists(OUTLINE_KEY):
                    self._write_buckets_to_redis(buckets)
                    logger.info(
                        "Range outline: map-redis was emptied; restored %d buckets for %s.",
                        len(buckets), self._date,
                    )

    def _write_buckets_to_redis(self, buckets: dict) -> None:
        pipe = self._redis.pipeline()
        pipe.delete(OUTLINE_KEY)
        pipe.hset(OUTLINE_KEY, mapping={k: json.dumps(v) for k, v in buckets.items()})
        pipe.expire(OUTLINE_KEY, _SAFETY_TTL_SECONDS)
        pipe.execute()

    def _read_buckets_from_redis(self) -> dict:
        raw = self._redis.hgetall(OUTLINE_KEY)
        out = {}
        for field, value in raw.items():
            try:
                out[field] = json.loads(value)
            except (TypeError, ValueError):
                continue
        return out

    @staticmethod
    def _point_nm(raw) -> float:
        if not raw:
            return 0.0
        try:
            return float(json.loads(raw).get("nm", 0.0))
        except (TypeError, ValueError):
            return 0.0

    def _write_snapshot(self, date: str) -> None:
        buckets = self._read_buckets_from_redis()
        payload = {
            "date": date,
            "center": {"lat": self._center[0], "lon": self._center[1]} if self._center else None,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "buckets": buckets,
        }
        self._dir.mkdir(parents=True, exist_ok=True)
        target = self._dir / f"{date}.json"
        try:
            fd, tmp = tempfile.mkstemp(dir=self._dir, prefix=f".{date}.", suffix=".tmp")
            with os.fdopen(fd, "w") as fh:
                json.dump(payload, fh, separators=(",", ":"))
            os.replace(tmp, target)
        except OSError as exc:
            logger.warning("Range outline: snapshot write for %s failed: %r", date, exc)

    def _read_snapshot_file(self, path: pathlib.Path) -> Optional[dict]:
        try:
            with path.open() as fh:
                data = json.load(fh)
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            logger.warning("Range outline: could not read %s: %r", path, exc)
            return None
        buckets = data.get("buckets")
        return buckets if isinstance(buckets, dict) else {}

    def _cleanup_old_snapshots(self) -> None:
        if not self._dir.is_dir():
            return
        cutoff = datetime.now(timezone.utc).date()
        for path in self._dir.glob("*.json"):
            if not _is_date(path.stem):
                continue
            file_date = datetime.strptime(path.stem, _DATE_FMT).date()
            if (cutoff - file_date).days > self._retention_days:
                try:
                    path.unlink()
                except OSError as exc:
                    logger.warning("Range outline: could not delete %s: %r", path, exc)

    # -- geojson ------------------------------------------------------------

    def _build_geojson(self, buckets: dict, date: str, band: Optional[str]) -> dict:
        # Bearing stays an index (0-719) here -- it's only used to order and
        # de-duplicate points, and each point already carries its own real
        # lat/lon. Nothing below needs the /2 conversion back to degrees.
        by_band: dict[str, dict[int, dict]] = {label: {} for label in _BAND_LABELS}
        envelope: dict[int, dict] = {}
        for field, point in buckets.items():
            try:
                bearing_str, band_label = field.split(":", 1)
                bearing_index = int(bearing_str)
            except (ValueError, AttributeError):
                continue
            if band_label not in by_band or not isinstance(point, dict):
                continue
            by_band[band_label][bearing_index] = point
            if point.get("nm", 0) > envelope.get(bearing_index, {}).get("nm", -1):
                envelope[bearing_index] = point

        wanted = [band] if band and band != "envelope" else (
            [] if band == "envelope" else list(_BAND_LABELS)
        )
        features = []
        for label in wanted:
            feature = self._band_feature(by_band[label], label)
            if feature is not None:
                features.append(feature)
        if not band or band == "envelope":
            envelope_feature = self._band_feature(envelope, "envelope")
            if envelope_feature is not None:
                features.append(envelope_feature)

        bearings_seen = {int(f.split(":", 1)[0]) for f in buckets if ":" in f and f.split(":", 1)[0].isdigit()}
        return self._feature_collection(
            features, date, bearing_count=len(bearings_seen),
            center={"lat": self._center[0], "lon": self._center[1]} if self._center else None,
        )

    @staticmethod
    def _band_feature(points_by_bearing: dict[int, dict], label: str) -> Optional[dict]:
        if len(points_by_bearing) < 3:
            return None
        ordered = [points_by_bearing[b] for b in sorted(points_by_bearing)]
        ring = [[p["lon"], p["lat"], p.get("alt")] for p in ordered]
        ring.append(ring[0])  # Close the polygon.
        return {
            "type": "Feature",
            "geometry": {"type": "Polygon", "coordinates": [ring]},
            "properties": {
                "band": label,
                "point_count": len(ordered),
                "max_range_nm": round(max(p.get("nm", 0) for p in ordered), 1),
            },
        }

    def _feature_collection(
        self, features: list, date: str, bearing_count: int = 0, center: Optional[dict] = None
    ) -> dict:
        max_range = max((f["properties"]["max_range_nm"] for f in features), default=0.0)
        return {
            "type": "FeatureCollection",
            "features": features,
            "properties": {
                "date": date,
                "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "point_count": bearing_count,
                "max_range_nm": max_range,
                "center": center,
            },
        }


def _is_date(stem: str) -> bool:
    try:
        datetime.strptime(stem, _DATE_FMT)
        return True
    except ValueError:
        return False


def _validate_date(date: str) -> None:
    if not _is_date(date):
        raise ValueError(f"date must be YYYY-MM-DD (got {date!r})")
