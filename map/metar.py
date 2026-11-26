"""
Current METAR reports for the stations near the configured "center"
reference point, from the Aviation Weather Center's cache files.

The reports file is downloaded on a fixed cadence, filtered to
METAR_RADIUS_NM and kept in the map's own Redis. A failed or unreadable
download leaves the last good data in place. Station names and display
priorities come from a second cache file and are optional.

Disabled entirely when no center is configured.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import time
import urllib.request
from datetime import datetime, timezone
from typing import Callable, Optional

from map.geo import great_circle_nm
from shared.timing import (
    MAP_METAR_FETCH_TIMEOUT_SECONDS,
    MAP_METAR_REPORT_MAX_AGE_SECONDS,
    MAP_METAR_STATION_REFRESH_INTERVAL_SECONDS,
)

logger = logging.getLogger("map.metar")

DEFAULT_BASE_URL = "https://aviationweather.gov/data/cache"
REPORTS_FILE = "metars.cache.csv.gz"
STATIONS_FILE = "stations.cache.json.gz"

METAR_KEY = "map:metar:reports"
METAR_RADIUS_NM = 250.0

_USER_AGENT = "P5Software SkyFollower"
# Far above the real sizes; guards against a runaway response.
_MAX_DOWNLOAD_BYTES = 20 * 1024 * 1024

_FLIGHT_CATEGORIES = frozenset(("VFR", "MVFR", "IFR", "LIFR"))
# Raw-report tokens meaning no cloud to list; the file leaves sky_cover blank.
_CLEAR_SKY_TOKENS = ("CAVOK", "CLR", "SKC")


def download(url: str) -> bytes:
    """GET `url` and return the decompressed body."""
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(request, timeout=MAP_METAR_FETCH_TIMEOUT_SECONDS) as response:
        body = response.read(_MAX_DOWNLOAD_BYTES + 1)
    if len(body) > _MAX_DOWNLOAD_BYTES:
        raise ValueError(f"{url} is larger than {_MAX_DOWNLOAD_BYTES} bytes")
    return gzip.decompress(body)


def _number(value: str) -> Optional[float]:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(number) if number.is_integer() else number


def _epoch(value: str) -> Optional[float]:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def parse_reports(data: bytes, latitude: float, longitude: float) -> list[dict]:
    """The reports within METAR_RADIUS_NM of the given point. Raises
    ValueError when `data` isn't a usable reports file."""
    rows = csv.reader(io.StringIO(data.decode("utf-8", errors="replace")))
    header = next(rows, [])
    # sky_cover and cloud_base_ft_agl repeat, so columns are found by position.
    columns = {name: index for index, name in reversed(list(enumerate(header)))}
    required = ("raw_text", "station_id", "observation_time", "latitude", "longitude", "flight_category")
    if any(name not in columns for name in required):
        raise ValueError("METAR file is missing expected columns")
    cloud_columns = [
        (index, index + 1)
        for index, name in enumerate(header)
        if name == "sky_cover" and header[index + 1 : index + 2] == ["cloud_base_ft_agl"]
    ]

    reports: dict[str, dict] = {}
    total = 0
    for row in rows:
        if len(row) != len(header):
            continue
        total += 1

        def field(name: str) -> str:
            return row[columns[name]].strip() if name in columns else ""

        station_id = field("station_id")
        lat, lon = _number(field("latitude")), _number(field("longitude"))
        observed = _epoch(field("observation_time"))
        if not station_id or lat is None or lon is None or observed is None:
            continue
        if great_circle_nm(latitude, longitude, lat, lon) > METAR_RADIUS_NM:
            continue
        previous = reports.get(station_id)
        if previous and previous["observed_at"] >= observed:
            continue

        raw = field("raw_text")
        tokens = raw.split(" RMK ")[0].split()
        category = field("flight_category")
        clouds = []
        for cover_index, base_index in cloud_columns:
            cover = row[cover_index].strip()
            if cover:
                clouds.append({"cover": cover, "base_ft": _number(row[base_index])})
        variable = any(t.startswith("VRB") and t.endswith(("KT", "MPS")) for t in tokens)
        report = {
            "station_id": station_id,
            "lat": lat,
            "lon": lon,
            "observed_at": observed,
            "flight_category": category if category in _FLIGHT_CATEGORIES else None,
            "raw": raw,
            "wind_variable": variable or None,
            # The file reports a variable wind as direction 0.
            "wind_dir_deg": None if variable else _number(field("wind_dir_degrees")),
            "wind_speed_kt": _number(field("wind_speed_kt")),
            "wind_gust_kt": _number(field("wind_gust_kt")),
            "visibility_sm": field("visibility_statute_mi") or None,
            "weather": field("wx_string") or None,
            "clouds": clouds or None,
            "sky_clear": next((t for t in tokens if t in _CLEAR_SKY_TOKENS), None),
            "temperature_c": _number(field("temp_c")),
            "dewpoint_c": _number(field("dewpoint_c")),
            "altimeter_inhg": _number(field("altim_in_hg")),
        }
        reports[station_id] = {k: v for k, v in report.items() if v is not None}

    if not total:
        raise ValueError("METAR file has no reports")
    return sorted(reports.values(), key=lambda r: r["station_id"])


def parse_stations(data: bytes) -> dict[str, dict]:
    """ICAO identifier -> the `name` and `station_priority` the file carries.
    A lower priority number is a more important station."""
    stations: dict[str, dict] = {}
    for station in json.loads(data):
        icao = station.get("icaoId")
        if not icao:
            continue
        details: dict = {}
        site = (station.get("site") or "").strip()
        if site:
            parts = [site, station.get("state"), station.get("country")]
            details["name"] = ", ".join(p.strip() for p in parts if p and p.strip())
        if isinstance(station.get("priority"), int):
            details["station_priority"] = station["priority"]
        if details:
            stations[icao] = details
    if not stations:
        raise ValueError("station file has no usable stations")
    return stations


class MetarStore:
    def __init__(
        self,
        redis_client,
        center_latitude: Optional[float],
        center_longitude: Optional[float],
        base_url: str = DEFAULT_BASE_URL,
        fetch: Callable[[str], bytes] = download,
    ):
        self._redis = redis_client
        self._latitude = center_latitude
        self._longitude = center_longitude
        self._base_url = base_url.rstrip("/")
        self._fetch = fetch
        self._stations: dict[str, dict] = {}
        self._stations_fetched_at: Optional[float] = None
        self.last_success_at: Optional[float] = None
        self.consecutive_failures = 0

    @property
    def enabled(self) -> bool:
        return self._latitude is not None and self._longitude is not None

    def poll(self) -> None:
        """One download cycle. Does nothing when disabled; never raises for
        a failed download."""
        if not self.enabled:
            return
        try:
            data = self._fetch(f"{self._base_url}/{REPORTS_FILE}")
            reports = parse_reports(data, self._latitude, self._longitude)
            # Every report is past its maximum age by the time the key expires.
            self._redis.set(METAR_KEY, json.dumps(reports), ex=MAP_METAR_REPORT_MAX_AGE_SECONDS)
        except Exception as exc:
            self.consecutive_failures += 1
            logger.warning(
                "METAR download failed (%d in a row): %s", self.consecutive_failures, exc
            )
        else:
            self.last_success_at = time.time()
            self.consecutive_failures = 0
            logger.info("METAR download: %d stations within %.0f nm.", len(reports), METAR_RADIUS_NM)
        self._refresh_station_details()

    def _refresh_station_details(self) -> None:
        now = time.monotonic()
        if (
            self._stations_fetched_at is not None
            and now - self._stations_fetched_at < MAP_METAR_STATION_REFRESH_INTERVAL_SECONDS
        ):
            return
        try:
            self._stations = parse_stations(self._fetch(f"{self._base_url}/{STATIONS_FILE}"))
            self._stations_fetched_at = now
        except Exception as exc:
            logger.warning("METAR station file download failed: %s", exc)

    def get(self) -> dict:
        """Reports no older than the maximum age, with station details where
        known."""
        stations: list[dict] = []
        raw = self._redis.get(METAR_KEY) if self.enabled else None
        if raw:
            oldest = time.time() - MAP_METAR_REPORT_MAX_AGE_SECONDS
            for report in json.loads(raw):
                if report["observed_at"] < oldest:
                    continue
                stations.append({**report, **self._stations.get(report["station_id"], {})})
        return {"max_age_seconds": MAP_METAR_REPORT_MAX_AGE_SECONDS, "stations": stations}
