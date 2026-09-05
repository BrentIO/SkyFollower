"""
Interpolates missing altitude across a flight's position reports and builds
the resulting 3D GeoJSON LineString.

A flight's positions[]/velocities[] are the source of truth; a flight path is
always rebuildable from them, so nothing here is persisted -- consumers
(archive processing, the management-ui view-flight endpoint, and eventually
a live moving-map) call this on demand instead of each keeping their own copy
of the interpolation logic.
"""

from __future__ import annotations

import bisect
from datetime import datetime
from typing import Optional


def _interpolate_altitudes(positions: list[dict]) -> list[Optional[int]]:
    """
    Return a list of altitudes (possibly interpolated) for the given position
    list. For each position whose altitude is None, linearly interpolate from
    the nearest preceding and following positions that do have an altitude.
    If no surrounding positions have an altitude, leave as None.
    """
    alts: list[Optional[int]] = [p.get("altitude") for p in positions]
    n = len(alts)

    for i in range(n):
        if alts[i] is not None:
            continue
        # Find the previous known altitude
        prev_idx = None
        for j in range(i - 1, -1, -1):
            if alts[j] is not None:
                prev_idx = j
                break
        # Find the next known altitude
        next_idx = None
        for j in range(i + 1, n):
            if alts[j] is not None:
                next_idx = j
                break

        if prev_idx is not None and next_idx is not None:
            # Linear interpolation
            span = next_idx - prev_idx
            frac = (i - prev_idx) / span
            alts[i] = int(round(alts[prev_idx] + frac * (alts[next_idx] - alts[prev_idx])))
        # If only one side is available, leave as None — the coordinate will
        # fall back to 2D.

    return alts


def _parse_epoch_seconds(ts) -> Optional[float]:
    """Best-effort timestamp -> Unix epoch seconds. Accepts a numeric epoch
    (already seconds) or an ISO 8601 string (the shape positions[]/
    velocities[] timestamps are stored in after CompletedFlight's
    mode="json" serialisation) -- 'Z' is normalised to '+00:00' since
    datetime.fromisoformat only accepts the latter on Python < 3.11.
    Returns None for anything unparseable rather than raising, since a
    single malformed sample shouldn't break the whole flight path."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _speed_at(t: float, samples: list[tuple[float, float]]) -> Optional[float]:
    """Nearest-match or linear-interpolate a velocity sample at time `t`,
    mirroring _interpolate_altitudes' prev/next approach but keyed on
    timestamp across a *different* list (velocities are sampled
    independently of positions) rather than by shared index. `samples` must
    already be sorted by timestamp."""
    times = [s[0] for s in samples]
    idx = bisect.bisect_left(times, t)
    if idx < len(times) and times[idx] == t:
        return samples[idx][1]

    prev = samples[idx - 1] if idx > 0 else None
    nxt = samples[idx] if idx < len(samples) else None
    if prev is not None and nxt is not None:
        span = nxt[0] - prev[0]
        if span <= 0:
            return prev[1]
        frac = (t - prev[0]) / span
        return prev[1] + frac * (nxt[1] - prev[1])
    # Only one side available (t is before the first or after the last
    # sample) -- nearest-match rather than leaving it unset.
    if prev is not None:
        return prev[1]
    if nxt is not None:
        return nxt[1]
    return None


def _interpolate_speeds(position_times: list[Optional[float]], velocities: list[dict]) -> list[Optional[int]]:
    """Return a speed (knots, rounded to the nearest integer) per position
    timestamp, nearest-matched or linearly interpolated from velocities[]'s
    own independent timestamp series. None where no velocity sample exists
    at all, or the position's own timestamp couldn't be parsed."""
    samples: list[tuple[float, float]] = []
    for v in velocities:
        vt = _parse_epoch_seconds(v.get("timestamp"))
        speed = v.get("velocity")
        if vt is not None and speed is not None:
            samples.append((vt, speed))
    samples.sort(key=lambda s: s[0])

    if not samples:
        return [None] * len(position_times)

    return [
        None if pt is None else int(round(_speed_at(pt, samples)))
        for pt in position_times
    ]


def build_flight_path(positions: list[dict], velocities: Optional[list[dict]] = None) -> Optional[dict]:
    """
    Build a 3D GeoJSON LineString Feature from a flight's position reports,
    linearly interpolating altitude where it's missing.

    `properties.coordTimes` is always included -- one Unix-epoch-seconds
    int (or None if unparseable) per coordinate, parallel to
    geometry.coordinates -- so a consumer can always correlate a point back
    to when it was recorded. `properties.coordSpeeds` (knots, nearest-
    matched/interpolated from velocities' own independent timestamp series)
    is included only when `velocities` is explicitly passed, keeping the
    lighter default shape for a caller that doesn't need it.

    Returns None when there are fewer than 2 positions.
    """
    if len(positions) < 2:
        return None

    alts = _interpolate_altitudes(positions)

    coordinates = []
    for pos, alt in zip(positions, alts):
        lon = pos.get("longitude")
        lat = pos.get("latitude")
        if alt is not None:
            coordinates.append([lon, lat, alt])
        else:
            coordinates.append([lon, lat])

    position_times = [_parse_epoch_seconds(p.get("timestamp")) for p in positions]
    properties: dict = {
        "coordTimes": [None if t is None else int(round(t)) for t in position_times],
    }
    if velocities is not None:
        properties["coordSpeeds"] = _interpolate_speeds(position_times, velocities)

    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates,
        },
        "properties": properties,
    }
