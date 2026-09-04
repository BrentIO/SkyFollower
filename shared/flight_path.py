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


def build_flight_path(positions: list[dict]) -> Optional[dict]:
    """
    Build a 3D GeoJSON LineString Feature from a flight's position reports,
    linearly interpolating altitude where it's missing.

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

    return {
        "type": "Feature",
        "geometry": {
            "type": "LineString",
            "coordinates": coordinates,
        },
        "properties": {},
    }
