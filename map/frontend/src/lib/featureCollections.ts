// Pure builders for the MapLibre GeoJSON sources MapView.tsx keeps in
// sync with the live AircraftMap: the aircraft-icon feature collection and
// the per-segment trail feature collection. Split out of MapView.tsx (same
// rationale as aircraftState.ts's own split from useMapFlights.ts) so the
// hidden-aircraft filtering rules are covered by plain unit tests instead
// of a full MapLibre component mount.

import type { Feature, FeatureCollection } from "geojson";
import type { AircraftRecord } from "./aircraftState";
import { altitudeColor } from "./altitudeColor";
import { buildTrailSegments } from "./trailSegments";

export const EMPTY_FEATURE_COLLECTION: FeatureCollection = { type: "FeatureCollection", features: [] };

export function hasPosition(a: AircraftRecord): a is AircraftRecord & { lat: number; lon: number } {
  return a.lat != null && a.lon != null;
}

// A hidden aircraft (past MAP_HIDE_SECONDS, not yet evicted) is omitted
// from both feature collections below -- its record and trail are kept
// server- and client-side (see aircraftState.ts), but it must not be drawn
// until a position/metadata event un-hides it again.

export function aircraftFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  selected: Set<string>,
): FeatureCollection {
  const features: Feature[] = Object.values(aircraft)
    .filter(hasPosition)
    .filter((a) => !a.hidden)
    .map((a) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [a.lon, a.lat] },
      properties: {
        icao_hex: a.icao_hex,
        heading: a.hdg ?? 0,
        color: altitudeColor(a.alt ?? null),
        selected: selected.has(a.icao_hex),
        stale: a.stale,
      },
    }));
  return { type: "FeatureCollection", features };
}

export function trailFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
): FeatureCollection {
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    if (!visibleIds.has(a.icao_hex)) continue;
    if (a.hidden) continue;
    // Per-segment coloring (see trailSegments.ts) -- each two-point piece
    // of the trail is colored by the altitude the aircraft actually had
    // at its earlier point, not by the aircraft's current altitude. Only
    // the icon fill (aircraftFeatureCollection above) uses current altitude.
    for (const segment of buildTrailSegments(a.trail)) {
      features.push({
        type: "Feature",
        geometry: { type: "LineString", coordinates: segment.coordinates },
        properties: {
          icao_hex: a.icao_hex,
          color: segment.color,
        },
      });
    }
  }
  return { type: "FeatureCollection", features };
}
