// Pure builders for the MapLibre GeoJSON sources MapView.tsx keeps in
// sync with the live AircraftMap: the aircraft-icon feature collection and
// the per-segment trail feature collection. Split out of MapView.tsx (same
// rationale as aircraftState.ts's own split from useMapFlights.ts) so the
// hidden-aircraft filtering rules are covered by plain unit tests instead
// of a full MapLibre component mount.

import type { Feature, FeatureCollection } from "geojson";
import type { AircraftRecord } from "./aircraftState";
import { altitudeColor } from "./altitudeColor";
import { isFollowLost } from "./followTarget";
import { buildTrailSegments } from "./trailSegments";

export const EMPTY_FEATURE_COLLECTION: FeatureCollection = { type: "FeatureCollection", features: [] };

export function hasPosition(a: AircraftRecord): a is AircraftRecord & { lat: number; lon: number } {
  return a.lat != null && a.lon != null;
}

// Isolate (isolateId) and Follow (followId) both come from the aircraft
// detail panel and both key off the currently-selected aircraft, but they
// affect these builders differently: Isolate is a hard filter (only the
// isolated aircraft's icon/trail are ever drawn); Follow instead *widens*
// what's drawn -- its target stays visible (dimmed, via isFollowLost) even
// once it would otherwise be filtered out for being hidden.
export interface VisibilityOptions {
  isolateId?: string | null;
  followId?: string | null;
}

// A hidden aircraft (past MAP_HIDE_SECONDS, not yet evicted) is omitted
// from both feature collections below -- its record and trail are kept
// server- and client-side (see aircraftState.ts), but it must not be drawn
// until a position/metadata event un-hides it again. The sole exception is
// the actively-Followed aircraft (see VisibilityOptions.followId above).

export function aircraftFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  selected: Set<string>,
  options: VisibilityOptions = {},
): FeatureCollection {
  const { isolateId, followId } = options;
  const features: Feature[] = Object.values(aircraft)
    .filter(hasPosition)
    .filter((a) => {
      if (isolateId && a.icao_hex !== isolateId) return false;
      return a.icao_hex === followId || !a.hidden;
    })
    .map((a) => ({
      type: "Feature",
      geometry: { type: "Point", coordinates: [a.lon, a.lat] },
      properties: {
        icao_hex: a.icao_hex,
        heading: a.hdg ?? 0,
        color: altitudeColor(a.alt ?? null),
        selected: selected.has(a.icao_hex),
        // Reuses the existing stale-dims-the-icon paint rule (see
        // MapView.tsx's icon-opacity) for the Follow-lost case too, rather
        // than adding a second dimming mechanism.
        stale: a.stale || isFollowLost(a, followId ?? null),
        // Silhouette + on-map size, resolved once per metadata event in
        // aircraftState.ts (not per render). MapView registers each shape's
        // SDF image lazily, keyed by this `shape` value.
        shape: a.shape,
        icon_scale: a.iconScale,
      },
    } satisfies Feature));
  return { type: "FeatureCollection", features };
}

export function trailFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
  options: VisibilityOptions = {},
): FeatureCollection {
  const { isolateId, followId } = options;
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    if (!visibleIds.has(a.icao_hex)) continue;
    if (isolateId && a.icao_hex !== isolateId) continue;
    const followLost = isFollowLost(a, followId ?? null);
    if (a.hidden && !followLost) continue;
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
          // See MapView.tsx's trail line-opacity paint rule -- dims the
          // Follow-lost aircraft's trail the same way its icon is dimmed
          // above, instead of letting it disappear with the hidden filter.
          dimmed: followLost,
        },
      });
    }
  }
  return { type: "FeatureCollection", features };
}
