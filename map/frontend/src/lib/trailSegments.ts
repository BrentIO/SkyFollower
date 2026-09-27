// Builds the per-run trail coloring for MapView.tsx's trail layer. The
// live trail is grouped into contiguous *runs* of consecutive points that
// share the same altitude color, one multi-point LineString run per
// contiguous run -- not one two-point LineString per pair of points, which
// could produce tens of thousands of individual MapLibre features for a
// long-tracked aircraft or the whole fleet with "History: All" on.
//
// Each run's color is the *earlier* point's altitude at every transition,
// so a climbing departure's already-drawn trail keeps the color the
// aircraft actually was at each point rather than repainting on update.
// A run boundary point is shared between the run that ends there and the
// run that starts there, so grouping never changes a rendered coordinate
// or color -- only how many features those coordinates are split across.
//
// Pure and MapLibre-agnostic (plain coordinate pairs, not a GeoJSON
// Feature) so it's covered by plain unit tests.

import { altitudeColor } from "./altitudeColor";
import type { TrailPoint } from "./aircraftState";

export interface TrailRun {
  /** [longitude, latitude] pairs, oldest first -- 2 or more points. */
  coordinates: [number, number][];
  /** altitudeColor() shared by every point-to-point step in this run. */
  color: string;
}

// A trail with fewer than two points has nothing to draw. Recomputed from
// the full trail on every call -- callers only invoke this for aircraft
// whose trail actually changed this tick (see featureCollections.ts), and
// this still reduces MapLibre's feature count by orders of magnitude even
// though the JS-side pass itself is still O(trail length) per call.
export function buildTrailRuns(trail: TrailPoint[]): TrailRun[] {
  const runs: TrailRun[] = [];
  let current: TrailRun | null = null;

  for (let i = 0; i < trail.length - 1; i++) {
    const from = trail[i];
    const to = trail[i + 1];
    const color = altitudeColor(from.altitude);
    const fromCoord: [number, number] = [from.longitude, from.latitude];
    const toCoord: [number, number] = [to.longitude, to.latitude];

    if (current && current.color === color) {
      current.coordinates.push(toCoord);
    } else {
      current = { color, coordinates: [fromCoord, toCoord] };
      runs.push(current);
    }
  }

  return runs;
}
