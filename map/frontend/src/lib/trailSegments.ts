// Builds the per-segment trail coloring for MapView.tsx's trail layer. The
// live trail (aircraftState.ts's TrailPoint[]) is split into N-1 two-point
// segments, one per consecutive pair of points, each colored by the
// *earlier* point's altitude -- the color the aircraft actually was when
// it was at that point on the map. This replaces rendering the trail as a
// single LineString colored by the aircraft's *current* altitude, which
// made a climbing departure's whole historical trail repaint to the
// cruise color on every update.
//
// Pure and MapLibre-agnostic (plain coordinate pairs, not a GeoJSON
// Feature) so it's covered by plain unit tests, same pattern as
// labelStackOrder.ts.

import { altitudeColor } from "./altitudeColor";
import type { TrailPoint } from "./aircraftState";

export interface TrailSegment {
  /** [from, to], each [longitude, latitude]. */
  coordinates: [[number, number], [number, number]];
  /** altitudeColor() of the segment's earlier point. */
  color: string;
}

// A trail with fewer than two points has nothing to draw.
export function buildTrailSegments(trail: TrailPoint[]): TrailSegment[] {
  const segments: TrailSegment[] = [];
  for (let i = 0; i < trail.length - 1; i++) {
    const from = trail[i];
    const to = trail[i + 1];
    segments.push({
      coordinates: [
        [from.longitude, from.latitude],
        [to.longitude, to.latitude],
      ],
      color: altitudeColor(from.altitude),
    });
  }
  return segments;
}
