// Builds the per-run trail coloring for MapView.tsx's trail layer. The
// live trail (aircraftState.ts's TrailPoint[]) is grouped into contiguous
// *runs* of consecutive points that share the same altitude color, one
// multi-point LineString run per contiguous run -- not one two-point
// LineString per individual pair of points (#1820: that per-pair design
// meant a single long-tracked aircraft, or the whole fleet with "History:
// All" on, could produce tens of thousands of individual MapLibre
// features -- confirmed via a live DevTools trace as the dominant driver
// of ~93% GPU-process and ~51% MapLibre-worker-thread CPU. A real 747-hour
// cruise sits at one altitude/color for almost its entire trail, so
// grouping collapses that to a small handful of features with no loss of
// the colored altitude "story" -- the exact same color-per-point, drawn
// as fewer, longer lines instead of thousands of tiny disconnected ones).
//
// Each run's color is still the *earlier* point's altitude at every
// transition -- unchanged from the original per-segment design, and
// still what makes a climbing departure's already-drawn trail keep the
// color the aircraft actually was at each point, rather than repainting
// to the current altitude on every update.
//
// A run boundary point is shared between the run that ends there and the
// run that starts there (matching how a color transition itself renders:
// the two runs visually connect, exactly as the un-grouped segments did),
// so grouping never changes a single rendered coordinate or color -- only
// how many separate features those coordinates are split across.
//
// Pure and MapLibre-agnostic (plain coordinate pairs, not a GeoJSON
// Feature) so it's covered by plain unit tests, same pattern as
// labelStackOrder.ts.

import { altitudeColor } from "./altitudeColor";
import type { TrailPoint } from "./aircraftState";

export interface TrailRun {
  /** [longitude, latitude] pairs, oldest first -- 2 or more points. */
  coordinates: [number, number][];
  /** altitudeColor() shared by every point-to-point step in this run. */
  color: string;
}

// A trail with fewer than two points has nothing to draw. Recomputed from
// the full trail on every call (same cost profile the original
// buildTrailSegments had) -- callers only invoke this for aircraft whose
// trail actually changed this tick (see featureCollections.ts), and this
// still reduces MapLibre's feature count by orders of magnitude even
// though the JS-side pass itself is still O(trail length) per call. A
// further incremental (only reprocess newly-appended points) pass is a
// natural follow-up if profiling still shows this JS cost material, but
// isn't needed to fix the GPU/worker bottleneck this addresses -- see
// #1820.
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
