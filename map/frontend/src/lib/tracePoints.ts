// Trace Points rendering logic for the aircraft detail panel's Trace
// Points action (components/AircraftDetailPanel.tsx). Ported verbatim
// (same logic, same output for the same input) from management-ui/
// frontend/src/lib/flightView.ts's traceLabelSortKey()/formatTraceLabel()/
// tracePointsFeatureCollection() -- this is a separate, standalone
// frontend project, so it carries its own copy rather than importing
// across the two (same convention as altitudeColor.ts). darkenColor()
// itself is ported into lib/altitudeColor.ts, colocated with
// altitudeColor() there the same way management-ui colocates the two.
//
// Only the *rendering* is ported: dot color, label format, and the
// decluttering sort-key. The *data source* is this frontend's own live,
// client-accumulated Trace Points samples (aircraftState.ts's
// AircraftRecord.tracePoints) rather than an archived S3 flight path's
// GeoJSON coordinates/coordTimes/coordSpeeds arrays -- so
// tracePointsFeatureCollection below takes one TracePoint[] instead of
// three parallel arrays. See the issue this implements for why the data
// source differs and why that's fine.

import type { Feature, FeatureCollection, Point } from "geojson";
import type { TracePoint } from "./aircraftState";
import { altitudeColor, darkenColor } from "./altitudeColor";

// A lower key wins MapLibre's `symbol-sort-key` conflict resolution (kept
// preferentially when labels collide). Plain index order would mean "first
// N points visible" always wins, clumping surviving labels at the start of
// the track regardless of zoom. Recursive bisection instead ranks the very
// first/last point highest, then the midpoint, then each remaining
// quarter-point, etc. -- so whichever subset MapLibre's collision detection
// keeps at a given zoom is always roughly evenly spread across the whole
// track, not bunched at one end.
export function traceLabelSortKey(index: number, total: number): number {
  if (total <= 1 || index === 0 || index === total - 1) return 0;
  let level = 0;
  let lo = 0;
  let hi = total - 1;
  while (true) {
    const mid = Math.floor((lo + hi) / 2);
    if (index === mid) return level + 1;
    level++;
    if (index < mid) hi = mid;
    else lo = mid;
  }
}

// "416 kt  16050 ft\n10:52:31 AM" -- either measurement half may be absent
// without collapsing to a double space or a stray leading unit; the time
// line is dropped entirely when the sample has no timestamp.
export function formatTraceLabel(
  speedKt: number | null,
  altitudeFt: number | null,
  epochSeconds: number | null,
): string {
  const measurements = [
    speedKt != null ? `${speedKt} kt` : null,
    altitudeFt != null ? `${altitudeFt} ft` : null,
  ].filter((p): p is string => p !== null);
  const lines = [measurements.join("  ")];
  if (epochSeconds != null) {
    lines.push(new Date(epochSeconds * 1000).toLocaleTimeString());
  }
  return lines.join("\n");
}

export interface TracePointProperties {
  color: string;
  strokeColor: string;
  label: string;
  sortKey: number;
}

// Builds the Point FeatureCollection the Trace Points circle/symbol layers
// read from -- color and label text are precomputed per point here
// (rather than as MapLibre expressions) since altitudeColor/
// formatTraceLabel are plain JS already used elsewhere; a `["get", ...]`
// paint/layout property is cheaper than re-deriving either in an
// expression.
export function tracePointsFeatureCollection(
  points: TracePoint[],
): FeatureCollection<Point, TracePointProperties> {
  const features: Feature<Point, TracePointProperties>[] = points.map((point, i) => {
    const color = altitudeColor(point.altitude);
    return {
      type: "Feature",
      geometry: { type: "Point", coordinates: [point.longitude, point.latitude] },
      properties: {
        color,
        strokeColor: darkenColor(color),
        label: formatTraceLabel(point.velocity, point.altitude, point.epochSeconds),
        sortKey: traceLabelSortKey(i, points.length),
      },
    };
  });
  return { type: "FeatureCollection", features };
}
