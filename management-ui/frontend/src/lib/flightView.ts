// Pure helpers for FlightViewModal.tsx, split out so they're testable the
// same way the rest of this codebase's lib/ logic is (component tests
// aren't otherwise a thing here).

import type { ExpressionSpecification } from "maplibre-gl";
import type { FlightViewAirport } from "../api/archiveSearch";

export const EMERGENCY_SQUAWKS = new Set(["7500", "7600", "7700", "7777"]);
export const VFR_SQUAWK = "1200";

export type Coord = number[]; // [lon, lat] or [lon, lat, alt_ft]

// Altitude-to-color mapping: hue sweeps from 0 (red, ground) to 300
// (violet) as altitude rises to ~45,000ft, so a flight's climb/cruise/
// descent reads as a smooth color gradient along the path.
export function altitudeColor(altitudeFt: number | null): string {
  if (altitudeFt === null) return "hsl(0, 0%, 55%)";
  const hue = Math.max(0, Math.min(300, ((altitudeFt + 2000) / 47000) * 300));
  return `hsl(${hue.toFixed(0)}, 85%, 50%)`;
}

// A single LineString carrying every coordinate, for use with MapLibre's
// `line-gradient` paint property (which recolors along the rendered line's
// cumulative distance, not per-feature) -- this is what replaces the old
// one-feature-per-point-pair approach, which visually collapsed into dots
// at zoom levels where a segment's on-screen length was smaller than the
// line width.
export function flightPathFeature(coordinates: Coord[]) {
  return {
    type: "FeatureCollection" as const,
    features: [
      {
        type: "Feature" as const,
        geometry: {
          type: "LineString" as const,
          coordinates: coordinates.map((c) => c.slice(0, 2)),
        },
        properties: {},
      },
    ],
  };
}

// Approximate great-circle distance in meters (haversine) -- only used for
// relative cumulative-distance weighting along the path when building
// gradient stops, so the spherical-earth approximation is fine.
function haversineMeters(a: Coord, b: Coord): number {
  const earthRadiusMeters = 6371000;
  const toRad = (deg: number) => (deg * Math.PI) / 180;
  const dLat = toRad(b[1] - a[1]);
  const dLon = toRad(b[0] - a[0]);
  const lat1 = toRad(a[1]);
  const lat2 = toRad(b[1]);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLon / 2) ** 2;
  return 2 * earthRadiusMeters * Math.asin(Math.min(1, Math.sqrt(h)));
}

// Builds the `line-gradient` interpolate expression: each coordinate's
// altitude color at its normalized cumulative distance (0..1) along the
// path (MapLibre's `["line-progress"]`), so the line reads as one
// continuous gradient instead of discrete per-segment colors.
// `interpolate` requires strictly increasing input stops, so consecutive
// points at (or effectively at) the same location -- which would produce
// the same or a decreasing progress value -- are nudged forward by a
// negligible epsilon instead of producing a duplicate/out-of-order stop.
export function lineGradientExpression(coordinates: Coord[]): ExpressionSpecification {
  const neutral = altitudeColor(null);

  if (coordinates.length === 0) {
    return ["interpolate", ["linear"], ["line-progress"], 0, neutral, 1, neutral];
  }
  if (coordinates.length === 1) {
    const color = altitudeColor(coordinates[0].length > 2 ? coordinates[0][2] : null);
    return ["interpolate", ["linear"], ["line-progress"], 0, color, 1, color];
  }

  const cumulative: number[] = [0];
  for (let i = 1; i < coordinates.length; i++) {
    cumulative.push(cumulative[i - 1] + haversineMeters(coordinates[i - 1], coordinates[i]));
  }
  const total = cumulative[cumulative.length - 1];

  const EPSILON = 1e-6;
  const expression: unknown[] = ["interpolate", ["linear"], ["line-progress"]];
  let lastProgress = -Infinity;
  for (let i = 0; i < coordinates.length; i++) {
    const raw = total > 0 ? cumulative[i] / total : i / (coordinates.length - 1);
    const progress = raw <= lastProgress ? Math.min(1, lastProgress + EPSILON) : raw;
    lastProgress = progress;
    const alt = coordinates[i].length > 2 ? coordinates[i][2] : null;
    expression.push(progress, altitudeColor(alt));
  }
  return expression as ExpressionSpecification;
}

export function boundsOf(coordinates: Coord[]): [[number, number], [number, number]] {
  let minLon = Infinity;
  let minLat = Infinity;
  let maxLon = -Infinity;
  let maxLat = -Infinity;
  for (const [lon, lat] of coordinates) {
    minLon = Math.min(minLon, lon);
    maxLon = Math.max(maxLon, lon);
    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
  }
  return [
    [minLon, minLat],
    [maxLon, maxLat],
  ];
}

// "1h 17m 12s" -- drops whichever unit is zero rather than always showing
// all three, and never converts hours to days (a 30-hour ferry flight reads
// "30h 4m", not "1d 6h 4m").
export function formatDuration(startIso: string, endIso: string): string {
  const totalSeconds = Math.max(0, Math.round((new Date(endIso).getTime() - new Date(startIso).getTime()) / 1000));
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts: string[] = [];
  if (hours > 0) parts.push(`${hours}h`);
  if (minutes > 0) parts.push(`${minutes}m`);
  if (seconds > 0 || parts.length === 0) parts.push(`${seconds}s`);
  return parts.join(" ");
}

// ---------------------------------------------------------------------------
// Trace points (tar1090-style per-position dots + labels, #1441)
// ---------------------------------------------------------------------------

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

// "416 kt  16050 ft\n10:52:31" -- either measurement half may be absent
// (no surrounding velocity sample, or a 2D-only coordinate with no
// altitude) without collapsing to a double space or a stray leading unit;
// the time line is dropped entirely when the sample has no timestamp.
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
  label: string;
  sortKey: number;
}

// Builds the Point FeatureCollection the trace-points toggle's circle/symbol
// layers read from -- color and label text are precomputed per point here
// (rather than as MapLibre expressions) since altitudeColor/formatTraceLabel
// are plain JS already used elsewhere; a `["get", ...]` paint/layout
// property is cheaper than re-deriving either in an expression.
export function tracePointsFeatureCollection(
  coordinates: Coord[],
  coordTimes: (number | null)[],
  coordSpeeds: (number | null)[],
) {
  return {
    type: "FeatureCollection" as const,
    features: coordinates.map((coord, i) => {
      const altitude = coord.length > 2 ? coord[2] : null;
      const speed = coordSpeeds[i] ?? null;
      const time = coordTimes[i] ?? null;
      return {
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: coord.slice(0, 2) },
        properties: {
          color: altitudeColor(altitude),
          label: formatTraceLabel(speed, altitude, time),
          sortKey: traceLabelSortKey(i, coordinates.length),
        } satisfies TracePointProperties,
      };
    }),
  };
}

export function airportLocation(airport: FlightViewAirport): string | null {
  const filtered = [airport.city, airport.region, airport.country].filter(
    (p): p is string => !!p && p.trim() !== "",
  );
  // Drop a part equal to the one immediately before it -- e.g. region
  // "Singapore" in country "Singapore" would otherwise render
  // "Singapore, Singapore".
  const parts = filtered.filter((p, i) => i === 0 || p !== filtered[i - 1]);
  return parts.length > 0 ? parts.join(", ") : null;
}
