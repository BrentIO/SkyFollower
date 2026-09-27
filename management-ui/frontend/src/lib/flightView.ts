// Pure helpers for FlightViewModal.tsx, split out to be testable like the rest of lib/.

import type { ExpressionSpecification } from "maplibre-gl";
import type { FlightViewAirport } from "../api/archiveSearch";

export const EMERGENCY_SQUAWKS = new Set(["7500", "7600", "7700", "7777"]);
export const VFR_SQUAWK = "1200";

export type SquawkPillVariant = "alert" | "vfr" | "neutral";

// Emergency/VFR codes just get emphasis, not exclusive display. Null means
// no squawk to render at all.
export function squawkPillVariant(squawk: string | null | undefined): SquawkPillVariant | null {
  if (!squawk) return null;
  if (EMERGENCY_SQUAWKS.has(squawk)) return "alert";
  if (squawk === VFR_SQUAWK) return "vfr";
  return "neutral";
}

export type Coord = number[]; // [lon, lat] or [lon, lat, alt_ft]

// Altitude-to-color lookup table (hue then lightness interpolated from the
// breakpoints below). Only "air" is needed; null altitude renders as pure
// black separately (see altitudeColor), not via a ground/unknown entry.
const COLOR_BY_ALT_AIR = {
  s: 88,
  h: [
    { alt: 0, val: 20 },
    { alt: 2000, val: 32.5 },
    { alt: 4000, val: 43 },
    { alt: 6000, val: 54 },
    { alt: 8000, val: 72 },
    { alt: 9000, val: 85 },
    { alt: 11000, val: 140 },
    { alt: 40000, val: 300 },
    { alt: 51000, val: 360 },
  ],
  l: [
    { h: 0, val: 53 },
    { h: 20, val: 50 },
    { h: 32, val: 54 },
    { h: 40, val: 52 },
    { h: 46, val: 51 },
    { h: 50, val: 46 },
    { h: 60, val: 43 },
    { h: 80, val: 41 },
    { h: 100, val: 41 },
    { h: 120, val: 41 },
    { h: 140, val: 41 },
    { h: 160, val: 40 },
    { h: 180, val: 40 },
    { h: 190, val: 44 },
    { h: 198, val: 50 },
    { h: 200, val: 58 },
    { h: 220, val: 58 },
    { h: 240, val: 58 },
    { h: 255, val: 55 },
    { h: 266, val: 55 },
    { h: 270, val: 58 },
    { h: 280, val: 58 },
    { h: 290, val: 47 },
    { h: 300, val: 43 },
    { h: 310, val: 48 },
    { h: 320, val: 48 },
    { h: 340, val: 52 },
    { h: 360, val: 53 },
  ],
};

// Trimmed to what a static archived path needs: no stale/selected/mlat/squawk
// modifiers. Null altitude renders pure black (solid enough against the light
// basemap); altitude is interpolated at its raw value, not quantized, since
// this path never updates so there's no jitter to guard against.
export function altitudeColor(altitudeFt: number | null): string {
  if (altitudeFt === null) return "hsl(0, 0%, 0%)";

  const s = COLOR_BY_ALT_AIR.s;

  const hpoints = COLOR_BY_ALT_AIR.h;
  let h = hpoints[0].val;
  for (let i = hpoints.length - 1; i >= 0; --i) {
    if (altitudeFt > hpoints[i].alt) {
      h =
        i === hpoints.length - 1
          ? hpoints[i].val
          : hpoints[i].val +
            ((hpoints[i + 1].val - hpoints[i].val) * (altitudeFt - hpoints[i].alt)) /
              (hpoints[i + 1].alt - hpoints[i].alt);
      break;
    }
  }

  const lpoints = COLOR_BY_ALT_AIR.l;
  let l = lpoints[0].val;
  for (let i = lpoints.length - 1; i >= 0; --i) {
    if (h > lpoints[i].h) {
      l =
        i === lpoints.length - 1
          ? lpoints[i].val
          : lpoints[i].val + ((lpoints[i + 1].val - lpoints[i].val) * (h - lpoints[i].h)) / (lpoints[i + 1].h - lpoints[i].h);
      break;
    }
  }

  if (h < 0) h = (h % 360) + 360;
  else if (h >= 360) h = h % 360;
  const clampedS = Math.max(0, Math.min(95, s));
  const clampedL = Math.max(0, Math.min(95, l));
  return `hsl(${h.toFixed(1)}, ${clampedS.toFixed(1)}%, ${clampedL.toFixed(1)}%)`;
}

// Darkens an altitudeColor() output ~10 lightness points (clamped at 0), same
// hue/saturation -- used for trace-point strokes so overlapping points read
// as a shade of the same color instead of a flat outline.
export function darkenColor(color: string): string {
  const match = color.match(/^hsl\(([\d.]+), ([\d.]+)%, ([\d.]+)%\)$/);
  if (!match) return color;
  const [, h, s, l] = match;
  const darkenedL = Math.max(0, Number(l) - 10);
  return `hsl(${h}, ${s}%, ${darkenedL.toFixed(1)}%)`;
}

// A single LineString for use with MapLibre's `line-gradient` paint property,
// which recolors along cumulative distance rather than per-feature.
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

// Great-circle distance in meters; used only for relative weighting along the
// path, so the spherical-earth approximation is fine.
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

// Each coordinate's altitude color at its normalized cumulative distance
// (0..1) along `["line-progress"]`. `interpolate` requires strictly
// increasing stops, so coincident points are nudged forward by an epsilon.
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

// "1h 17m 12s" -- drops zero units; never converts hours to days.
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
// Trace points (per-position dots + labels)
// ---------------------------------------------------------------------------

// A lower key wins MapLibre's `symbol-sort-key` collision resolution. Bisection
// ranks first/last highest, then the midpoint, then each quarter-point, etc.,
// so surviving labels stay spread across the track instead of bunching at one end.
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

// "416 kt  16050 ft\n10:52:31" -- either measurement may be absent without a
// stray double-space; the time line drops entirely without a timestamp.
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

// Color/label are precomputed per point (not as MapLibre expressions) since a
// `["get", ...]` paint property is cheaper than re-deriving them in-expression.
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
      const color = altitudeColor(altitude);
      return {
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: coord.slice(0, 2) },
        properties: {
          color,
          strokeColor: darkenColor(color),
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
  // Drop a part equal to the one before it, e.g. region "Singapore" in
  // country "Singapore" would otherwise render "Singapore, Singapore".
  const parts = filtered.filter((p, i) => i === 0 || p !== filtered[i - 1]);
  return parts.length > 0 ? parts.join(", ") : null;
}
