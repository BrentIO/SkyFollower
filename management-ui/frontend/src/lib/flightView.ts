// Pure helpers for FlightViewModal.tsx, split out so they're testable the
// same way the rest of this codebase's lib/ logic is (component tests
// aren't otherwise a thing here).

import type { FlightViewAirport } from "../api/archiveSearch";

export const EMERGENCY_SQUAWKS = new Set(["7500", "7600", "7700", "7777"]);
export const VFR_SQUAWK = "1200";

export type Coord = number[]; // [lon, lat] or [lon, lat, alt_ft]

// tar1090's own altitude-to-color mapping (public/well-known): hue sweeps
// from 0 (red, ground) to 300 (violet) as altitude rises to ~45,000ft, so a
// flight's climb/cruise/descent reads the same way it would in that tool.
export function altitudeColor(altitudeFt: number | null): string {
  if (altitudeFt === null) return "hsl(0, 0%, 55%)";
  const hue = Math.max(0, Math.min(300, ((altitudeFt + 2000) / 47000) * 300));
  return `hsl(${hue.toFixed(0)}, 85%, 50%)`;
}

// MapLibre can't color a single LineString per-vertex, so the path is split
// into one short segment per point-pair, each carrying its own `color`
// property that a data-driven paint expression reads.
export function segmentFeatures(coordinates: Coord[]) {
  const features = [];
  for (let i = 0; i < coordinates.length - 1; i++) {
    const a = coordinates[i];
    const b = coordinates[i + 1];
    const altA = a.length > 2 ? a[2] : null;
    const altB = b.length > 2 ? b[2] : null;
    const alt = altA !== null && altB !== null ? (altA + altB) / 2 : (altA ?? altB);
    features.push({
      type: "Feature" as const,
      geometry: { type: "LineString" as const, coordinates: [a.slice(0, 2), b.slice(0, 2)] },
      properties: { color: altitudeColor(alt) },
    });
  }
  return { type: "FeatureCollection" as const, features };
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

export function airportLocation(airport: FlightViewAirport): string | null {
  const parts = [airport.city, airport.region, airport.country].filter(
    (p): p is string => !!p && p.trim() !== "",
  );
  return parts.length > 0 ? parts.join(", ") : null;
}
