// Pure geodesic math + GeoJSON builders for the "home" range-ring overlay.
// MapLibre has no native fixed-real-world-radius circle layer -- its
// `circle` layer type is a fixed pixel radius that doesn't correspond to a
// real distance and warps with zoom/latitude. Each ring is instead built
// here as a many-vertex closed LineString whose points are geodesic
// destination points around the home coordinate (a spherical "reckon"
// calculation), not a flat-projected circle -- at 100-200nmi, a naive
// circle would visibly warp away from the equator.

import type { Feature, FeatureCollection } from "geojson";
import type { HomePoint } from "./config";

// Mean earth radius (IUGG), expressed in nautical miles.
const EARTH_RADIUS_NM = 3440.065;

// One vertex every 5 degrees of bearing around the ring.
const RING_VERTEX_COUNT = 72;

export const RANGE_RING_RADII_NM: readonly number[] = [100, 150, 200];

function toRadians(deg: number): number {
  return (deg * Math.PI) / 180;
}

function toDegrees(rad: number): number {
  return (rad * 180) / Math.PI;
}

/**
 * Spherical "reckon"/destination-point formula: given a center point, a
 * bearing (degrees, 0 = north, clockwise) and a great-circle distance in
 * nautical miles, returns the destination [longitude, latitude] -- GeoJSON
 * coordinate order.
 */
export function destinationPoint(
  center: HomePoint,
  bearingDegrees: number,
  distanceNm: number,
): [number, number] {
  const angularDistance = distanceNm / EARTH_RADIUS_NM;
  const bearing = toRadians(bearingDegrees);
  const lat1 = toRadians(center.latitude);
  const lon1 = toRadians(center.longitude);

  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(angularDistance) +
      Math.cos(lat1) * Math.sin(angularDistance) * Math.cos(bearing),
  );
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(bearing) * Math.sin(angularDistance) * Math.cos(lat1),
      Math.cos(angularDistance) - Math.sin(lat1) * Math.sin(lat2),
    );

  // Normalize longitude to (-180, 180].
  const normalizedLon = ((toDegrees(lon2) + 540) % 360) - 180;
  return [normalizedLon, toDegrees(lat2)];
}

/**
 * Closed ring of geodesic points at `radiusNm` around `center`, suitable as
 * a GeoJSON LineString's coordinates (first and last point identical).
 */
export function ringCoordinates(center: HomePoint, radiusNm: number): [number, number][] {
  const coordinates: [number, number][] = [];
  for (let i = 0; i <= RING_VERTEX_COUNT; i++) {
    const bearing = (360 * i) / RING_VERTEX_COUNT;
    coordinates.push(destinationPoint(center, bearing, radiusNm));
  }
  return coordinates;
}

// Returns an empty FeatureCollection when there's no home point -- same
// "no home configured -> nothing drawn, no error" behavior as the existing
// home marker/recenter button (see config.ts).

export function rangeRingsFeatureCollection(
  home: HomePoint | null,
  radiiNm: readonly number[] = RANGE_RING_RADII_NM,
): FeatureCollection {
  if (!home) return { type: "FeatureCollection", features: [] };
  const features: Feature[] = radiiNm.map((radiusNm) => ({
    type: "Feature",
    geometry: { type: "LineString", coordinates: ringCoordinates(home, radiusNm) },
    properties: { radiusNm },
  }));
  return { type: "FeatureCollection", features };
}

/**
 * One label point per ring, at its southernmost point (bearing 180 from
 * home) -- e.g. `{ label: "100 nmi" }` 100nmi due south of home.
 */
export function rangeRingLabelsFeatureCollection(
  home: HomePoint | null,
  radiiNm: readonly number[] = RANGE_RING_RADII_NM,
): FeatureCollection {
  if (!home) return { type: "FeatureCollection", features: [] };
  const features: Feature[] = radiiNm.map((radiusNm) => ({
    type: "Feature",
    geometry: { type: "Point", coordinates: destinationPoint(home, 180, radiusNm) },
    properties: { radiusNm, label: `${radiusNm} nmi` },
  }));
  return { type: "FeatureCollection", features };
}
