// Spherical-earth great-circle distance -- the inverse of rangeRings.ts's
// destinationPoint() (a "reckon"/destination-point calculation). Mirrors
// map/geo.py's great_circle_nm(lat1, lon1, lat2, lon2) (haversine, same
// mean earth radius); see that module's own header comment, which
// explicitly cross-references this frontend's rangeRings.ts as its pair.
//
// Used for the aircraft detail panel's "Distance from Home" row:
// greatCircleNm(config.home, { latitude: flight.lat, longitude: flight.lon }).
// config.home is HomePoint | null -- when null, callers simply omit the
// distance row; it is not this function's job to handle a missing point,
// only to compute a distance when given two real ones.

import type { HomePoint } from "./config";
import { EARTH_RADIUS_NM } from "./rangeRings";

function toRadians(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Great-circle (haversine) distance between two lat/lon points, in
 * nautical miles.
 */
export function greatCircleNm(from: HomePoint, to: HomePoint): number {
  const phi1 = toRadians(from.latitude);
  const phi2 = toRadians(to.latitude);
  const dPhi = toRadians(to.latitude - from.latitude);
  const dLambda = toRadians(to.longitude - from.longitude);

  const a =
    Math.sin(dPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) ** 2;

  return 2 * EARTH_RADIUS_NM * Math.asin(Math.min(1, Math.sqrt(a)));
}
