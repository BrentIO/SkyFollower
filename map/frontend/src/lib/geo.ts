// Spherical-earth great-circle distance -- the inverse of rangeRings.ts's
// destinationPoint() (a "reckon"/destination-point calculation). Mirrors
// map/geo.py's great_circle_nm(lat1, lon1, lat2, lon2) (haversine, same
// mean earth radius); see that module's own header comment, which
// explicitly cross-references this frontend's rangeRings.ts as its pair.
//
// Used for the aircraft detail panel's Distance row:
// greatCircleNm(config.center, { latitude: flight.lat, longitude: flight.lon }).
// config.center is CenterPoint | null -- when null, callers simply omit the
// distance row; it is not this function's job to handle a missing point,
// only to compute a distance when given two real ones.

import type { CenterPoint } from "./config";
import { EARTH_RADIUS_NM } from "./rangeRings";

function toRadians(deg: number): number {
  return (deg * Math.PI) / 180;
}

/**
 * Great-circle (haversine) distance between two lat/lon points, in
 * nautical miles.
 */
export function greatCircleNm(from: CenterPoint, to: CenterPoint): number {
  const phi1 = toRadians(from.latitude);
  const phi2 = toRadians(to.latitude);
  const dPhi = toRadians(to.latitude - from.latitude);
  const dLambda = toRadians(to.longitude - from.longitude);

  const a =
    Math.sin(dPhi / 2) ** 2 +
    Math.cos(phi1) * Math.cos(phi2) * Math.sin(dLambda / 2) ** 2;

  return 2 * EARTH_RADIUS_NM * Math.asin(Math.min(1, Math.sqrt(a)));
}

// Ported verbatim from map/geo.py's initial_bearing(lat1, lon1, lat2, lon2)
// (#1950) -- same great-circle bearing formula, same [0, 360) normalisation.
// Two identical points return 0 (atan2(0, 0) == 0 in both Python and JS),
// which callers relying on a minimum-separation gate before calling this
// (see #1950's trail-heading correction in featureCollections.ts) should
// never actually hit in practice.
/**
 * Initial great-circle bearing from `from` to `to`, in degrees clockwise
 * from true north, normalised to [0, 360).
 */
export function initialBearing(from: CenterPoint, to: CenterPoint): number {
  const phi1 = toRadians(from.latitude);
  const phi2 = toRadians(to.latitude);
  const dLambda = toRadians(to.longitude - from.longitude);

  const y = Math.sin(dLambda) * Math.cos(phi2);
  const x = Math.cos(phi1) * Math.sin(phi2) - Math.sin(phi1) * Math.cos(phi2) * Math.cos(dLambda);

  return (((Math.atan2(y, x) * 180) / Math.PI) + 360) % 360;
}
