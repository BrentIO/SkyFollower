// Stack order for overlapping info-box labels (see
// components/InfoBoxLayer.tsx) -- boxes are allowed to overlap freely, so
// which one draws on top of a cluster is decided purely by altitude
// rather than by any collision-avoidance placement.
//
// Altitude (0-60,000ft, the realistic range for tracked traffic) is
// linearly mapped onto a small bounded integer z-index range. An aircraft
// with no known altitude gets a fixed floor value below every
// known-altitude box. Two aircraft can legitimately map to the same
// z-index (e.g. two aircraft holding at the same flight level, or two
// altitudes that round into the same bucket) -- ties are broken by
// icao_hex so the stack order is fully deterministic and never flickers
// between renders for an unchanged set of aircraft.

export const UNKNOWN_ALTITUDE_Z_INDEX = 0;

const MAX_ALTITUDE_FT = 60_000;
const MAX_KNOWN_Z_INDEX = 1000;

// Maps a known altitude onto [1, MAX_KNOWN_Z_INDEX]; out-of-range values
// are clamped rather than extrapolated.
export function altitudeZIndex(altitudeFt: number | null | undefined): number {
  if (altitudeFt == null) return UNKNOWN_ALTITUDE_Z_INDEX;
  const clamped = Math.min(Math.max(altitudeFt, 0), MAX_ALTITUDE_FT);
  return 1 + Math.round((clamped / MAX_ALTITUDE_FT) * (MAX_KNOWN_Z_INDEX - 1));
}

export interface LabelStackInput {
  /** Stable identifier (icao_hex) -- also the tie-break key. */
  id: string;
  altitude: number | null | undefined;
}

// Deterministic ascending stack order (lowest first, so callers rendering
// in this order paint the highest-altitude/lowest-priority box last, i.e.
// on top): sorted by z-index, then by id for any tie.
export function sortByLabelStackOrder<T extends LabelStackInput>(items: T[]): T[] {
  return [...items].sort((a, b) => {
    const za = altitudeZIndex(a.altitude);
    const zb = altitudeZIndex(b.altitude);
    if (za !== zb) return za - zb;
    return a.id.localeCompare(b.id);
  });
}
