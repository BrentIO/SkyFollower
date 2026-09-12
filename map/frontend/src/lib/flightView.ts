// Ported verbatim (logic and dedup rule unchanged) from
// management-ui/frontend/src/lib/flightView.ts's airportLocation() and
// FlightViewModal.tsx's receiverSourceLabel() -- this is a separate,
// standalone frontend project, so it carries its own copy rather than
// importing across the two. Same "separate project, own copy" convention
// as this file's sibling altitudeColor.ts.

import type { AirportRef } from "../api/types";

/**
 * Builds the "city, region, country" display string shown under an
 * airport's name, dropping a part equal to the one immediately before it
 * -- e.g. a region named the same as its country: "Singapore, Singapore"
 * -> "Singapore". Returns null when no part is present.
 */
export function airportLocation(airport: AirportRef): string | null {
  const filtered = [airport.city, airport.region, airport.country].filter(
    (p): p is string => !!p && p.trim() !== "",
  );
  // Drop a part equal to the one immediately before it -- e.g. region
  // "Singapore" in country "Singapore" would otherwise render
  // "Singapore, Singapore".
  const parts = filtered.filter((p, i) => i === 0 || p !== filtered[i - 1]);
  return parts.length > 0 ? parts.join(", ") : null;
}

/**
 * Short display form for a receiver_sources entry -- "EXTERNAL" -> "External",
 * everything else ("1090"/"978") passed through unchanged.
 */
export function receiverSourceLabel(source: string): string {
  return source === "EXTERNAL" ? "External" : source;
}
