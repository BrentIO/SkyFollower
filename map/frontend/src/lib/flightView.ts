// Ported verbatim from management-ui/frontend/src/lib/flightView.ts's
// airportLocation() and FlightViewModal.tsx's receiverSourceLabel() -- a
// separate, standalone frontend project carries its own copy rather than
// importing across the two.

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
