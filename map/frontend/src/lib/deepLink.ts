// Pure decision helpers for the deep-link-on-load flow (address-bar
// selection, components/MapView.tsx). Split out so "has this aircraft
// shown up in tracked state yet" and "is it safe to Zoom To now" are
// covered by plain unit tests instead of only being exercisable through a
// mounted component -- same pattern as lib/followTarget.ts.

import type { AircraftMap } from "./aircraftState";

// True once `icaoHex` has appeared in tracked state -- either already
// present in the initial GET /api/flights snapshot, or added moments
// later by a WS event. Returns false (forever, if it never appears) when
// the aircraft is unseen; the caller is expected to just keep checking on
// every state update rather than time out, since "the aircraft never
// shows up" (already landed, evicted, or a bogus identifier) is exactly
// the issue's "fail silently, normal default view" case -- there's
// nothing to distinguish "not yet" from "not ever" until it happens.
export function deepLinkAircraftAvailable(aircraft: AircraftMap, icaoHex: string): boolean {
  return icaoHex in aircraft;
}

// True once the deep-linked aircraft has a known position *and* the map
// is ready to be recentered on it -- Zoom To's own precondition (see
// MapView.tsx's handleZoomTo). Identity can arrive before position (e.g.
// an ident/squawk-only sighting), so this is checked separately from
// deepLinkAircraftAvailable rather than folded into it.
export function deepLinkReadyToZoom(aircraft: AircraftMap, icaoHex: string, mapLoaded: boolean): boolean {
  if (!mapLoaded) return false;
  const a = aircraft[icaoHex];
  return !!a && a.lat != null && a.lon != null;
}
