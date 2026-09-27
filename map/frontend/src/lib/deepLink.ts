// Pure decision helpers for the deep-link-on-load flow (address-bar
// selection, components/MapView.tsx), extracted so they're unit-testable
// without a mounted component.

import type { AircraftMap } from "./aircraftState";

// True once `icaoHex` has appeared in tracked state. Returns false forever
// if it never appears -- the caller keeps checking on every state update
// rather than timing out, since there's nothing to distinguish "not yet"
// from "not ever" until it happens.
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
