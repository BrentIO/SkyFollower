// Pure helpers for the aircraft detail panel's Follow action. Split out so
// MapView's recenter-on-every-update effect and the Follow-lost dimming
// rule are unit-testable without a real MapLibre map.

import type { AircraftMap, AircraftRecord } from "./aircraftState";

// The {lat, lon} to recenter the map on for the currently-followed
// aircraft, or null when there's nothing to recenter to. Keeps returning
// the aircraft's *last known* position even after it goes stale/hidden or
// a `remove` is deferred, so the map stays parked at the last place the
// aircraft was actually seen -- the "held in view" behavior Follow
// specifies on loss.
export function followTargetPosition(
  aircraft: AircraftMap,
  followId: string | null,
): { lat: number; lon: number } | null {
  if (!followId) return null;
  const a = aircraft[followId];
  if (!a || a.lat == null || a.lon == null) return null;
  return { lat: a.lat, lon: a.lon };
}

// True when `a` is the aircraft currently being Followed or the one the
// detail panel has open (`protectedId`), but has been lost -- evicted
// (pendingRemoval) or gone stale/hidden. Instead of simply disappearing
// like every other aircraft, this aircraft and its trail stay visible,
// dimmed, until the panel closes or it's deselected -- see
// featureCollections.ts's aircraftFeatureCollection/trailFeatureCollection,
// which use this to bypass their normal hidden-filter.
export function isFollowLost(
  a: Pick<AircraftRecord, "icao_hex" | "hidden" | "pendingRemoval">,
  followId: string | null,
  protectedId: string | null = null,
): boolean {
  return (a.icao_hex === followId || a.icao_hex === protectedId) && (a.hidden || !!a.pendingRemoval);
}

// Whether a map `dragstart` event should cancel Follow. MapLibre carries
// `originalEvent` (the underlying DOM pointer/touch event) only for a
// genuine user-driven drag -- a programmatic camera move (`easeTo`/
// `panTo`/`flyTo`, as used by Follow's own recenter, Zoom To, and the
// recenter button) never sets it. Also false when nothing is being
// followed, since there's nothing to cancel.
export function shouldCancelFollowOnDrag(e: { originalEvent?: unknown }, followId: string | null): boolean {
  return !!e.originalEvent && !!followId;
}
