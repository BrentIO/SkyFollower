// Pure helpers for the aircraft detail panel's Follow action
// (components/MapView.tsx / components/AircraftDetailPanel.tsx). Split out
// so MapView's recenter-on-every-update effect and the Follow-lost
// dimming rule are covered by plain unit tests instead of only being
// exercisable through a real MapLibre map (this project has no jsdom/
// component-render test setup -- see lib/config.test.ts's own note).

import type { AircraftMap, AircraftRecord } from "./aircraftState";

// The {lat, lon} to recenter the map on for the currently-followed
// aircraft, or null when there's nothing to recenter to (nothing is being
// followed, or the followed aircraft has no known position yet). Follow
// keeps returning the aircraft's *last known* position even after it goes
// stale/hidden or a `remove` is deferred (see aircraftState.ts's
// pendingRemoval) -- those states don't clear lat/lon, so the map simply
// stops receiving new recenters and stays parked at the last place the
// aircraft was actually seen, which is the "held in view" behavior Follow
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

// True when `a` is the aircraft currently being Followed but has been lost
// -- evicted (pendingRemoval, deferred while the panel is open), or gone
// stale/hidden from a signal gap. This is Follow's deliberate exception to
// the panel's normal eviction-defer rule: instead of simply disappearing
// (the plain hidden/pendingRemoval behavior every other aircraft gets),
// the followed aircraft and its trail stay visible, dimmed, until the
// panel closes or the aircraft is deselected -- see
// featureCollections.ts's aircraftFeatureCollection/trailFeatureCollection,
// which use this to both bypass their normal hidden-filter and force the
// dimmed visual for this one aircraft.
export function isFollowLost(
  a: Pick<AircraftRecord, "icao_hex" | "hidden" | "pendingRemoval">,
  followId: string | null,
): boolean {
  return a.icao_hex === followId && (a.hidden || !!a.pendingRemoval);
}
