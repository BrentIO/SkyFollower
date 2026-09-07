// Pure reducer logic for the client-side per-aircraft state this frontend
// holds: the REST snapshot, then every WebSocket position/metadata/stale/
// remove event layered on top. Split out from hooks/useMapFlights.ts so
// the merge-never-overwrite and trail-accumulation rules are covered by
// plain unit tests.
//
// There is no server-exposed trail/history endpoint (map/main.py only
// ever returns each aircraft's *current* merged state) -- the live trail
// this frontend draws is built up client-side, purely from `position`
// events/fields observed after this page loaded (plus one seed point from
// the initial snapshot's current position, if known).

import type { MapFlight, MapWsEvent } from "../api/types";

export interface TrailPoint {
  latitude: number;
  longitude: number;
  altitude: number | null;
}

export interface AircraftRecord extends MapFlight {
  /** True after a `stale` event and before the next position/metadata event or a `remove`. */
  stale: boolean;
  /** Oldest-first; client-accumulated only, see module docstring. */
  trail: TrailPoint[];
}

export type AircraftMap = Record<string, AircraftRecord>;

function pushTrailPoint(trail: TrailPoint[], flight: Partial<MapFlight>): TrailPoint[] {
  if (flight.latitude == null || flight.longitude == null) return trail;
  const point: TrailPoint = {
    latitude: flight.latitude,
    longitude: flight.longitude,
    altitude: flight.altitude ?? null,
  };
  const last = trail[trail.length - 1];
  if (last && last.latitude === point.latitude && last.longitude === point.longitude) {
    return trail; // Same position as the last sample -- nothing new to plot.
  }
  return [...trail, point];
}

// Builds the initial AircraftMap from GET /api/flights. Seeds each
// aircraft's trail with one point from its current position, if known,
// so a trail already has a starting dot before any live position event
// arrives.
export function applySnapshot(snapshot: MapFlight[]): AircraftMap {
  const state: AircraftMap = {};
  for (const flight of snapshot) {
    state[flight.icao_hex] = {
      ...flight,
      stale: false,
      trail: pushTrailPoint([], flight),
    };
  }
  return state;
}

// Applies one WebSocket event on top of existing state. Never mutates
// its input -- returns a new AircraftMap (or the same reference when the
// event is a no-op, e.g. `stale`/`remove` for an aircraft not currently
// tracked). Field-level merge for position/metadata events, mirroring the
// backend's own merge-never-overwrite semantics (map/state_store.py's
// apply_update): a field absent from this event leaves the existing
// value untouched.
export function applyWsEvent(state: AircraftMap, event: MapWsEvent): AircraftMap {
  switch (event.type) {
    case "position":
    case "metadata": {
      const { icao_hex } = event;
      const existing = state[icao_hex];
      const { type: _type, ...fields } = event;
      const merged: AircraftRecord = {
        ...(existing ?? { icao_hex, stale: false, trail: [] }),
        ...fields,
        stale: false, // Any live update un-fades a previously-stale aircraft.
      };
      merged.trail = event.type === "position" ? pushTrailPoint(existing?.trail ?? [], merged) : (existing?.trail ?? []);
      return { ...state, [icao_hex]: merged };
    }
    case "stale": {
      const existing = state[event.icao_hex];
      if (!existing || existing.stale) return state;
      return { ...state, [event.icao_hex]: { ...existing, stale: true } };
    }
    case "remove": {
      if (!(event.icao_hex in state)) return state;
      const next = { ...state };
      delete next[event.icao_hex];
      return next;
    }
    default:
      return state;
  }
}

export function applyWsEvents(state: AircraftMap, events: MapWsEvent[]): AircraftMap {
  return events.reduce(applyWsEvent, state);
}
