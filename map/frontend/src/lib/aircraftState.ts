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
  /**
   * True after a `hide` event and before the next position/metadata event
   * or a `remove`. A hidden aircraft is dropped from view (see
   * featureCollections.ts's feature-collection builders) but its record
   * and `trail` are kept -- a resumed flight reappears with its pre-gap
   * trail intact.
   */
  hidden: boolean;
  /** Oldest-first; client-accumulated only, see module docstring. */
  trail: TrailPoint[];
}

export type AircraftMap = Record<string, AircraftRecord>;

// Caps how many points a client-accumulated trail can hold. Since #1567
// the trail is rendered as one two-point LineString feature per
// consecutive pair of points, so an unbounded trail means unbounded
// features per aircraft on a long-lived page session. A point-count cap
// is used rather than a time-window cap because TrailPoint carries no
// timestamp -- adding one purely to support capping would be a bigger
// change than the cap itself needs. 300 deduplicated position samples is
// generous history for "a few dozen aircraft" at this map's scale.
export const MAX_TRAIL_POINTS = 300;

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
  const next = [...trail, point];
  return next.length > MAX_TRAIL_POINTS ? next.slice(next.length - MAX_TRAIL_POINTS) : next;
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
      hidden: false,
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
        ...(existing ?? { icao_hex, stale: false, hidden: false, trail: [] }),
        ...fields,
        stale: false, // Any live update un-fades a previously-stale aircraft.
        hidden: false, // ...and un-hides a previously-hidden one (contact resumed).
      };
      merged.trail = event.type === "position" ? pushTrailPoint(existing?.trail ?? [], merged) : (existing?.trail ?? []);
      return { ...state, [icao_hex]: merged };
    }
    case "stale": {
      const existing = state[event.icao_hex];
      if (!existing || existing.stale) return state;
      return { ...state, [event.icao_hex]: { ...existing, stale: true } };
    }
    case "hide": {
      const existing = state[event.icao_hex];
      // Do not delete the record -- its trail must survive so a resumed
      // flight reappears as one continuous track (see AircraftRecord.hidden).
      if (!existing || existing.hidden) return state;
      return { ...state, [event.icao_hex]: { ...existing, hidden: true } };
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
