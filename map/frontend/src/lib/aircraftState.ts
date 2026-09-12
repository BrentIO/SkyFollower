// Pure reducer logic for the client-side per-aircraft state this frontend
// holds: the REST snapshot, then every WebSocket position/metadata/stale/
// remove event layered on top. Split out from hooks/useMapFlights.ts so
// the merge-never-overwrite and trail-accumulation rules are covered by
// plain unit tests.
//
// The live trail this frontend draws is built up client-side, from
// `position` events/fields observed after this page loaded (plus one seed
// point from the initial snapshot's current position, if known). When an
// aircraft is selected, the client also fetches the server's own
// accumulated trail (GET /api/flights/{icao_hex}) and reseeds from it via
// applyTrailSeed(), so the drawn trail covers the whole flight rather than
// only what this browser has seen -- and survives a page reload.

import type { MapFlight, MapWsEvent, TrailWirePoint } from "../api/types";
import { FALLBACK_SHAPE, resolveAircraftShape, shapeScale } from "./aircraftIconResolver";

export interface TrailPoint {
  latitude: number;
  longitude: number;
  altitude: number | null;
}

// One live sample for the aircraft detail panel's Trace Points action (see
// components/AircraftDetailPanel.tsx / lib/tracePoints.ts). A parallel,
// richer accumulation alongside `trail` above rather than an extension of
// it: `trail` is an established, tested shape consumed by
// featureCollections.ts/trailSegments.ts, and Trace Points additionally
// needs velocity and a timestamp per sample (for the "{speed} kt
// {altitude} ft" + local-time label) that the plain map trail has never
// needed. Client-accumulated only, same as `trail` -- Trace Points draws
// this frontend's own live trail, not a server/archive-fetched one (see
// the issue this implements).
export interface TracePoint {
  latitude: number;
  longitude: number;
  altitude: number | null;
  velocity: number | null;
  /** Unix epoch seconds when this sample was captured (wall-clock read at
   * push time, not a value carried on the wire -- see pushTracePoint). */
  epochSeconds: number;
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
  /**
   * True once a `remove` event for this aircraft has been deferred because
   * its detail panel was open (see ApplyWsEventsOptions.protectedIcaoHex
   * and releasePendingRemoval below) -- the aircraft stays in state, not
   * actually evicted, until the panel closes/deselects. Never set outside
   * that deferral path.
   */
  pendingRemoval?: boolean;
  /** Oldest-first; client-accumulated only, see module docstring. */
  trail: TrailPoint[];
  /** Oldest-first; see TracePoint's own docstring. */
  tracePoints: TracePoint[];
  /**
   * The resolved silhouette shape key (aircraftIconResolver.ts) and its
   * on-map size multiplier. Computed only when the aircraft's `aircraft`
   * enrichment sub-object changes (a `metadata` event or the snapshot),
   * not on every position update -- so the icon layer never re-runs the
   * resolver per render.
   */
  shape: string;
  iconScale: number;
}

export type AircraftMap = Record<string, AircraftRecord>;

// Caps how many points a client-accumulated trail can hold. The trail is
// rendered as one two-point LineString feature per consecutive pair of
// points, so an unbounded trail means unbounded features per aircraft on
// a long-lived page session. A point-count cap is used rather than a
// time-window cap because TrailPoint carries no timestamp -- adding one
// purely to support capping would be a bigger change than the cap itself
// needs.
//
// Mirrors the server's own trail-line cap (map/state_store.py's
// MAX_TRAIL_POINTS -- see that constant's docstring for the full memory/
// rendering reasoning behind 25,000): GET /api/flights/{icao_hex}'s
// server-accumulated trail (applyTrailSeed below) is already capped to
// this same value server-side, so a larger client cap here would never
// actually see more points from that source, and a smaller one would
// discard server history this client could otherwise keep. Kept in sync
// by hand -- the two can't share a constant across the Python/TypeScript
// boundary.
//
// Independent from MAX_TRACE_POINTS below: this cap governs the drawn
// trail line only, not the Aircraft Detail Panel's Trace Points sample
// buffer (see TracePoint's own docstring for why that stays separately,
// and much more tightly, capped).
export const MAX_TRAIL_POINTS = 25000;

// Caps the Aircraft Detail Panel's Trace Points sample buffer (see
// TracePoint's docstring). Deliberately independent of, and far smaller
// than, MAX_TRAIL_POINTS above: Trace Points renders one labeled dot per
// sample (plus a "{speed} kt {altitude} ft" + local-time label), not a
// thin line segment, so it's far more visually and computationally
// expensive per point than the trail line -- an uncapped or even
// trail-line-sized buffer here would mean thousands of overlapping
// labeled dots for a single long-tracked aircraft. Left at the same 300
// the combined constant used to carry: that number was never the
// bottleneck this issue was about, and 300 labeled samples is still
// generous for what Trace Points is actually for (spot-checking a
// flight's recent history), independent of however long the drawn trail
// line itself now reaches back.
export const MAX_TRACE_POINTS = 300;

function pushTrailPoint(trail: TrailPoint[], flight: Partial<MapFlight>): TrailPoint[] {
  if (flight.lat == null || flight.lon == null) return trail;
  const point: TrailPoint = {
    latitude: flight.lat,
    longitude: flight.lon,
    altitude: flight.alt ?? null,
  };
  const last = trail[trail.length - 1];
  if (last && last.latitude === point.latitude && last.longitude === point.longitude) {
    return trail; // Same position as the last sample -- nothing new to plot.
  }
  const next = [...trail, point];
  return next.length > MAX_TRAIL_POINTS ? next.slice(next.length - MAX_TRAIL_POINTS) : next;
}

function capTrail(trail: TrailPoint[]): TrailPoint[] {
  return trail.length > MAX_TRAIL_POINTS ? trail.slice(trail.length - MAX_TRAIL_POINTS) : trail;
}

// Same push/dedupe/cap rules as pushTrailPoint, plus velocity and a
// wall-clock timestamp for Trace Points' label (see TracePoint's
// docstring). `now` is an injectable epoch-milliseconds reading (default
// Date.now()) purely so callers/tests can pin it -- there is no per-sample
// timestamp on the wire to use instead.
function pushTracePoint(points: TracePoint[], flight: Partial<MapFlight>, now: number): TracePoint[] {
  if (flight.lat == null || flight.lon == null) return points;
  const point: TracePoint = {
    latitude: flight.lat,
    longitude: flight.lon,
    altitude: flight.alt != null ? Math.round(flight.alt) : null,
    velocity: flight.velocity != null ? Math.round(flight.velocity) : null,
    epochSeconds: Math.floor(now / 1000),
  };
  const last = points[points.length - 1];
  if (last && last.latitude === point.latitude && last.longitude === point.longitude) {
    return points; // Same position as the last sample -- nothing new to plot.
  }
  const next = [...points, point];
  return next.length > MAX_TRACE_POINTS ? next.slice(next.length - MAX_TRACE_POINTS) : next;
}

// Replaces an aircraft's client-accumulated trail with the server's own
// accumulated trail (GET /api/flights/{icao_hex}'s `trail`), converting the
// wire shape (`lat`/`lon`/`alt`) to TrailPoint and dropping any point with
// no position. No-op if the aircraft isn't currently in state (it was
// removed between selecting it and the fetch resolving) or the server
// returned no trail.
//
// A straight replace rather than a merge: the server records a point per
// accepted `position` packet -- the same packets that reach this client as
// `position` WS events -- so its trail is the authoritative, more complete
// version of the same history. Any handful of live points this client
// appended while the fetch was in flight are dropped here and re-appended
// by the next `position` event a moment later (pushTrailPoint's
// same-as-last dedupe keeps the seam clean).
export function applyTrailSeed(
  state: AircraftMap,
  icaoHex: string,
  wireTrail: TrailWirePoint[],
): AircraftMap {
  const existing = state[icaoHex];
  if (!existing || wireTrail.length === 0) return state;
  const trail: TrailPoint[] = wireTrail
    .filter((p) => p.lat != null && p.lon != null)
    .map((p) => ({ latitude: p.lat, longitude: p.lon, altitude: p.alt ?? null }));
  if (trail.length === 0) return state;
  return { ...state, [icaoHex]: { ...existing, trail: capTrail(trail) } };
}

// Builds the initial AircraftMap from GET /api/flights. Seeds each
// aircraft's trail with one point from its current position, if known,
// so a trail already has a starting dot before any live position event
// arrives. `now` (default Date.now()) is only relevant to the Trace
// Points seed point's timestamp -- see pushTracePoint.
export function applySnapshot(snapshot: MapFlight[], now: number = Date.now()): AircraftMap {
  const state: AircraftMap = {};
  for (const flight of snapshot) {
    const shape = resolveAircraftShape(flight.aircraft);
    state[flight.icao_hex] = {
      ...flight,
      stale: false,
      hidden: false,
      trail: pushTrailPoint([], flight),
      tracePoints: pushTracePoint([], flight, now),
      shape,
      iconScale: shapeScale(shape),
    };
  }
  return state;
}

export interface ApplyWsEventsOptions {
  /**
   * The icao_hex the aircraft detail panel currently has open, if any (see
   * components/MapView.tsx / the panel's close-button eviction contract).
   * A `remove` event for this icao_hex is deferred (the record stays in
   * state, flagged `pendingRemoval`) rather than deleting it -- an
   * aircraft the operator is actively looking at must never disappear out
   * from under the open panel. Every other event type (including `hide`)
   * is applied normally regardless of this option; only final eviction is
   * deferred. Call releasePendingRemoval() once the panel closes/deselects
   * to apply the deferred removal. See this module's docstring and the
   * issue this implements for the frontend-vs-backend design discussion.
   */
  protectedIcaoHex?: string | null;
  /** Injectable wall-clock reading (epoch milliseconds) for Trace Points
   * sample timestamps -- see pushTracePoint. Defaults to Date.now(). */
  now?: number;
}

// Applies one WebSocket event on top of existing state. Never mutates
// its input -- returns a new AircraftMap (or the same reference when the
// event is a no-op, e.g. `stale`/`remove` for an aircraft not currently
// tracked). Field-level merge for position/metadata events, mirroring the
// backend's own merge-never-overwrite semantics (map/state_store.py's
// apply_update): a field absent from this event leaves the existing
// value untouched.
export function applyWsEvent(state: AircraftMap, event: MapWsEvent, options?: ApplyWsEventsOptions): AircraftMap {
  const now = options?.now ?? Date.now();
  switch (event.type) {
    case "position":
    case "metadata": {
      const { icao_hex } = event;
      const existing = state[icao_hex];
      const { type: _type, ...fields } = event;
      const merged: AircraftRecord = {
        ...(existing ?? {
          icao_hex, stale: false, hidden: false, trail: [], tracePoints: [],
          shape: FALLBACK_SHAPE, iconScale: shapeScale(FALLBACK_SHAPE),
        }),
        ...fields,
        stale: false, // Any live update un-fades a previously-stale aircraft.
        hidden: false, // ...and un-hides a previously-hidden one (contact resumed).
        pendingRemoval: false, // ...and cancels a deferred eviction (contact resumed).
      };
      merged.trail = event.type === "position" ? pushTrailPoint(existing?.trail ?? [], merged) : (existing?.trail ?? []);
      merged.tracePoints =
        event.type === "position" ? pushTracePoint(existing?.tracePoints ?? [], merged, now) : (existing?.tracePoints ?? []);
      // Only a `metadata` event carries the `aircraft` sub-object, so only
      // then can the resolved silhouette change -- a `position` event just
      // keeps whatever was resolved last.
      if ("aircraft" in fields) {
        merged.shape = resolveAircraftShape(merged.aircraft);
        merged.iconScale = shapeScale(merged.shape);
      }
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
      const existing = state[event.icao_hex];
      if (!existing) return state;
      if (options?.protectedIcaoHex === event.icao_hex) {
        // Deferred eviction -- see ApplyWsEventsOptions.protectedIcaoHex.
        if (existing.pendingRemoval) return state;
        return { ...state, [event.icao_hex]: { ...existing, pendingRemoval: true } };
      }
      const next = { ...state };
      delete next[event.icao_hex];
      return next;
    }
    default:
      return state;
  }
}

export function applyWsEvents(state: AircraftMap, events: MapWsEvent[], options?: ApplyWsEventsOptions): AircraftMap {
  return events.reduce((acc, event) => applyWsEvent(acc, event, options), state);
}

// Applies a `remove` that was previously deferred by protectedIcaoHex (see
// ApplyWsEventsOptions) -- called once the aircraft detail panel closes or
// deselects. A no-op (same reference) if the aircraft was never flagged
// pendingRemoval (nothing was ever deferred, so there's nothing to apply)
// or is no longer tracked at all.
export function releasePendingRemoval(state: AircraftMap, icaoHex: string): AircraftMap {
  const existing = state[icaoHex];
  if (!existing || !existing.pendingRemoval) return state;
  const next = { ...state };
  delete next[icaoHex];
  return next;
}
