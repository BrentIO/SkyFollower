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
   * Epoch ms of the last `position` or `metadata` WS event actually
   * received for this aircraft (see applyWsEvent's position/metadata case)
   * -- stamped unconditionally on every such event, unlike the wire's own
   * `last_message` field (MapFlight.last_message), which the message
   * processor only re-sends when a displayed field changes and would
   * therefore freeze on a steady cruise flight. Drives the aircraft detail
   * panel's live-relative "Last Message Received" row (lib/aircraftDetail.ts,
   * components/AircraftDetailPanel.tsx). Undefined until the first
   * position/metadata event, unless applySnapshot could seed it from the
   * wire's `last_message` on initial load.
   */
  lastReceivedAt?: number;
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
// rendered as one multi-point LineString feature per contiguous
// same-color run (trailSegments.ts's buildTrailRuns, #1820), but even a
// single steady-altitude run still holds one coordinate per point, so an
// unbounded trail still means unbounded per-feature geometry size on a
// long-lived page session. A point-count cap is used rather than a
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

// Parses the wire's ISO-8601 `last_message` timestamp (MapFlight.last_message)
// to epoch milliseconds, for seeding AircraftRecord.lastReceivedAt from the
// initial snapshot (see applySnapshot) -- returns undefined for a missing or
// unparseable value rather than NaN, so a malformed timestamp behaves the
// same as an absent one (row omitted until a live event stamps it).
function parseWireTimestamp(value: string | undefined): number | undefined {
  if (!value) return undefined;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? undefined : parsed;
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
      lastReceivedAt: parseWireTimestamp(flight.last_message),
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
   * sample timestamps (see pushTracePoint) and for stamping
   * AircraftRecord.lastReceivedAt on position/metadata events. Defaults to
   * Date.now(). */
  now?: number;
}

// One event's effect on a single aircraft's record, independent of how
// many other hexes/events are in the same batch -- the shared core both
// applyWsEvent (single-event callers, tests) and applyWsEvents (#1820:
// one map-wide clone for the whole batch, not one per event -- see that
// function's own comment) apply against whatever map object they're each
// working on. "set" upserts `record` at `icaoHex`; "delete" evicts it;
// "noop" (same reference as `existing`) means this event had nothing new
// to apply (e.g. `stale` on an already-stale aircraft, or any event for a
// hex not currently tracked except position/metadata, which always
// create one). Field-level merge for position/metadata events, mirroring
// the backend's own merge-never-overwrite semantics (map/state_store.py's
// apply_update): a field absent from this event leaves the existing
// value untouched.
type EventOutcome =
  | { kind: "set"; icaoHex: string; record: AircraftRecord }
  | { kind: "delete"; icaoHex: string }
  | { kind: "noop" };

function applyEventToRecord(existing: AircraftRecord | undefined, event: MapWsEvent, now: number, options?: ApplyWsEventsOptions): EventOutcome {
  switch (event.type) {
    case "position":
    case "metadata": {
      const { icao_hex } = event;
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
      // Unconditional on every position/metadata event -- a message was
      // actually just heard from this aircraft, regardless of whether any
      // displayed field changed (see AircraftRecord.lastReceivedAt's
      // docstring for why this deliberately differs from the wire's own
      // last_message field).
      merged.lastReceivedAt = now;
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
      return { kind: "set", icaoHex: icao_hex, record: merged };
    }
    case "stale": {
      if (!existing || existing.stale) return { kind: "noop" };
      return { kind: "set", icaoHex: event.icao_hex, record: { ...existing, stale: true } };
    }
    case "hide": {
      // Do not delete the record -- its trail must survive so a resumed
      // flight reappears as one continuous track (see AircraftRecord.hidden).
      if (!existing || existing.hidden) return { kind: "noop" };
      return { kind: "set", icaoHex: event.icao_hex, record: { ...existing, hidden: true } };
    }
    case "remove": {
      if (!existing) return { kind: "noop" };
      if (options?.protectedIcaoHex === event.icao_hex) {
        // Deferred eviction -- see ApplyWsEventsOptions.protectedIcaoHex.
        if (existing.pendingRemoval) return { kind: "noop" };
        return { kind: "set", icaoHex: event.icao_hex, record: { ...existing, pendingRemoval: true } };
      }
      return { kind: "delete", icaoHex: event.icao_hex };
    }
    default:
      return { kind: "noop" };
  }
}

// Applies one WebSocket event on top of existing state. Never mutates
// its input -- returns a new AircraftMap (or the same reference when the
// event is a no-op, e.g. `stale`/`remove` for an aircraft not currently
// tracked). See applyEventToRecord for the actual merge rules; kept as a
// single-clone-per-call convenience for single-event callers (tests,
// mainly) -- applyWsEvents below is what a real WS batch goes through.
export function applyWsEvent(state: AircraftMap, event: MapWsEvent, options?: ApplyWsEventsOptions): AircraftMap {
  const outcome = applyEventToRecord(state[eventIcaoHex(event)], event, options?.now ?? Date.now(), options);
  switch (outcome.kind) {
    case "noop":
      return state;
    case "set":
      return { ...state, [outcome.icaoHex]: outcome.record };
    case "delete": {
      const next = { ...state };
      delete next[outcome.icaoHex];
      return next;
    }
  }
}

function eventIcaoHex(event: MapWsEvent): string {
  return event.icao_hex;
}

// #1820: applies a whole WebSocket batch against at most *one* working
// copy of `state`, rather than aircraftState's previous reduce-over-
// applyWsEvent, which spread the entire map fresh on every individual
// event -- O(batch size x tracked-fleet size) object-copy work per batch,
// confirmed via a live DevTools trace as a real contributor to sustained
// high CPU with the full fleet. The clone is lazy (copy-on-write): a
// batch whose every event turns out to be a no-op (e.g. a redundant
// `stale` for an already-stale aircraft) returns the exact same `state`
// reference untouched, same as a single no-op applyWsEvent call always
// has -- avoiding a wasted top-level AircraftMap identity change (and the
// React re-render that would trigger) even though nothing changed.
// Every untouched aircraft's record reference is still preserved exactly
// once the draft *does* get created (aircraftMapDiff.ts's diffing
// depends on this -- see that file's own comment): the clone copies
// every key by reference, and only hexes an event in this batch actually
// names are ever reassigned/deleted on it afterward. A hex touched by
// more than one event in the same batch sees each event applied against
// the *result* of the previous one, same ordering guarantee the old
// reduce-based version had.
export function applyWsEvents(state: AircraftMap, events: MapWsEvent[], options?: ApplyWsEventsOptions): AircraftMap {
  const now = options?.now ?? Date.now();
  let draft: AircraftMap | null = null;
  for (const event of events) {
    const current = draft ?? state;
    const outcome = applyEventToRecord(current[eventIcaoHex(event)], event, now, options);
    if (outcome.kind === "noop") continue;
    if (!draft) draft = { ...state };
    if (outcome.kind === "set") draft[outcome.icaoHex] = outcome.record;
    else delete draft[outcome.icaoHex];
  }
  return draft ?? state;
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
