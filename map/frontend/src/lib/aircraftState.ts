// Pure reducer logic for the client-side per-aircraft state this frontend
// holds: the REST snapshot, then every WebSocket position/metadata/stale/
// remove event layered on top. Split out from hooks/useMapFlights.ts so
// the merge-never-overwrite and trail-accumulation rules are unit-testable.
//
// The live trail is built up client-side from `position` events observed
// after page load. When an aircraft is selected, the client also fetches
// the server's own accumulated trail and reseeds from it via
// applyTrailSeed(), so the drawn trail covers the whole flight and survives
// a page reload.

import type { MapFlight, MapWsEvent, TrailWirePoint } from "../api/types";
import { FALLBACK_SHAPE, resolveAircraftShape, shapeScale } from "./aircraftIconResolver";

export interface TrailPoint {
  latitude: number;
  longitude: number;
  altitude: number | null;
}

// One live sample for the aircraft detail panel's Trace Points action.
// Parallel to, not an extension of, `trail`: Trace Points additionally
// needs velocity and a timestamp per sample for its label, which the plain
// map trail never needed. Client-accumulated only, same as `trail`.
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
  /** True after a `hide` event and before the next position/metadata event
   * or a `remove`. A hidden aircraft is dropped from view but its record
   * and `trail` are kept, so a resumed flight reappears with its pre-gap
   * trail intact. */
  hidden: boolean;
  /** True once a `remove` for this aircraft has been deferred because its
   * detail panel was open (see ApplyWsEventsOptions.protectedIcaoHex and
   * releasePendingRemoval below); stays in state until the panel closes. */
  pendingRemoval?: boolean;
  /** Oldest-first; client-accumulated only, see module docstring. */
  trail: TrailPoint[];
  /** Oldest-first; see TracePoint's own docstring. */
  tracePoints: TracePoint[];
  /**
   * Epoch ms of the last genuinely new `position` or `metadata` message
   * (a monotonic max, never a plain wall-clock stamp). message-processor
   * unconditionally resends every flight's `metadata` datagram every ~60s
   * carrying the same `last_message`, so a resend must not advance this or
   * the stale/live cycle would reset every ~60s regardless of whether the
   * aircraft is still transmitting. A `position` event is never resent, so
   * its arrival time is itself a genuine freshness signal.
   */
  lastReceivedAt?: number;
  /** Resolved silhouette shape key and on-map size multiplier. Recomputed
   * only when the `aircraft` enrichment sub-object changes, not on every
   * position update. */
  shape: string;
  iconScale: number;
}

export type AircraftMap = Record<string, AircraftRecord>;

// Caps how many points a client-accumulated trail can hold, bounding
// per-feature geometry size over a long-lived page session. A point-count
// cap rather than a time-window one, since TrailPoint carries no timestamp.
// Mirrors the server's own trail-line cap (map/state_store.py's
// MAX_TRAIL_POINTS) -- kept in sync by hand across the Python/TypeScript
// boundary, since a larger client cap would never see more points from
// GET /api/flights/{icao_hex} and a smaller one would discard server
// history this client could otherwise keep.
export const MAX_TRAIL_POINTS = 25000;

// Independent of, and far smaller than, MAX_TRAIL_POINTS: Trace Points
// renders one labeled dot per sample, not a thin line segment, so an
// uncapped or trail-sized buffer would mean thousands of overlapping
// labeled dots for a long-tracked aircraft.
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
// accumulated trail, converting the wire shape to TrailPoint and dropping
// any point with no position. No-op if the aircraft isn't currently in
// state or the server returned no trail.
//
// A straight replace rather than a merge: the server's trail is the
// authoritative, more complete version of the same history. Any live
// points appended while the fetch was in flight are dropped here and
// re-appended by the next `position` event moments later.
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
   * The icao_hex the aircraft detail panel currently has open, if any. A
   * `remove` for this icao_hex is deferred (flagged `pendingRemoval`)
   * rather than deleted, so an aircraft the operator is looking at never
   * disappears out from under the open panel. Every other event type
   * still applies normally. Call releasePendingRemoval() once the panel
   * closes/deselects to apply the deferred removal.
   */
  protectedIcaoHex?: string | null;
  /** Injectable wall-clock reading (epoch milliseconds) for Trace Points
   * sample timestamps (see pushTracePoint) and for stamping
   * AircraftRecord.lastReceivedAt on position/metadata events. Defaults to
   * Date.now(). */
  now?: number;
}

// One event's effect on a single aircraft's record -- the shared core both
// applyWsEvent and applyWsEvents apply. "set" upserts `record` at
// `icaoHex`; "delete" evicts it; "noop" means this event had nothing new
// to apply. Field-level merge for position/metadata events, mirroring the
// backend's own merge-never-overwrite semantics: a field absent from this
// event leaves the existing value untouched.
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
      // A `position` event is never resent, so arrival time is a genuine
      // freshness signal; a `metadata` resend carries the same
      // `last_message` as before, so its own carried timestamp (not
      // arrival time) is what tells resend and real update apart.
      const eventTimestamp = event.type === "position" ? now : parseWireTimestamp(event.last_message);
      const previousLastReceivedAt = existing?.lastReceivedAt;
      // Monotonic max: a resend with an unchanged last_message must never
      // move lastReceivedAt; only a genuinely newer timestamp advances it.
      const nextLastReceivedAt =
        eventTimestamp == null
          ? previousLastReceivedAt
          : previousLastReceivedAt == null
            ? eventTimestamp
            : Math.max(eventTimestamp, previousLastReceivedAt);
      const isGenuinelyFresh =
        eventTimestamp != null && (previousLastReceivedAt == null || eventTimestamp > previousLastReceivedAt);
      const merged: AircraftRecord = {
        ...(existing ?? {
          icao_hex, stale: false, hidden: false, trail: [], tracePoints: [],
          shape: FALLBACK_SHAPE, iconScale: shapeScale(FALLBACK_SHAPE),
        }),
        ...fields,
        // Only genuinely fresh data un-fades a stale aircraft -- a
        // metadata resend carrying old data must never re-brighten one
        // that's already dimmed.
        stale: existing && !isGenuinelyFresh ? existing.stale : false,
        // Likewise only genuinely fresh data un-hides a hidden aircraft
        // (contact resumed); a resend must not.
        hidden: existing && !isGenuinelyFresh ? existing.hidden : false,
        pendingRemoval: false, // ...and cancels a deferred eviction (contact resumed).
      };
      merged.lastReceivedAt = nextLastReceivedAt;
      merged.trail = event.type === "position" ? pushTrailPoint(existing?.trail ?? [], merged) : (existing?.trail ?? []);
      merged.tracePoints =
        event.type === "position" ? pushTracePoint(existing?.tracePoints ?? [], merged, now) : (existing?.tracePoints ?? []);
      // Only a `metadata` event carries the `aircraft` sub-object, so only
      // then can the resolved silhouette change.
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

// Applies one WebSocket event on top of existing state. Never mutates its
// input -- returns a new AircraftMap, or the same reference on a no-op.
// Single-clone-per-call convenience for single-event callers (tests,
// mainly); applyWsEvents below is what a real WS batch goes through.
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

// Applies a whole WebSocket batch against at most *one* working copy of
// `state`, rather than reducing over applyWsEvent (which cloned the map
// per event -- O(batch size x fleet size) per batch). The clone is lazy: a
// batch whose every event is a no-op returns the exact same `state`
// reference untouched. Once a draft is created, every untouched aircraft's
// record reference is preserved exactly (aircraftMapDiff.ts's diffing
// depends on this) since the clone copies every key by reference.
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

// Applies a `remove` that was previously deferred by protectedIcaoHex --
// called once the aircraft detail panel closes or deselects. A no-op if
// the aircraft was never flagged pendingRemoval or is no longer tracked.
export function releasePendingRemoval(state: AircraftMap, icaoHex: string): AircraftMap {
  const existing = state[icaoHex];
  if (!existing || !existing.pendingRemoval) return state;
  const next = { ...state };
  delete next[icaoHex];
  return next;
}
