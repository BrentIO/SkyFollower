// Pure builders for the MapLibre GeoJSON sources MapView.tsx keeps in
// sync with the live AircraftMap: the aircraft-icon feature collection and
// the per-segment trail feature collection. Split out of MapView.tsx so the
// hidden-aircraft filtering rules are unit-testable without a full MapLibre
// component mount.
//
// Every feature carries a stable, explicit `id` (icao_hex for aircraft,
// `${icao_hex}:${segmentIndex}` for trail segments), required by
// GeoJSONSource.updateData()'s incremental diff API. The single-feature/
// segment builders (aircraftFeature, trailSegmentFeatures) are what that
// diff path calls per changed aircraft; the *FeatureCollection functions
// are thin wrappers over them so the full-rebuild and incremental paths
// can never drift apart on the inclusion/dimming rules.

import type { Feature, FeatureCollection } from "geojson";
import type { GeoJSONSourceDiff } from "maplibre-gl";
import type { AircraftRecord, TrailPoint } from "./aircraftState";
import { altitudeColor } from "./altitudeColor";
import { isFollowLost } from "./followTarget";
import { greatCircleNm, initialBearing } from "./geo";
import { buildTrailRuns } from "./trailSegments";

export const EMPTY_FEATURE_COLLECTION: FeatureCollection = { type: "FeatureCollection", features: [] };

// Reported heading can go stale indefinitely once a velocity message with
// a GPS track/IAS-heading field stops arriving, even as the aircraft's
// actual track visibly changes. trailHeading() derives a heading from
// where the aircraft has actually been moving, as a check against that.

// Minimum distance (nm) a trail point must be from the aircraft's current
// position before it's used to derive a heading -- closely-spaced points
// are dominated by GPS/ADS-B jitter rather than real direction of travel.
// A brand-new track or one that hasn't moved far enough simply has no
// qualifying point, so trailHeading returns null.
const MIN_TRAIL_HEADING_SEPARATION_NM = 0.05;

// Reported heading vs. trail-derived heading must diverge by more than
// this before the trail heading wins -- a small difference is ordinary
// noise/rounding, not the stale-heading bug this exists to catch.
export const TRAIL_HEADING_DIVERGENCE_DEG = 20;

// Shortest angular distance between two compass bearings in [0, 360), e.g.
// angularDifference(10, 350) === 20, not 340.
function angularDifference(a: number, b: number): number {
  return Math.abs((((a - b + 540) % 360)) - 180);
}

// Bearing from the most recent trail point at least
// MIN_TRAIL_HEADING_SEPARATION_NM away from `current`, to `current` --
// i.e. "which way has this aircraft actually been moving," not "what did
// its last heading message say." `trail` is oldest-first (see
// aircraftState.ts), so this walks backward from the newest point. Returns
// null when no trail point qualifies.
export function trailHeading(trail: TrailPoint[], current: { latitude: number; longitude: number }): number | null {
  for (let i = trail.length - 1; i >= 0; i--) {
    const candidate = trail[i];
    if (greatCircleNm(candidate, current) >= MIN_TRAIL_HEADING_SEPARATION_NM) {
      return initialBearing(candidate, current);
    }
  }
  return null;
}

// The heading to render an aircraft's icon at: the reported `hdg` (or 0 if
// absent), unless a trail-derived heading is available and diverges from
// it by more than TRAIL_HEADING_DIVERGENCE_DEG, in which case the trail
// heading wins. Recomputed independently on every call -- no
// hysteresis/deadband, deliberately.
export function resolvedHeading(a: AircraftRecord, current: { latitude: number; longitude: number }): number {
  const reported = a.hdg ?? 0;
  const trail = trailHeading(a.trail, current);
  if (trail !== null && angularDifference(trail, reported) > TRAIL_HEADING_DIVERGENCE_DEG) {
    return trail;
  }
  return reported;
}

export function hasPosition(a: AircraftRecord): a is AircraftRecord & { lat: number; lon: number } {
  return a.lat != null && a.lon != null;
}

// Isolate, Follow, and the panel-open aircraft (protectedId) all key off
// the currently-selected aircraft but affect these builders differently:
// Isolate is a hard filter (only the isolated aircraft is ever drawn);
// Follow and protectedId instead *widen* what's drawn -- their target
// stays visible (dimmed, via isFollowLost) even once it would otherwise be
// filtered out for being hidden.
export interface VisibilityOptions {
  isolateId?: string | null;
  followId?: string | null;
  protectedId?: string | null;
}

// Whether `a` should be drawn at all right now: isolate is a hard filter;
// a hidden aircraft is excluded unless it's the actively-Followed or
// currently-panel-open (protectedId) exception. Deliberately *not*
// checking hasPosition -- callers needing that TypeScript narrowing (e.g.
// aircraftFeature below) must call hasPosition themselves.
// components/MapView.tsx's info-box filtering applies this same rule
// inline, so an aircraft's icon and its label stay in sync.
export function isAircraftVisible(a: AircraftRecord, options: VisibilityOptions = {}): boolean {
  const { isolateId, followId, protectedId } = options;
  if (isolateId && a.icao_hex !== isolateId) return false;
  return a.icao_hex === followId || a.icao_hex === protectedId || !a.hidden;
}

// Builds one aircraft's icon feature, or null if it shouldn't be drawn at
// all right now. The `id` (icao_hex) is what lets GeoJSONSource.updateData()
// treat a later call with the same id as an upsert of this exact feature.
export function aircraftFeature(
  a: AircraftRecord,
  selected: Set<string>,
  options: VisibilityOptions = {},
): Feature | null {
  if (!hasPosition(a)) return null;
  if (!isAircraftVisible(a, options)) return null;
  const { followId, protectedId } = options;
  return {
    type: "Feature",
    id: a.icao_hex,
    geometry: { type: "Point", coordinates: [a.lon, a.lat] },
    properties: {
      icao_hex: a.icao_hex,
      // Lighter-than-air aircraft (balloon/airship/blimp, aliased to "BALL")
      // don't have a "nose" heading; their reported ADS-B heading reflects
      // drift direction, not an orientation to rotate the icon to. Force
      // north-up for that shape regardless of the reported value.
      heading: a.shape === "BALL" ? 0 : resolvedHeading(a, { latitude: a.lat, longitude: a.lon }),
      color: altitudeColor(a.alt ?? null),
      selected: selected.has(a.icao_hex),
      // Reuses the stale-dims-the-icon paint rule for the Follow-lost/
      // selected-lost case too, rather than a second dimming mechanism.
      stale: a.stale || isFollowLost(a, followId ?? null, protectedId ?? null),
      // Resolved once per metadata event in aircraftState.ts, not per
      // render. MapView registers each shape's SDF image lazily.
      shape: a.shape,
      icon_scale: a.iconScale,
    },
  } satisfies Feature;
}

export function aircraftFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  selected: Set<string>,
  options: VisibilityOptions = {},
): FeatureCollection {
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    const feature = aircraftFeature(a, selected, options);
    if (feature) features.push(feature);
  }
  return { type: "FeatureCollection", features };
}

// Whether an aircraft's trail should be drawn at all right now: present in
// the caller's visible-ids set, not isolated out, and not hidden (unless
// it's the Follow-lost/protectedId exception, same widening rule as
// aircraftFeature above).
function trailIncluded(a: AircraftRecord, visibleIds: Set<string>, options: VisibilityOptions): boolean {
  if (!visibleIds.has(a.icao_hex)) return false;
  const { isolateId, followId, protectedId } = options;
  if (isolateId && a.icao_hex !== isolateId) return false;
  const followLost = isFollowLost(a, followId ?? null, protectedId ?? null);
  return !a.hidden || followLost;
}

// --- Trail blocks -------------------------------------------------------
//
// A trail is drawn as fixed-size blocks of TRAIL_BLOCK_SIZE points each,
// split by *absolute* point index (not array index) -- block `k` covers
// absolute indices [64k, 64k+64] inclusive, so consecutive blocks share
// their boundary point and the line stays connected. "Absolute index" is
// the point's position since the trail was first seeded/reset, which keeps
// every block's identity stable across aircraftState.ts's cap-triggered
// front-truncation -- otherwise every block would be renumbered (and
// re-sent) on every single trim.
//
// GeoJSONSource.updateData() reloads every tile intersecting an upserted
// feature's bounding box, so re-upserting a whole ever-growing trail as one
// run reloaded nearly every tile it crossed, every tick. Blocking the trail
// means a steady-state append tick only touches the block(s) still growing.
//
// Within a block, color runs still come from buildTrailRuns on that
// block's point slice -- grouping into blocks is layered on top of, not
// instead of, that per-color-run grouping. Feature id:
// `${icao_hex}:${k}:${runInBlock}`.
export const TRAIL_BLOCK_SIZE = 64;

// Per-aircraft bookkeeping buildTrailSourceDiff needs to diff the next
// tick's trail against this tick's, and to know exactly which feature ids
// are currently in TRAIL_SOURCE_ID for that hex. `trailRef` is kept only
// for its identity (===), not its content, across ticks.
export interface TrailSyncState {
  trailRef: TrailPoint[];
  /** Absolute index of trailRef[0] (points dropped off the front since seed/reset). */
  base: number;
  dimmed: boolean;
  /** Every feature id currently pushed to the source for this hex, across every block. */
  ids: string[];
}

// The half-open-by-inclusive-endpoint array-index range (into `trail`,
// [start, end] both inclusive) that block `k` covers, given `base`, or
// null if that range holds fewer than 2 points (nothing to draw -- a run
// needs at least a from/to pair).
function trailBlockIndexRange(trailLength: number, base: number, k: number): [number, number] | null {
  if (trailLength < 1) return null;
  const absStart = k * TRAIL_BLOCK_SIZE;
  const absEnd = absStart + TRAIL_BLOCK_SIZE;
  const trailAbsEnd = base + trailLength - 1;
  const start = Math.max(absStart, base);
  const end = Math.min(absEnd, trailAbsEnd);
  if (end - start < 1) return null;
  return [start - base, end - base];
}

function trailFirstBlockIndex(base: number): number {
  return Math.floor(base / TRAIL_BLOCK_SIZE);
}

function trailLastBlockIndex(trailLength: number, base: number): number {
  return Math.floor((base + trailLength - 1) / TRAIL_BLOCK_SIZE);
}

// Parses the block index `k` back out of a `${icao_hex}:${k}:${run}` id --
// lets buildTrailSourceDiff tell which previously-synced ids belong to a
// block being rebuilt or dropped, without a redundant per-block index.
function trailBlockIndexFromId(id: string): number {
  return Number(id.slice(id.indexOf(":") + 1, id.lastIndexOf(":")));
}

// Builds one block's run features for one aircraft. Does not itself check
// trailIncluded -- callers decide inclusion.
function trailBlockFeatures(icaoHex: string, trail: TrailPoint[], base: number, k: number, dimmed: boolean): Feature[] {
  const range = trailBlockIndexRange(trail.length, base, k);
  if (!range) return [];
  const [start, end] = range;
  return buildTrailRuns(trail.slice(start, end + 1)).map(
    (run, index): Feature => ({
      type: "Feature",
      id: `${icaoHex}:${k}:${index}`,
      geometry: { type: "LineString", coordinates: run.coordinates },
      properties: {
        icao_hex: icaoHex,
        color: run.color,
        // Dims the Follow-lost/selected-lost aircraft's trail the same way
        // its icon is dimmed, instead of letting it disappear.
        dimmed,
      },
    }),
  );
}

// Every block feature for one aircraft's *entire* current trail, as if
// freshly seeded (base 0). Used by both the full-rebuild path
// (trailFeatureCollection) and buildTrailSourceDiff's full-resend cases, so
// they can never drift apart on the block-splitting/run-grouping rules.
export function trailSegmentFeatures(a: AircraftRecord, dimmed: boolean): Feature[] {
  const trail = a.trail;
  if (trail.length < 2) return [];
  const features: Feature[] = [];
  const last = trailLastBlockIndex(trail.length, 0);
  for (let k = 0; k <= last; k++) {
    features.push(...trailBlockFeatures(a.icao_hex, trail, 0, k, dimmed));
  }
  return features;
}

export function trailFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
  options: VisibilityOptions = {},
): FeatureCollection {
  const { followId, protectedId } = options;
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    if (!trailIncluded(a, visibleIds, options)) continue;
    const dimmed = isFollowLost(a, followId ?? null, protectedId ?? null);
    features.push(...trailSegmentFeatures(a, dimmed));
  }
  return { type: "FeatureCollection", features };
}

// A diff with nothing in it. MapLibre still does a full worker round-trip
// and tile reload for an empty updateData() call, so callers skip sending
// these (e.g. every trail diff while no trails are visible).
export function isEmptySourceDiff(diff: GeoJSONSourceDiff): boolean {
  return !diff.removeAll && !diff.add?.length && !diff.remove?.length && !diff.update?.length;
}

// --- Incremental (GeoJSONSource.updateData()) diff builders -------------
//
// Used only on a data-only sync tick (no visibility-affecting toggle
// changed this run -- see MapView.tsx's sync effect). `changedIcaoHexes`
// should come from aircraftMapDiff.ts's diffAircraftMaps(), so these
// builders only do work proportional to what actually changed.

// AIRCRAFT_SOURCE_ID's diff: each changed hex either still resolves to a
// visible feature (upsert via `add`) or no longer does (`remove`).
export function buildAircraftSourceDiff(
  changedIcaoHexes: Iterable<string>,
  aircraft: Record<string, AircraftRecord>,
  selected: Set<string>,
  options: VisibilityOptions = {},
): GeoJSONSourceDiff {
  const add: Feature[] = [];
  const remove: string[] = [];
  for (const hex of changedIcaoHexes) {
    const record = aircraft[hex];
    const feature = record ? aircraftFeature(record, selected, options) : null;
    if (feature) add.push(feature);
    else remove.push(hex);
  }
  return { add, remove };
}

// TRAIL_SOURCE_ID's diff: per changed hex, re-sends only the trail
// block(s) that actually changed, instead of the whole trail. `syncState`
// is bookkeeping (TrailSyncState) of what was last pushed for that hex;
// the caller owns storing the returned `syncState` back for next tick.
//
// Four cases per changed, still-included hex:
// - No prior sync record, or `dimmed` changed: full re-send, base reset
//   to 0 -- same as a fresh seed.
// - Pure append (new trail's first/last point is === the previous trail's
//   first/last point by reference): only the block(s) at or after the
//   previous last point's block can have changed.
// - Front truncation at the cap: `base` advances by the dropped count;
//   blocks entirely before the new `base` are dropped, the block now
//   straddling `base` is rebuilt, and any appended tail is re-sent per
//   the append case above.
// - Anything else (a reseed via applyTrailSeed, or any unrecognized
//   shape): full re-send, same as the no-prior-record case.
export function buildTrailSourceDiff(
  changedIcaoHexes: Iterable<string>,
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
  syncState: ReadonlyMap<string, TrailSyncState>,
  options: VisibilityOptions = {},
): { diff: GeoJSONSourceDiff; syncState: Map<string, TrailSyncState> } {
  const { followId, protectedId } = options;
  const add: Feature[] = [];
  const remove: string[] = [];
  const next = new Map(syncState);

  const fullResend = (hex: string, record: AircraftRecord, dimmed: boolean, prev: TrailSyncState | undefined) => {
    if (prev) remove.push(...prev.ids);
    const features = trailSegmentFeatures(record, dimmed);
    add.push(...features);
    const ids = features.map((f) => f.id as string);
    if (ids.length > 0) next.set(hex, { trailRef: record.trail, base: 0, dimmed, ids });
    else next.delete(hex);
  };

  for (const hex of changedIcaoHexes) {
    const record = aircraft[hex];
    const prev = syncState.get(hex);

    if (!record || !trailIncluded(record, visibleIds, options)) {
      if (prev) remove.push(...prev.ids);
      next.delete(hex);
      continue;
    }

    const dimmed = isFollowLost(record, followId ?? null, protectedId ?? null);
    const trail = record.trail;

    if (!prev || prev.dimmed !== dimmed) {
      fullResend(hex, record, dimmed, prev);
      continue;
    }

    const P = prev.trailRef;
    // Pure append: P[P.length-1] must still equal trail[P.length-1] (P is
    // an untouched prefix of the new trail), not merely trail's own last.
    const isPureAppend =
      P.length > 0 && trail.length >= P.length && trail[0] === P[0] && trail[P.length - 1] === P[P.length - 1];

    let base = prev.base;
    // A Set, not a contiguous range: front-truncation touches one block
    // near the trail's start *and* the tail's block(s), rarely adjacent.
    const rebuildTargets = new Set<number>();
    // -1 means nothing dropped (the pure-append case, base never moves);
    // otherwise, ids in a block below this no longer have a point left.
    let dropBefore = -1;

    if (isPureAppend) {
      rebuildTargets.add(trailFirstBlockIndex(base + P.length - 1));
    } else {
      const d = P.length > 0 ? P.indexOf(trail[0]) : -1;
      const oldLastIdxInNew = P.length - 1 - d;
      const isFrontTruncation =
        d > 0 && oldLastIdxInNew >= 0 && oldLastIdxInNew < trail.length && trail[oldLastIdxInNew] === P[P.length - 1];
      if (!isFrontTruncation) {
        fullResend(hex, record, dimmed, prev);
        continue;
      }
      const oldLastAbsolute = base + P.length - 1;
      base += d;
      dropBefore = trailFirstBlockIndex(base);
      rebuildTargets.add(dropBefore); // the block now containing `base` -- its start was trimmed.
      rebuildTargets.add(trailFirstBlockIndex(oldLastAbsolute)); // the appended tail's first block.
    }

    // The tail can span more than one block in a single tick -- extend
    // from the highest target found so far through the new last block.
    const toBlock = trailLastBlockIndex(trail.length, base);
    for (let k = Math.max(...rebuildTargets); k <= toBlock; k++) rebuildTargets.add(k);

    const rebuiltIdsByBlock = new Map<number, string[]>();
    for (const k of rebuildTargets) {
      const features = trailBlockFeatures(hex, trail, base, k, dimmed);
      const ids = features.map((f) => f.id as string);
      rebuiltIdsByBlock.set(k, ids);
      add.push(...features);
    }

    for (const id of prev.ids) {
      const k = trailBlockIndexFromId(id);
      const rebuilt = rebuiltIdsByBlock.get(k);
      if (k < dropBefore || (rebuilt && !rebuilt.includes(id))) remove.push(id);
    }

    const untouchedIds = prev.ids.filter((id) => {
      const k = trailBlockIndexFromId(id);
      return k >= dropBefore && !rebuiltIdsByBlock.has(k);
    });
    next.set(hex, { trailRef: trail, base, dimmed, ids: [...untouchedIds, ...rebuiltIdsByBlock.values()].flat() });
  }

  return { diff: { add, remove }, syncState: next };
}
