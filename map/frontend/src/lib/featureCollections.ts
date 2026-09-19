// Pure builders for the MapLibre GeoJSON sources MapView.tsx keeps in
// sync with the live AircraftMap: the aircraft-icon feature collection and
// the per-segment trail feature collection. Split out of MapView.tsx (same
// rationale as aircraftState.ts's own split from useMapFlights.ts) so the
// hidden-aircraft filtering rules are covered by plain unit tests instead
// of a full MapLibre component mount.
//
// Every feature below carries a stable, explicit `id` (icao_hex for
// aircraft, `${icao_hex}:${segmentIndex}` for trail segments) -- required
// by MapLibre's GeoJSONSource.updateData() incremental diff API, which
// MapView.tsx's sync effect uses for a data-only tick (see that effect's
// own comment for why full setData() is still used on a visibility/toggle
// tick). The single-feature/segment builders below (aircraftFeature,
// trailSegmentFeatures) are what that diff path calls per changed
// aircraft; the *FeatureCollection functions are thin wrappers over them
// so the full-rebuild and incremental paths can never drift apart on the
// actual inclusion/dimming rules.

import type { Feature, FeatureCollection } from "geojson";
import type { GeoJSONSourceDiff } from "maplibre-gl";
import type { AircraftRecord, TrailPoint } from "./aircraftState";
import { altitudeColor } from "./altitudeColor";
import { isFollowLost } from "./followTarget";
import { buildTrailRuns } from "./trailSegments";

export const EMPTY_FEATURE_COLLECTION: FeatureCollection = { type: "FeatureCollection", features: [] };

export function hasPosition(a: AircraftRecord): a is AircraftRecord & { lat: number; lon: number } {
  return a.lat != null && a.lon != null;
}

// Isolate (isolateId), Follow (followId), and the panel-open aircraft
// (protectedId) all come from the aircraft detail panel and all key off the
// currently-selected aircraft, but they affect these builders differently:
// Isolate is a hard filter (only the isolated aircraft's icon/trail are
// ever drawn); Follow and protectedId instead *widen* what's drawn -- their
// target stays visible (dimmed, via isFollowLost) even once it would
// otherwise be filtered out for being hidden. protectedId is what keeps a
// merely-selected (not Followed) aircraft visible while its panel is open --
// see isFollowLost's own comment for why it's folded into the same check
// as followId rather than a separate mechanism.
export interface VisibilityOptions {
  isolateId?: string | null;
  followId?: string | null;
  protectedId?: string | null;
}

// A hidden aircraft (past MAP_HIDE_SECONDS, not yet evicted) is omitted
// from both feature collections below -- its record and trail are kept
// server- and client-side (see aircraftState.ts), but it must not be drawn
// until a position/metadata event un-hides it again. The exceptions are the
// actively-Followed aircraft and the aircraft the panel currently has open
// (see VisibilityOptions.followId/protectedId above).

// Whether `a` should be drawn at all right now, given VisibilityOptions --
// isolate is a hard filter; a hidden aircraft is excluded unless it's the
// actively-Followed or currently-panel-open (protectedId) exception.
// Deliberately *not* checking hasPosition -- callers that need the
// TypeScript position-narrowing side effect (e.g. aircraftFeature below,
// which reads `a.lat`/`a.lon`) must still call hasPosition themselves;
// this only covers the isolate/hidden rules. Shared by aircraftFeature and
// the info-box label feature builder (lib/infoBoxSource.ts) so "is this
// aircraft currently drawn on the map" can never drift between an
// aircraft's icon and its label -- a label should never outlive, or lag
// behind, its own icon's visibility.
export function isAircraftVisible(a: AircraftRecord, options: VisibilityOptions = {}): boolean {
  const { isolateId, followId, protectedId } = options;
  if (isolateId && a.icao_hex !== isolateId) return false;
  return a.icao_hex === followId || a.icao_hex === protectedId || !a.hidden;
}

// Builds one aircraft's icon feature, or null if it shouldn't be drawn at
// all right now (no known position, isolated out, or hidden with neither
// Follow/protectedId exception). The `id` (icao_hex) is what lets
// GeoJSONSource.updateData() treat a later call with the same id as an
// upsert of this exact feature -- see this file's module docstring.
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
      // Lighter-than-air aircraft (balloon, and airship/blimp -- aliased to
      // the same "BALL" shape, see aircraftIconResolver.ts) don't have a
      // "nose" heading the way fixed-wing/rotary aircraft do; their reported
      // ADS-B heading reflects drift direction, not an orientation the icon
      // should rotate to face. Force north-up for that shape regardless of
      // the reported value (issue #1788).
      heading: a.shape === "BALL" ? 0 : (a.hdg ?? 0),
      color: altitudeColor(a.alt ?? null),
      selected: selected.has(a.icao_hex),
      // Reuses the existing stale-dims-the-icon paint rule (see
      // MapView.tsx's icon-opacity) for the Follow-lost/selected-lost
      // case too, rather than adding a second dimming mechanism.
      stale: a.stale || isFollowLost(a, followId ?? null, protectedId ?? null),
      // Silhouette + on-map size, resolved once per metadata event in
      // aircraftState.ts (not per render). MapView registers each shape's
      // SDF image lazily, keyed by this `shape` value.
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
// the caller's visible-ids set (historyAll -> every tracked aircraft,
// otherwise just the selected one -- see MapView.tsx), not isolated out,
// and not hidden (unless it's the Follow-lost/protectedId exception, same
// widening rule as aircraftFeature above).
function trailIncluded(a: AircraftRecord, visibleIds: Set<string>, options: VisibilityOptions): boolean {
  if (!visibleIds.has(a.icao_hex)) return false;
  const { isolateId, followId, protectedId } = options;
  if (isolateId && a.icao_hex !== isolateId) return false;
  const followLost = isFollowLost(a, followId ?? null, protectedId ?? null);
  return !a.hidden || followLost;
}

// --- Trail blocks (#1838) ---------------------------------------------
//
// A trail is drawn as fixed-size blocks of TRAIL_BLOCK_SIZE points each,
// split by *absolute* point index (not array index) -- block `k` covers
// absolute indices [64k, 64k+64] inclusive, so consecutive blocks share
// their boundary point and the line stays connected. "Absolute index" is
// the point's position since the trail was first seeded/reset, which
// keeps every block's identity (and its feature ids) stable across the
// cap-triggered front-truncation in aircraftState.ts's pushTrailPoint
// (MAX_TRAIL_POINTS) -- otherwise every block would be renumbered (and
// re-sent) on every single trim.
//
// This replaced sending the whole trail as one run-per-color-change
// feature set on every changed tick (buildTrailSourceDiff's previous
// design): MapLibre's GeoJSONSource.updateData() reloads every tile that
// intersects an upserted feature's bounding box, old or new geometry, so
// re-upserting a cruising aircraft's single, ever-growing run reloaded
// nearly every tile its whole trail crossed, every tick (confirmed via a
// live DevTools trace -- see the issue this implements). Blocking the
// trail means a steady-state append tick only ever touches the block(s)
// actually still growing, at most low tens of nm of tile coverage instead
// of the trail's entire extent.
//
// Within a block, color runs still come from buildTrailRuns (#1820) on
// that block's point slice -- grouping into blocks is layered on top of,
// not instead of, that per-color-run grouping. Feature id:
// `${icao_hex}:${k}:${runInBlock}`.
export const TRAIL_BLOCK_SIZE = 64;

// Per-aircraft bookkeeping buildTrailSourceDiff needs to diff the next
// tick's trail against this tick's, and to know exactly which feature ids
// are currently in TRAIL_SOURCE_ID for that hex (so it can remove exactly
// the ones that no longer apply). `trailRef` is kept only for its
// identity (===), not read for content, across ticks -- see
// buildTrailSourceDiff's append/front-truncation detection.
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
// used by buildTrailSourceDiff to tell which of a hex's *previously*
// synced ids belong to a block being rebuilt or dropped this tick, without
// needing a second, redundant per-block index alongside the flat `ids`
// list TrailSyncState keeps.
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
        // See MapView.tsx's trail line-opacity paint rule -- dims the
        // Follow-lost/selected-lost aircraft's trail the same way its
        // icon is dimmed above, instead of letting it disappear with the
        // hidden filter.
        dimmed,
      },
    }),
  );
}

// Every block feature for one aircraft's *entire* current trail, as if
// freshly seeded (base 0) -- used by the full-rebuild path
// (trailFeatureCollection) and by buildTrailSourceDiff whenever it must
// fully re-send a hex (no prior sync record, a dimmed change, or anything
// that isn't a recognized pure-append/front-truncation, e.g. a reseed).
// Both paths call this same function so they can never drift apart on the
// actual block-splitting/run-grouping rules (this file's module comment).
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

// --- Incremental (GeoJSONSource.updateData()) diff builders -- #1775 ---
//
// Used only on a data-only sync tick (no visibility-affecting toggle
// changed this run -- see MapView.tsx's sync effect for that split).
// `changedIcaoHexes` should come from aircraftMapDiff.ts's
// diffAircraftMaps() against the *previous* render's AircraftMap, so
// these builders only ever do work proportional to what actually changed,
// not the whole fleet.

// AIRCRAFT_SOURCE_ID's diff: each changed hex either still resolves to a
// visible feature (upsert via `add` -- GeoJSONSource.updateData() treats
// an `add` with an id already present as a full replace, not a duplicate)
// or no longer does (no longer tracked, or just became hidden/isolated-out
// with no Follow/protectedId exception) -- `remove`.
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

// TRAIL_SOURCE_ID's diff (#1838): per changed hex, re-sends only the
// trail block(s) that actually changed, instead of the whole trail --
// see this file's trail-blocks module comment for why. `syncState` is
// this MapView instance's own bookkeeping (TrailSyncState) of what it
// last pushed for that hex; the caller (MapView.tsx) owns storing the
// returned `syncState` back into its ref for next tick.
//
// Four cases per changed, still-included hex:
// - No prior sync record, or `dimmed` changed: full re-send (every
//   current block), base reset to 0 -- same as a fresh seed.
// - Pure append (new trail's first/last point is === the previous
//   trail's first/last point, by reference -- aircraftState.ts's
//   trail arrays keep point identity across a spread-rebuild): only
//   the block(s) at or after the previous last point's block can have
//   changed.
// - Front truncation at the cap (the new trail's first point is found
//   later in the previous trail, and the previous trail's last point is
//   still at the corresponding offset from the new trail's end): `base`
//   advances by the dropped count; blocks now entirely before the new
//   `base` are dropped, the block now straddling `base` is rebuilt (its
//   start was trimmed), and any appended tail is re-sent per the append
//   case above.
// - Anything else (a reseed via applyTrailSeed, or any other shape this
//   doesn't specifically recognize): full re-send, same as the
//   no-prior-record case.
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
    // N[P.length-1] (not N's own last element -- N can be longer than P)
    // must still equal P's last element for this to be a pure append: the
    // whole of P is an untouched prefix of N.
    const isPureAppend =
      P.length > 0 && trail.length >= P.length && trail[0] === P[0] && trail[P.length - 1] === P[P.length - 1];

    let base = prev.base;
    // Block indices whose content must be rebuilt fresh this tick --
    // deliberately a Set of individually-named blocks, not a contiguous
    // [from, to] range: the front-truncation case touches one block near
    // the trail's (possibly very distant) start *and* the tail's block(s),
    // which are almost never adjacent on a long trail.
    const rebuildTargets = new Set<number>();
    // Old ids belonging to a block index below this are dropped outright
    // (that block no longer has any point left in the trail at all) --
    // -1 (an impossible block index) means "nothing dropped", i.e. the
    // pure-append case, where base never moves.
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

    // The tail can span more than one block in a single tick (a batch of
    // several points landing before this throttled sync fires) -- extend
    // from the highest target found so far (always the tail's start, per
    // the two cases above) through the trail's new last block.
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
