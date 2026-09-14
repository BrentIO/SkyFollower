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
import type { AircraftRecord } from "./aircraftState";
import { altitudeColor } from "./altitudeColor";
import { isFollowLost } from "./followTarget";
import { buildTrailSegments } from "./trailSegments";

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

// Builds one aircraft's trail-segment features (one two-point LineString
// per consecutive pair of trail points -- see buildTrailSegments), each
// with a stable `${icao_hex}:${index}` id. Segment content never changes
// once drawn (each segment is a fixed pair of historical points), so a
// data-only diff tick only ever needs to add newly-appended segments and
// remove stale ones (a trail reseed replacing history wholesale, or the
// aircraft losing trail visibility) -- see MapView.tsx's sync effect.
// Does not itself check trailIncluded -- callers decide inclusion.
export function trailSegmentFeatures(a: AircraftRecord, dimmed: boolean): Feature[] {
  return buildTrailSegments(a.trail).map(
    (segment, index): Feature => ({
      type: "Feature",
      id: `${a.icao_hex}:${index}`,
      geometry: { type: "LineString", coordinates: segment.coordinates },
      properties: {
        icao_hex: a.icao_hex,
        color: segment.color,
        // See MapView.tsx's trail line-opacity paint rule -- dims the
        // Follow-lost/selected-lost aircraft's trail the same way its
        // icon is dimmed above, instead of letting it disappear with the
        // hidden filter.
        dimmed,
      },
    }),
  );
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

// TRAIL_SOURCE_ID's diff: per changed hex, removes whichever of its
// previously-synced segment ids (`syncedSegmentIds`, this MapView
// instance's own bookkeeping of what it last pushed for that hex -- see
// the sync effect) no longer appear in that hex's *current* segment set,
// and (re-)adds every current segment (upsert -- a segment whose content
// is unchanged from before is a harmless redundant re-write, cheap since
// buildTrailSegments/trailSegmentFeatures are pure and only run for
// touched hexes, not the whole fleet). Recomputing "current" fresh and
// diffing by id-set membership, rather than assuming a trail only ever
// grows by appending, is what keeps this correct across a trail *reseed*
// (applyTrailSeed replaces history wholesale) or a cap-triggered
// front-truncation (aircraftState.ts's MAX_TRAIL_POINTS), not just the
// steady-state single-point-appended case.
//
// Returns the diff plus the updated syncedSegmentIds bookkeeping -- the
// caller (MapView.tsx) owns storing it back into its ref for next tick;
// this function itself has no side effects.
export function buildTrailSourceDiff(
  changedIcaoHexes: Iterable<string>,
  aircraft: Record<string, AircraftRecord>,
  visibleIds: Set<string>,
  syncedSegmentIds: ReadonlyMap<string, string[]>,
  options: VisibilityOptions = {},
): { diff: GeoJSONSourceDiff; syncedSegmentIds: Map<string, string[]> } {
  const { followId, protectedId } = options;
  const add: Feature[] = [];
  const remove: string[] = [];
  const nextSynced = new Map(syncedSegmentIds);

  for (const hex of changedIcaoHexes) {
    const record = aircraft[hex];
    const previousIds = syncedSegmentIds.get(hex) ?? [];
    if (!record || !trailIncluded(record, visibleIds, options)) {
      remove.push(...previousIds);
      nextSynced.delete(hex);
      continue;
    }
    const dimmed = isFollowLost(record, followId ?? null, protectedId ?? null);
    const segments = trailSegmentFeatures(record, dimmed);
    const currentIds = segments.map((f) => f.id as string);
    const currentIdSet = new Set(currentIds);
    remove.push(...previousIds.filter((id) => !currentIdSet.has(id)));
    add.push(...segments);
    if (currentIds.length > 0) nextSynced.set(hex, currentIds);
    else nextSynced.delete(hex);
  }
  return { diff: { add, remove }, syncedSegmentIds: nextSynced };
}
