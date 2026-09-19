// Pure builders for INFO_BOX_SOURCE_ID -- the GeoJSON source behind
// MapView.tsx's INFO_BOX_LAYER_ID symbol layer, which replaced the
// DOM-based InfoBoxLayer.tsx component (issue #1808: hundreds of
// per-aircraft absolutely-positioned DOM nodes, restyled up to 20Hz, was
// the dominant cost behind sustained >100% CPU with "Labels: All" on).
//
// Same split-for-testability rationale as featureCollections.ts (this
// file's aircraft/trail-source counterpart): a label's inclusion rule and
// its diff-update path are plain, MapLibre-agnostic logic, so they're
// covered by ordinary unit tests instead of only being exercisable
// through a rendered map. lib/infoBox.ts's buildInfoBoxLines() (the
// field-order/omission rules) and lib/labelStackOrder.ts's altitudeZIndex()
// (the overlap stacking rule) are both still used here, just consumed
// differently than the removed DOM component consumed them: altitudeZIndex
// becomes each feature's `symbol-sort-key` value (MapLibre's per-feature
// paint-order control within one layer) instead of an inline CSS z-index.
//
// Every feature carries a stable `id` (icao_hex), same as
// featureCollections.ts's aircraft features -- required for
// GeoJSONSource.updateData()'s incremental diff API.

import type { Feature, FeatureCollection } from "geojson";
import type { GeoJSONSourceDiff } from "maplibre-gl";
import type { AircraftRecord } from "./aircraftState";
import { hasPosition, isAircraftVisible, type VisibilityOptions } from "./featureCollections";
import { buildInfoBoxLines } from "./infoBox";
import { altitudeZIndex } from "./labelStackOrder";

// The label-only inclusion inputs -- distinct from VisibilityOptions
// (isolate/Follow/protected, shared with the aircraft icon/trail sources)
// since these three only ever affect whether a *label* is drawn, never an
// aircraft's icon or trail. Mirrors InfoBoxLayer.tsx's removed filter
// (`items.filter((item) => showAll || selected.has(item.id) || item.id ===
// hoveredId)`) exactly.
export interface LabelFilter {
  /** icao_hex of every currently-selected aircraft -- always labeled. */
  selected: Set<string>;
  /** "Labels: All" toggle -- when on, every visible aircraft is labeled. */
  showAll: boolean;
  /** icao_hex of the aircraft currently hovered, if any. */
  hoveredId: string | null;
}

function labelIncluded(a: AircraftRecord, filter: LabelFilter): boolean {
  return filter.showAll || filter.selected.has(a.icao_hex) || a.icao_hex === filter.hoveredId;
}

/**
 * Builds one aircraft's info-box label feature, or null if it shouldn't be
 * labeled right now: no known position, not currently drawn at all (same
 * isolate/hidden rule as its icon -- see featureCollections.ts's
 * isAircraftVisible), not selected/hovered/showAll-included, or every line
 * would be empty (no ident, altitude, speed, registration, or type
 * resolved yet -- same "don't render an empty box" rule InfoBoxLayer.tsx
 * had).
 *
 * `identLine`/`detailLines`/`hasBothLines` are pre-joined here (rather
 * than left as three separate always-present properties) so the
 * newline-omission rule buildInfoBoxLines() already encodes -- omit a
 * blank line entirely rather than render a placeholder -- carries through
 * correctly into the layer's `text-field` `format` expression, which can't
 * itself decide "was this line actually present" from an empty string
 * alone without risking a stray leading/trailing blank line. `detailLines`
 * joins the altitude/speed and registration/type lines (in that order) with
 * "\n", omitting either that's null; `identLine` is the ident line alone
 * (or "" if unresolved), kept separate so the layer can render it at a
 * larger `text-scale`, matching the DOM box's bolder/larger first line.
 */
export function infoBoxLabelFeature(a: AircraftRecord, filter: LabelFilter, options: VisibilityOptions = {}): Feature | null {
  if (!hasPosition(a)) return null;
  if (!isAircraftVisible(a, options)) return null;
  if (!labelIncluded(a, filter)) return null;

  const lines = buildInfoBoxLines(a);
  if (lines.ident === null && lines.altitudeSpeed === null && lines.registrationType === null) return null;

  const identLine = lines.ident ?? "";
  const detailLines = [lines.altitudeSpeed, lines.registrationType].filter((l): l is string => l !== null).join("\n");

  return {
    type: "Feature",
    id: a.icao_hex,
    geometry: { type: "Point", coordinates: [a.lon, a.lat] },
    properties: {
      icao_hex: a.icao_hex,
      identLine,
      detailLines,
      hasBothLines: identLine !== "" && detailLines !== "",
      // Higher altitude draws on top when boxes overlap (unknown altitude
      // sits at the bottom of the stack) -- see labelStackOrder.ts.
      sortKey: altitudeZIndex(a.alt ?? null),
    },
  } satisfies Feature;
}

export function infoBoxLabelFeatureCollection(
  aircraft: Record<string, AircraftRecord>,
  filter: LabelFilter,
  options: VisibilityOptions = {},
): FeatureCollection {
  const features: Feature[] = [];
  for (const a of Object.values(aircraft)) {
    const feature = infoBoxLabelFeature(a, filter, options);
    if (feature) features.push(feature);
  }
  return { type: "FeatureCollection", features };
}

// INFO_BOX_SOURCE_ID's diff: each changed hex either still resolves to a
// labeled feature (upsert via `add`) or no longer does (no longer tracked,
// no longer drawn, no longer selected/hovered/showAll-included, or its
// content emptied out) -- `remove`, but only when that hex's id is
// actually `presentIds` (this MapView instance's own bookkeeping of what
// it last pushed to the source, see the sync effect) -- with labels
// mostly off (no "Labels: All", nothing selected/hovered), most changed
// hexes were never added in the first place, and unconditionally
// `remove`-ing them anyway used to produce a `{add: [], remove:
// [...]}` diff every single tick for no reason (#1838). Same shape as
// featureCollections.ts's buildAircraftSourceDiff otherwise; callers
// (MapView.tsx) should pass the same `changedIcaoHexes` (from
// aircraftMapDiff.ts's diffAircraftMaps) used for the aircraft/trail
// diff, since a label's content depends only on the aircraft record and
// the label filter -- not on anything else that would need a separate
// change-detection pass.
export function buildInfoBoxLabelSourceDiff(
  changedIcaoHexes: Iterable<string>,
  aircraft: Record<string, AircraftRecord>,
  filter: LabelFilter,
  presentIds: ReadonlySet<string>,
  options: VisibilityOptions = {},
): GeoJSONSourceDiff {
  const add: Feature[] = [];
  const remove: string[] = [];
  for (const hex of changedIcaoHexes) {
    const record = aircraft[hex];
    const feature = record ? infoBoxLabelFeature(record, filter, options) : null;
    if (feature) add.push(feature);
    else if (presentIds.has(hex)) remove.push(hex);
  }
  return { add, remove };
}
