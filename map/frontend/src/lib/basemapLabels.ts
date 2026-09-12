import { RANGE_RING_LABEL_LAYER_ID, TRACE_POINTS_LABEL_LAYER_ID } from "./mapLayerIds";

// Discovers the basemap's own text-bearing layers generically, rather than
// hardcoding the current style's layer IDs (place names, road
// names/shields, water names, airport labels, etc.) -- those belong to a
// remote, third-party style (see lib/maplibreSetup.ts's MAP_STYLE) and
// could be renamed or restructured on a future style swap. Pure and
// MapLibre-agnostic so it's covered by plain unit tests, same pattern as
// trailSeeding.ts/infoBoxOffset.ts/selection.ts.

// The minimal shape of MapLibre's own LayerSpecification this function
// reads -- lets tests pass plain objects instead of a full
// maplibregl.LayerSpecification.
export interface StyleLayerLike {
  id: string;
  type: string;
  // `Record<string, unknown>` (not a `"text-field"`-only shape) so a real
  // maplibregl.LayerSpecification's layout -- whose exact keys vary by
  // layer type -- is assignable here without a cast.
  layout?: Record<string, unknown>;
}

// Every SkyFollower-owned layer that renders its own text, so the basemap
// label toggle never touches our own overlays. Keep this set in sync with
// mapLayerIds.ts whenever a new label-bearing overlay layer is added (e.g.
// a future range-outline label).
const SKYFOLLOWER_LABEL_LAYER_IDS: ReadonlySet<string> = new Set([
  RANGE_RING_LABEL_LAYER_ID,
  TRACE_POINTS_LABEL_LAYER_ID,
]);

// Returns the ids of every symbol layer with a `text-field` in its layout,
// excluding SkyFollower's own label layers -- i.e. the basemap's own
// place-name/road/water/POI text layers, whatever the current style calls
// them.
export function basemapLabelLayerIds(layers: readonly StyleLayerLike[]): string[] {
  return layers
    .filter((layer) => layer.type === "symbol" && layer.layout?.["text-field"] !== undefined)
    .map((layer) => layer.id)
    .filter((id) => !SKYFOLLOWER_LABEL_LAYER_IDS.has(id));
}
