// Centralized MapLibre source/layer ids for MapView.tsx, plus the
// authoritative list of layers queried for aircraft click-selection.
// Keeping that list here (rather than inline in MapView.tsx) lets the
// "range rings must never be selectable" rule be covered by a plain unit
// test instead of a full MapLibre mount.

export const AIRCRAFT_SOURCE_ID = "sf-aircraft";
export const AIRCRAFT_LAYER_ID = "sf-aircraft-icons";
// Dilated-silhouette outline for shapes whose icon_scale < 1 -- a second,
// enlarged copy of the same per-shape SDF icon, drawn underneath
// AIRCRAFT_LAYER_ID's real icon (see MapView.tsx's AIRCRAFT_LAYER_ID paint
// block for why the icon's own SDF icon-halo-* can't render a clean
// fitted ring below that scale). Matches every icon_scale < 1 aircraft,
// not just a selected one: a thin black outline unselected, the original
// larger white ring selected, both data-driven on `selected`.
//
// Shares AIRCRAFT_SOURCE_ID -- no separate data-sync wiring needed. Not in
// SELECTABLE_LAYER_IDS -- purely decorative, like TRACE_POINTS_CIRCLE_LAYER_ID.
export const AIRCRAFT_OUTLINE_LAYER_ID = "sf-aircraft-outline";
export const TRAIL_SOURCE_ID = "sf-trails";
export const TRAIL_LAYER_ID = "sf-trails-line";
// Invisible, wider line sharing TRAIL_SOURCE_ID -- this is the layer actually
// queried for click/hover so a trail is easy to hit, while TRAIL_LAYER_ID
// stays purely cosmetic at its thin rendered width.
export const TRAIL_HIT_AREA_LAYER_ID = "sf-trails-hit-area";
export const RANGE_RING_SOURCE_ID = "sf-range-rings";
export const RANGE_RING_LAYER_ID = "sf-range-rings-line";
export const RANGE_RING_LABEL_SOURCE_ID = "sf-range-ring-labels";
export const RANGE_RING_LABEL_LAYER_ID = "sf-range-ring-labels-text";
// Daily reception range outline (GET /api/range-outline?band=envelope) --
// a toggle-able overlay, distinct from the always-on static range rings
// above.
export const RANGE_OUTLINE_SOURCE_ID = "sf-range-outline";
export const RANGE_OUTLINE_LAYER_ID = "sf-range-outline-line";
// Aircraft detail panel's Trace Points action (see lib/tracePoints.ts) --
// always present with the map's other sources/layers, driven to empty
// data rather than layout-visibility-toggled when off, matching this
// file's other always-on sources (AIRCRAFT_SOURCE_ID/TRAIL_SOURCE_ID).
export const TRACE_POINTS_SOURCE_ID = "sf-trace-points";
export const TRACE_POINTS_CIRCLE_LAYER_ID = "sf-trace-points-circle";
export const TRACE_POINTS_LABEL_LAYER_ID = "sf-trace-points-label";
// Static "center" reference-point marker (black dot).
// Rendered as a map layer, not a DOM `Marker` -- a DOM marker element
// appended into MapLibre's canvas container is either entirely in front
// of the WebGL canvas or entirely behind it (there's no z-index that puts
// it behind just the icons drawn on that single opaque surface), so a
// layer added with `beforeId: AIRCRAFT_LAYER_ID` is what actually makes
// "visible, but behind aircraft icons" possible at the same time.
export const CENTER_POINT_SOURCE_ID = "sf-center-point";
export const CENTER_POINT_CIRCLE_LAYER_ID = "sf-center-point-circle";

// Live weather radar overlay (see lib/radar.ts). Both the always-current
// display and playback are built from per-frame sources+layers
// (radarAmbientFrameId / radarPlaybackFrameId in lib/radar.ts) added and
// removed whole (not layout-visibility-toggled) when the operator turns
// the layer on/off, so there's a hard guarantee nothing fetches a tile
// while off -- so there's no fixed id constant for either one here, unlike
// this file's other single-instance layers.

// The only layer(s) MapView.tsx's click handler queries for aircraft
// selection. Range rings, the range outline, the center reference point,
// and their labels are deliberately excluded -- clicking them must never
// select/open an info box.
export const SELECTABLE_LAYER_IDS: readonly string[] = [AIRCRAFT_LAYER_ID, TRAIL_HIT_AREA_LAYER_ID];
