// Centralized MapLibre source/layer ids for MapView.tsx, plus the
// authoritative list of layers queried for aircraft click-selection.
// Keeping that list here (rather than inline in MapView.tsx) lets the
// "range rings must never be selectable" rule (see #1587) be covered by a
// plain unit test instead of a full MapLibre mount.

export const AIRCRAFT_SOURCE_ID = "sf-aircraft";
export const AIRCRAFT_LAYER_ID = "sf-aircraft-icons";
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

// The only layer(s) MapView.tsx's click handler queries for aircraft
// selection. Range rings, the range outline, the center reference point,
// and their labels are deliberately excluded -- clicking them must never
// select/open an info box.
export const SELECTABLE_LAYER_IDS: readonly string[] = [AIRCRAFT_LAYER_ID, TRAIL_HIT_AREA_LAYER_ID];
