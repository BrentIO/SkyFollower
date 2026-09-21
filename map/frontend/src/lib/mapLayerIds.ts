// Centralized MapLibre source/layer ids for MapView.tsx, plus the
// authoritative list of layers queried for aircraft click-selection.
// Keeping that list here (rather than inline in MapView.tsx) lets the
// "range rings must never be selectable" rule (see #1587) be covered by a
// plain unit test instead of a full MapLibre mount.

export const AIRCRAFT_SOURCE_ID = "sf-aircraft";
export const AIRCRAFT_LAYER_ID = "sf-aircraft-icons";
// Dilated-silhouette outline for shapes whose icon_scale < 1 -- a second,
// enlarged copy of the same per-shape SDF icon, drawn underneath
// AIRCRAFT_LAYER_ID's real icon (#1816, following up on #1806's
// fixed-circle version -- see MapView.tsx's AIRCRAFT_LAYER_ID paint block
// for why the icon's own SDF icon-halo-* can't render a clean fitted ring
// below that scale: a fixed EDGE_GAMMA shader term doesn't cancel against
// the shrinking fontScale, so the halo overflows into a filled box).
//
// #1912: broadened from "selected aircraft only" to every icon_scale < 1
// aircraft -- tar1090-style contrast against busy backgrounds (originally
// the radar overlay's own green returns, but a general readability fix)
// needs a permanent thin black outline on every aircraft, not just a
// selection indicator. This one layer now serves both: a thin black
// dilated silhouette for an unselected aircraft, the original larger white
// one when selected (see MapView.tsx's icon-size/icon-color, both now
// data-driven on `selected` instead of the layer being selected-only).
// icon_scale >= 1 aircraft don't use this layer at all -- the icon's own
// icon-halo-* is safe there (no wash bug above icon_scale 1) and now
// always draws a thin black halo too, brightening to the same white
// selection ring when selected (see AIRCRAFT_LAYER_ID's paint block).
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

// Live weather radar overlay (#1896, see lib/radar.ts for the tile-URL/
// zoom-bounds/frame-sequence logic). RADAR_SOURCE_ID/RADAR_LAYER_ID is the
// always-current snapshot, added/removed whole (not layout-visibility-
// toggled) when the operator turns the layer on/off, so there's a hard
// guarantee it never fetches a tile while off rather than relying on
// whether an invisible layer's source still requests tiles. Added first
// inside the map's "load" handler, before any other SkyFollower layer, so
// every later plain addLayer() (no explicit beforeId) naturally stacks
// above it -- "above the base map, below everything this app draws."
export const RADAR_SOURCE_ID = "sf-radar";
export const RADAR_LAYER_ID = "sf-radar-raster";
// Playback (#1896; rebuilt in #1910's 2nd attempt) uses one source+layer
// per frame (see lib/radar.ts's radarPlaybackFrameId), not a single
// reused id -- so there's no fixed constant for it here, unlike the
// current-snapshot source/layer above.

// The only layer(s) MapView.tsx's click handler queries for aircraft
// selection. Range rings, the range outline, the center reference point,
// and their labels are deliberately excluded -- clicking them must never
// select/open an info box.
export const SELECTABLE_LAYER_IDS: readonly string[] = [AIRCRAFT_LAYER_ID, TRAIL_HIT_AREA_LAYER_ID];
