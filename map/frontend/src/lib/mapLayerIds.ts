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

// The only layer(s) MapView.tsx's click handler queries for aircraft
// selection. Range rings and their labels are deliberately excluded --
// clicking them must never select/open an info box (see #1587).
export const SELECTABLE_LAYER_IDS: readonly string[] = [AIRCRAFT_LAYER_ID, TRAIL_HIT_AREA_LAYER_ID];
