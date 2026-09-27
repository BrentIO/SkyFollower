// Screen-pixel gap between an aircraft icon's screen position and its info
// box's near (top-left) corner. Boxes are never nudged to avoid a
// collision -- this is the box's only position (see lib/labelStackOrder.ts
// for how overlapping boxes are stacked instead). A single fixed gap reads
// fine at a normal zoom level, but at a zoomed-out view the same
// screen-pixel gap is more likely to have another aircraft or basemap
// label sitting inside it, making a box read as detached. Scaling the gap
// down as the view zooms out keeps every box visually anchored to its icon.

// Gap at MAX_OFFSET_ZOOM and above -- the info box draws no leader line
// connecting box to icon, so proximity is the only anchoring cue.
export const MAX_INFO_BOX_OFFSET = 9;

// Gap at MIN_OFFSET_ZOOM and below.
export const MIN_INFO_BOX_OFFSET = 3;

// Zoom levels bounding the ramp between the two gaps above -- outside this
// range the offset is clamped rather than extrapolated.
const MAX_OFFSET_ZOOM = 10;
const MIN_OFFSET_ZOOM = 4;

// Maps the map's current zoom onto the info box's screen-pixel gap:
// MAX_INFO_BOX_OFFSET at MAX_OFFSET_ZOOM and above, ramping down to
// MIN_INFO_BOX_OFFSET at MIN_OFFSET_ZOOM and below. The ramp is a cubic
// ease-in (t^3), not linear, on the zoom fraction `t`: a linear ramp would
// leave a regional view at zoom 8-9 -- most of an operator's normal
// working range -- still near the max offset, so cubing keeps the offset
// close to the minimum through most of the range and reserves the
// full-size gap for genuinely close-in views.
export function infoBoxOffsetForZoom(zoom: number): number {
  if (zoom >= MAX_OFFSET_ZOOM) return MAX_INFO_BOX_OFFSET;
  if (zoom <= MIN_OFFSET_ZOOM) return MIN_INFO_BOX_OFFSET;
  const t = (zoom - MIN_OFFSET_ZOOM) / (MAX_OFFSET_ZOOM - MIN_OFFSET_ZOOM);
  return MIN_INFO_BOX_OFFSET + t ** 3 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET);
}
