// Screen-pixel gap between an aircraft icon's screen position and its info
// box's near (top-left) corner (see components/InfoBoxLayer.tsx). Boxes are
// never nudged to avoid a collision -- this is the box's only position, not
// just a default (see lib/labelStackOrder.ts for how overlapping boxes are
// stacked instead). A single fixed gap reads fine at a normal zoom level,
// but at a zoomed-out view the same screen-pixel gap is much more likely to
// have another aircraft or a basemap place-name label sitting inside it,
// making a box read as detached from its icon. Scaling the gap down as the
// view zooms out keeps every box visually anchored to its icon without any
// collision-avoidance/nudging.

// Gap at MAX_OFFSET_ZOOM and above -- unchanged from the original fixed value.
export const MAX_INFO_BOX_OFFSET = 34;

// Gap at MIN_OFFSET_ZOOM and below.
export const MIN_INFO_BOX_OFFSET = 12;

// Zoom levels bounding the ramp between the two gaps above -- outside this
// range the offset is clamped rather than extrapolated.
const MAX_OFFSET_ZOOM = 10;
const MIN_OFFSET_ZOOM = 4;

// Maps the map's current zoom onto the info box's screen-pixel gap from its
// aircraft icon: MAX_INFO_BOX_OFFSET at MAX_OFFSET_ZOOM and above, ramping
// down to MIN_INFO_BOX_OFFSET at MIN_OFFSET_ZOOM and below.
//
// The ramp is a cubic ease-in (t^3), not linear, on the zoom fraction `t`
// between the two thresholds. A linear ramp spends its "budget" evenly
// across the whole 4-10 span, so a regional view at zoom 8-9 -- most of an
// operator's normal working range -- still sat at ~80-90% of the max offset
// (only a 12-20% reduction), leaving labels visibly detached from their
// icon. Cubing `t` keeps the offset close to the minimum through most of
// the range and saves the climb to the full 34px gap for the last stretch
// right below MAX_OFFSET_ZOOM, so the full-size gap stays reserved for
// genuinely close-in views while zoom 7-9 gets a meaningfully smaller one
// (e.g. ~18.5px at zoom 8, ~24.7px at zoom 9, down from ~27px/~30px).
export function infoBoxOffsetForZoom(zoom: number): number {
  if (zoom >= MAX_OFFSET_ZOOM) return MAX_INFO_BOX_OFFSET;
  if (zoom <= MIN_OFFSET_ZOOM) return MIN_INFO_BOX_OFFSET;
  const t = (zoom - MIN_OFFSET_ZOOM) / (MAX_OFFSET_ZOOM - MIN_OFFSET_ZOOM);
  return MIN_INFO_BOX_OFFSET + t ** 3 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET);
}
