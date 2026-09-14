// Screen-pixel gap between an aircraft icon's screen position and its info
// box's near (top-left) corner (see components/MapView.tsx's
// INFO_BOX_LAYER_ID symbol layer, and infoBoxTextOffsetZoomExpression()
// below for how this becomes a MapLibre `text-offset` value). Boxes are
// never nudged to avoid a collision -- this is the box's only position, not
// just a default (see lib/labelStackOrder.ts for how overlapping boxes are
// stacked instead). A single fixed gap reads fine at a normal zoom level,
// but at a zoomed-out view the same screen-pixel gap is much more likely to
// have another aircraft or a basemap place-name label sitting inside it,
// making a box read as detached from its icon. Scaling the gap down as the
// view zooms out keeps every box visually anchored to its icon without any
// collision-avoidance/nudging.

// Gap at MAX_OFFSET_ZOOM and above. Previously 34px -- confirmed live that
// even this closest-zoom maximum still read as detached from the icon,
// since the info box draws no leader line connecting box to icon and
// proximity is the only anchoring cue.
export const MAX_INFO_BOX_OFFSET = 9;

// Gap at MIN_OFFSET_ZOOM and below. Previously 12px.
export const MIN_INFO_BOX_OFFSET = 3;

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
// the range and saves the climb to the full-size gap for the last stretch
// right below MAX_OFFSET_ZOOM, so the full-size gap stays reserved for
// genuinely close-in views while zoom 7-9 gets a meaningfully smaller one
// (e.g. ~4.8px at zoom 8, ~6.5px at zoom 9).
export function infoBoxOffsetForZoom(zoom: number): number {
  if (zoom >= MAX_OFFSET_ZOOM) return MAX_INFO_BOX_OFFSET;
  if (zoom <= MIN_OFFSET_ZOOM) return MIN_INFO_BOX_OFFSET;
  const t = (zoom - MIN_OFFSET_ZOOM) / (MAX_OFFSET_ZOOM - MIN_OFFSET_ZOOM);
  return MIN_INFO_BOX_OFFSET + t ** 3 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET);
}

// --- MapLibre text-offset zoom expression (issue #1808) -------------------
//
// InfoBoxLayer.tsx (removed in #1808) called infoBoxOffsetForZoom() once
// per throttled "move" tick, in JS, to position each DOM box. Its
// replacement -- components/MapView.tsx's INFO_BOX_LAYER_ID symbol layer --
// has no per-frame JS callback to call it from; a MapLibre `text-offset`
// value is instead a *style expression*, evaluated on the GPU/style-engine
// side as the camera zoom changes, at zero per-frame JS cost. This builds
// that expression by sampling infoBoxOffsetForZoom() at a fixed set of zoom
// stops and feeding them to MapLibre's `interpolate`/`linear` -- enough
// stops to approximate the original cubic-ease ramp closely, without
// needing a cubic term in the style expression itself (MapLibre's
// `interpolate` only supports linear/exponential curves between stops, not
// an arbitrary power curve).
//
// The result is in ems (text-offset's own units, relative to the layer's
// text-size), not px -- INFO_BOX_TEXT_OFFSET_REFERENCE_PX is the text-size
// (px) the conversion is relative to (see MapView.tsx's INFO_BOX_LAYER_ID
// text-size), so the same on-screen pixel gap the DOM box used is
// preserved at that reference size. A future text-size change would need
// this reference updated to match, or the on-screen gap will drift
// slightly from what infoBoxOffsetForZoom() itself specifies.

export const INFO_BOX_TEXT_OFFSET_REFERENCE_PX = 10.5;

// Sample points across the ramp's actual range (MIN_OFFSET_ZOOM..
// MAX_OFFSET_ZOOM) plus the two flat tails immediately outside it --
// enough points that a linear interpolation between consecutive stops
// tracks the true cubic curve closely (worst-case deviation is small
// because t^3 is well-behaved/monotonic over this short an interval with
// this many stops).
const OFFSET_EXPRESSION_ZOOM_STOPS = [3, 4, 5, 6, 7, 7.5, 8, 8.5, 9, 9.5, 10, 11];

/**
 * A MapLibre `interpolate`/`linear`/`zoom` expression producing a
 * `[dx, dy]` (ems) `text-offset` value that ramps the same way
 * infoBoxOffsetForZoom() does, for INFO_BOX_LAYER_ID's `text-offset`
 * layout property (`text-anchor: "top-left"`, so both dx and dy are
 * positive -- a diagonal down-right offset from the aircraft's point,
 * matching InfoBoxLayer.tsx's removed `left: x + offset; top: y + offset`).
 * Returned as a plain array (not a maplibre-gl `ExpressionSpecification`)
 * so this file stays free of a MapLibre type dependency, same as the rest
 * of this module -- MapView.tsx passes it straight through as a style
 * expression, which is just JSON-shaped data.
 *
 * #1815: each stop's `[em, em]` output must be wrapped in `["literal",
 * [em, em]]` -- MapLibre's expression parser treats *every* nested array
 * as another sub-expression (first element = operator name) unless told
 * otherwise, so a bare `[em, em]` output failed style validation entirely
 * ("Expression name must be a string, but found number instead"). That
 * made `map.addLayer(INFO_BOX_LAYER_ID, ...)` fail MapLibre's own
 * validation and silently skip adding the layer (logged to console, not
 * thrown/caught by this app) -- no info box ever rendered, for any
 * aircraft, regardless of selection/hover/"Labels: All". See
 * infoBoxOffset.test.ts's own "is a valid MapLibre expression" test, which
 * exercises MapLibre's real expression parser and would have caught this
 * -- the removed test only checked the plain-array shape, never asked
 * MapLibre itself whether it was legal.
 */
export function infoBoxTextOffsetZoomExpression(referencePx: number = INFO_BOX_TEXT_OFFSET_REFERENCE_PX): unknown[] {
  const stops: unknown[] = [];
  for (const zoom of OFFSET_EXPRESSION_ZOOM_STOPS) {
    const em = infoBoxOffsetForZoom(zoom) / referencePx;
    stops.push(zoom, ["literal", [em, em]]);
  }
  return ["interpolate", ["linear"], ["zoom"], ...stops];
}
