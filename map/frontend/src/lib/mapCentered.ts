// Pure pixel-distance comparison for "is the map's camera currently
// centered on the configured center point" -- components/ControlsPanel.tsx's
// Center button (#1847), driven by components/MapView.tsx's `load`/
// `moveend` listeners. Split out so the centered/not-centered threshold is
// a plain unit-tested value instead of only being exercisable through a
// real MapLibre map (this project has no jsdom/component-render test
// setup -- see lib/config.test.ts's own note).
//
// Pixel distance via map.project(), not a fixed lat/lon epsilon -- see the
// issue this implements. Projecting both the camera's current center and
// the configured center through the map's current transform and comparing
// screen-pixel distance scales naturally with zoom: the same real-world
// offset reads as "still centered" at a wide-out zoom (a few px on screen)
// and "off-center" at a tight one (many px on screen), matching what a
// user actually perceives -- the crosshair icon visually still sitting on
// the center marker -- unlike a fixed real-world-distance threshold
// (imperceptible at low zoom, obviously off at high zoom) or a fixed
// lat/lon epsilon (the opposite problem, and not how a Mercator-projected
// pixel distance actually relates to a constant lat/lon delta across
// latitudes anyway). This comparison is deliberately zoom-*independent* in
// the sense that it never reads `zoom` itself -- `handleRecenter` only ever
// changes `center`, never `zoom`, so "centered" means the same thing (a
// pure position match) at any zoom level; the pixel-distance approach above
// is just how that position match is expressed on screen.

export interface ScreenPoint {
  x: number;
  y: number;
}

// A few screen pixels -- generous enough to absorb float/projection
// rounding (map.project() round-trips through Mercator projection plus the
// current transform matrix) without ever reading "centered" for a pan a
// user could actually perceive.
export const CENTER_TOLERANCE_PX = 3;

/**
 * True when `current` (the camera's current center, projected to screen
 * pixels) is within `tolerancePx` pixels of `target` (the configured
 * center, projected the same way). Both points must be projected through
 * the same map transform (zoom/pitch/bearing/size) for the comparison to
 * be meaningful -- see MapView.tsx's `updateIsCentered`.
 */
export function isWithinCenterTolerance(
  current: ScreenPoint,
  target: ScreenPoint,
  tolerancePx: number = CENTER_TOLERANCE_PX,
): boolean {
  const dx = current.x - target.x;
  const dy = current.y - target.y;
  return dx * dx + dy * dy <= tolerancePx * tolerancePx;
}
