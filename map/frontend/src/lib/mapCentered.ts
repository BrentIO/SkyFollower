// Pure pixel-distance comparison for "is the map's camera currently
// centered on the configured center point" -- components/ControlsPanel.tsx's
// Center button, driven by components/MapView.tsx's `load`/`moveend`
// listeners. Split out so the centered/not-centered threshold is a plain
// unit-tested value instead of only exercisable through a real MapLibre map.
//
// Pixel distance via map.project(), not a fixed lat/lon epsilon: comparing
// screen-pixel distance scales naturally with zoom, matching what a user
// actually perceives, unlike a fixed real-world-distance threshold
// (imperceptible at low zoom, obviously off at high zoom) or a fixed
// lat/lon epsilon (the opposite problem). This never reads `zoom` itself --
// `handleRecenter` only ever changes `center`, so "centered" means the
// same pure position match at any zoom level.

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
