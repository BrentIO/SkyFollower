// Builds the aircraft symbol-layer icon: an original, custom-authored
// top-down dart/chevron aircraft silhouette (not an icon-library asset --
// Tabler/Lucide/Material Symbols were evaluated and rejected in the
// design phase; none had a top-down, rotation-suited glyph).
//
// Rendered as a single SDF (signed-distance-field) image so MapLibre's
// `icon-color`/`icon-halo-*` paint properties can recolor it per feature
// -- required for the altitude-based fill (continuous hsl() values, not a
// fixed small palette) and for the selection halo, without pre-rendering
// a distinct bitmap per color.
//
// Nose points north (0°) -- `icon-rotate` is bound to each aircraft's
// `heading` field (see components/MapView.tsx), so the map itself never
// rotates (locked north-up). Selection is shown via `icon-halo-*`, never
// by recoloring the icon fill itself.

export const AIRCRAFT_ICON_ID = "sf-aircraft-icon";
export const AIRCRAFT_ICON_SIZE = 64;

// Dart/chevron outline in a 64x64 box: nose at top, two swept wingtips,
// and a single concave notch pulled forward between them -- the classic
// 4-point "dart" silhouette. Deliberately just 4 points (a more literal
// fuselage+wings+tail outline was tried first and turned out to
// self-intersect once filled -- a non-simple polygon fills with spiky,
// star-shaped artifacts under canvas's nonzero winding rule). This shape
// is simple (its boundary never crosses itself) by construction: nose ->
// right wingtip -> notch -> left wingtip traces the outline in one
// consistent direction. Reads clearly as a top-down aircraft/direction
// indicator at any rotation and at small sizes.
function aircraftPath(ctx: CanvasRenderingContext2D): void {
  ctx.beginPath();
  ctx.moveTo(32, 6); // nose
  ctx.lineTo(56, 50); // right wingtip
  ctx.lineTo(32, 40); // tail notch (pulled forward, concave)
  ctx.lineTo(8, 50); // left wingtip
  ctx.closePath();
}

// Renders the silhouette to ImageData suitable for
// `map.addImage(AIRCRAFT_ICON_ID, imageData, { sdf: true })`. Split out
// from any MapLibre call so it's independently testable/inspectable, and
// so MapView.tsx only needs to call it once (`map.addImage` is a one-time
// setup call, not per-render).
export function buildAircraftIconImageData(): ImageData {
  const size = AIRCRAFT_ICON_SIZE;
  const canvas = document.createElement("canvas");
  canvas.width = size;
  canvas.height = size;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("2D canvas context unavailable -- cannot build the aircraft icon.");
  }
  ctx.clearRect(0, 0, size, size);
  ctx.fillStyle = "#000000";
  aircraftPath(ctx);
  ctx.fill();
  return ctx.getImageData(0, 0, size, size);
}
