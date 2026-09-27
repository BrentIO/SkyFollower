// Renders one vendored aircraft silhouette (aircraftShapes.generated.ts,
// from src/assets/aircraft-shapes/*.svg -- GPL-3.0) into an ImageData
// suitable for `map.addImage(id, imageData, { sdf: true })`.
//
// SDF (signed-distance-field): MapLibre's `icon-color` recolors it per
// feature and `icon-halo-*` draws the selection ring -- neither works on a
// plain raster icon. A flat single-color fill can't show a *second* color
// for a shape's rare surviving Accent detail layer, so it's composited
// depending on `shape.accentMode`: `"cutout"` strokes it with
// `destination-out` (detail inside the outline, e.g. BALL's gore lines);
// `"add"` strokes it with `source-over` in the same fill color (detail
// extending beyond the outline, e.g. EC35's rotor blades).
//
// Filling a Path2D only produces a *coverage mask* (binary alpha with one
// anti-aliased edge texel), not a true SDF -- handed that, icon-halo-*
// collapses to a hard ~1px offset instead of a smooth ring.
// `buildShapeIconImageData` fills the path, then runs `coverageToSdf` over
// the coverage alpha to produce a true 8-bit SDF. Encoding matches
// MapLibre's glyph convention (@mapbox/tiny-sdf): the edge sits at alpha
// 255 * (1 - SDF_CUTOFF) = ~191, and SDF_RADIUS_PX pixels of distance map
// to the full 0..255 range. The distance transform is the Felzenszwalb &
// Huttenlocher two-pass 1-D squared-distance algorithm, run over the
// "outside" and "inside" coverage and combined into a signed value.

import type { AircraftShape } from "./aircraftShapes.generated";

// SDF canvas edge, in pixels. The shape occupies the central SDF_SHAPE_PX,
// leaving a margin (13px each side) that must be >= SDF_RADIUS_PX so the
// outside distance can ramp fully to 0 before hitting the canvas edge.
export const SDF_CANVAS_PX = 96;
const SDF_SHAPE_PX = 70;

// Distance, in canvas pixels, over which the field ramps from the edge
// value to fully saturated. MapLibre's `icon-halo-width` is expressed in
// the same units -- a 3px halo needs at least 3px of field outside the edge.
export const SDF_RADIUS_PX = 8;

// Fraction of the field that lies outside the edge -- tiny-sdf's default,
// matching the threshold MapLibre's SDF shader uses for the fill.
export const SDF_CUTOFF = 0.25;

/** Alpha value the silhouette edge sits at in a `coverageToSdf` result. */
export const SDF_EDGE_ALPHA = Math.round(255 * (1 - SDF_CUTOFF));

/** MapLibre image id for a shape key (`AIRCRAFT_SHAPES` key). */
export function shapeIconId(shapeKey: string): string {
  return `sf-ac-${shapeKey}`;
}

const INF = 1e20;

// One pass of the Felzenszwalb & Huttenlocher 1-D squared Euclidean
// distance transform along a single row or column of `grid`. Overwrites
// those samples in place with the squared distance to the nearest
// zero-valued sample. `f`, `v`, `z` are caller-provided scratch buffers
// reused across calls to avoid per-line allocation. Verbatim structure
// from @mapbox/tiny-sdf (ISC/MIT).
function edt1d(
  grid: Float64Array,
  offset: number,
  stride: number,
  length: number,
  f: Float64Array,
  v: Int32Array,
  z: Float64Array,
): void {
  v[0] = 0;
  z[0] = -INF;
  z[1] = INF;
  f[0] = grid[offset];

  for (let q = 1, k = 0, s = 0; q < length; q++) {
    f[q] = grid[offset + q * stride];
    const q2 = q * q;
    do {
      const r = v[k];
      s = (f[q] - f[r] + q2 - r * r) / (q - r) / 2;
    } while (s <= z[k] && --k > -1);

    k++;
    v[k] = q;
    z[k] = s;
    z[k + 1] = INF;
  }

  for (let q = 0, k = 0; q < length; q++) {
    while (z[k + 1] < q) k++;
    const r = v[k];
    grid[offset + q * stride] = f[r] + (q - r) * (q - r);
  }
}

// Full 2-D squared distance transform: one `edt1d` pass down every column,
// then one across every row. On return, each cell of `grid` holds the
// squared Euclidean distance to the nearest originally-zero cell.
function edt2d(grid: Float64Array, width: number, height: number): void {
  const longest = Math.max(width, height);
  const f = new Float64Array(longest);
  const v = new Int32Array(longest);
  const z = new Float64Array(longest + 1);
  for (let x = 0; x < width; x++) edt1d(grid, x, width, height, f, v, z);
  for (let y = 0; y < height; y++) edt1d(grid, y * width, 1, width, f, v, z);
}

/**
 * Convert a coverage-mask alpha channel (0..255 per texel, ~binary with a
 * thin anti-aliased edge) into a true signed distance field with the same
 * layout, using the Felzenszwalb & Huttenlocher distance transform.
 *
 * Output convention matches MapLibre / @mapbox/tiny-sdf: the silhouette
 * edge sits at `SDF_EDGE_ALPHA` (~191), values rise toward 255 moving
 * `SDF_RADIUS_PX` into the shape and fall toward 0 moving `SDF_RADIUS_PX`
 * out of it, clamped beyond that.
 *
 * Pure and DOM-free so it is unit-testable without a canvas.
 */
export function coverageToSdf(
  alpha: Uint8ClampedArray | number[],
  width: number,
  height: number,
): Uint8ClampedArray {
  const size = width * height;
  // gridOuter: distance to the nearest covered texel (0 inside the shape,
  // growing outside). gridInner: the mirror -- distance to the nearest
  // empty texel (0 outside, growing inside).
  const gridOuter = new Float64Array(size);
  const gridInner = new Float64Array(size);

  for (let i = 0; i < size; i++) {
    const a = alpha[i] / 255;
    if (a === 1) {
      gridOuter[i] = 0;
      gridInner[i] = INF;
    } else if (a === 0) {
      gridOuter[i] = INF;
      gridInner[i] = 0;
    } else {
      // Partially covered edge texel: treat its coverage as a straight
      // edge cutting the texel and seed the sub-texel offset so the field
      // is smooth across the boundary rather than quantised to whole
      // texels.
      const outer = Math.max(0, 0.5 - a);
      const inner = Math.max(0, a - 0.5);
      gridOuter[i] = outer * outer;
      gridInner[i] = inner * inner;
    }
  }

  edt2d(gridOuter, width, height);
  edt2d(gridInner, width, height);

  const scale = 255 / SDF_RADIUS_PX;
  const base = 255 * (1 - SDF_CUTOFF);
  const out = new Uint8ClampedArray(size);
  for (let i = 0; i < size; i++) {
    // Signed distance: positive outside the silhouette, negative inside.
    const d = Math.sqrt(gridOuter[i]) - Math.sqrt(gridInner[i]);
    out[i] = Math.round(base - scale * d);
  }
  return out;
}

/** Fill one shape's silhouette to an ImageData, centred and scaled to a
 * uniform footprint, then replace the coverage alpha with a true SDF (see
 * the file header and `coverageToSdf`). Throws if a 2D canvas context
 * isn't available (jsdom -- callers in the test suite don't exercise this
 * path). */
export function buildShapeIconImageData(shape: AircraftShape): ImageData {
  const canvas = document.createElement("canvas");
  canvas.width = SDF_CANVAS_PX;
  canvas.height = SDF_CANVAS_PX;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("2D canvas context unavailable -- cannot build the aircraft icon.");
  }

  ctx.clearRect(0, 0, SDF_CANVAS_PX, SDF_CANVAS_PX);
  ctx.fillStyle = "#000000";

  const k = SDF_SHAPE_PX / shape.span;
  // Map source-unit space so the path's bbox centre lands on the canvas
  // centre and `span` source units span SDF_SHAPE_PX pixels.
  ctx.setTransform(k, 0, 0, k, SDF_CANVAS_PX / 2 - shape.cx * k, SDF_CANVAS_PX / 2 - shape.cy * k);
  ctx.fill(new Path2D(shape.d));

  if (shape.accentD) {
    // `ctx.lineWidth` is in the same (still-active) user-space coordinates
    // as the fill above, so the current `k` scale carries it to device
    // pixels the same way it carried the outline geometry -- setting it to
    // `shape.accentStrokeWidth` here is equivalent to an on-canvas width of
    // `shape.accentStrokeWidth * k`. Clamp that to >= 1 on-canvas pixel (by
    // flooring the user-space width at `1 / k`) so the stroke doesn't
    // antialias away before the later downscale to the SDF canvas.
    ctx.lineWidth = Math.max(shape.accentStrokeWidth ?? 0, 1 / k);
    if (shape.accentMode === "add") {
      // Draw the Accent path on top of the fill in the same solid color,
      // for detail that extends beyond the outline (e.g. EC35's rotor
      // blades) and would carve nothing visible as a cutout.
      ctx.strokeStyle = "#000000";
      ctx.stroke(new Path2D(shape.accentD));
    } else {
      // "cutout" (today's only other mode, used by BALL): erase a thin gap
      // along the Accent path through the fill just laid down.
      ctx.globalCompositeOperation = "destination-out";
      ctx.stroke(new Path2D(shape.accentD));
      ctx.globalCompositeOperation = "source-over";
    }
  }

  ctx.setTransform(1, 0, 0, 1, 0, 0);

  const imageData = ctx.getImageData(0, 0, SDF_CANVAS_PX, SDF_CANVAS_PX);
  const px = imageData.data;

  // Pull the coverage alpha out, distance-transform it, and write the SDF
  // back as the alpha channel. RGB stays 0 -- MapLibre's SDF path only
  // reads alpha, and `icon-color` supplies the actual fill colour.
  const coverage = new Uint8ClampedArray(SDF_CANVAS_PX * SDF_CANVAS_PX);
  for (let i = 0; i < coverage.length; i++) coverage[i] = px[i * 4 + 3];
  const sdf = coverageToSdf(coverage, SDF_CANVAS_PX, SDF_CANVAS_PX);
  for (let i = 0; i < sdf.length; i++) {
    px[i * 4] = 0;
    px[i * 4 + 1] = 0;
    px[i * 4 + 2] = 0;
    px[i * 4 + 3] = sdf[i];
  }

  return imageData;
}
