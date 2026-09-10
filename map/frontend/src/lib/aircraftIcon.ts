// Renders one vendored aircraft silhouette (aircraftShapes.generated.ts,
// from src/assets/aircraft-shapes/*.svg -- GPL-3.0) into an ImageData
// suitable for `map.addImage(id, imageData, { sdf: true })`.
//
// SDF (signed-distance-field), same as the icon this replaced: MapLibre's
// `icon-color` recolors it per feature (the continuous altitude hsl(), not
// a fixed palette) and `icon-halo-*` draws the selection ring -- neither
// works on a plain raster icon. Each shape is filled solid to a canvas
// (the source path is a closed outline; its thin "Accent" detail layer is
// dropped at generation time) and every shape is scaled to the same pixel
// footprint here, so the SDF resolution is uniform; real relative size is
// applied on the map via `icon-size` and each shape's `scale`.
//
// Source paths are drawn nose-up (north), matching `icon-rotate` bound to
// heading -- no rotation offset.
//
// Why a distance transform and not just the filled path: MapLibre's
// `sdf: true` interprets the image's alpha channel as a *signed distance
// field* -- for every texel, the distance to the nearest silhouette edge,
// encoded so the edge sits at a fixed alpha and the field ramps linearly
// on either side. Filling a Path2D only produces a *coverage mask* (alpha
// ~255 inside, ~0 outside, one anti-aliased texel between). Handed that,
// `icon-color` still works but `icon-halo-width`/`-blur` have almost no
// field to grow into, so the selection ring collapses to a hard ~1px
// offset, and scaled-up thin features (a fighter's tail, wingtip pods)
// soften more than a real SDF would. `buildShapeIconImageData` therefore
// fills the path, then runs `coverageToSdf` over the coverage alpha to
// produce a true 8-bit SDF before returning.
//
// SDF encoding matches MapLibre's glyph convention (as produced by
// @mapbox/tiny-sdf, which MapLibre uses for text): the edge is at
// alpha 255 * (1 - SDF_CUTOFF) = ~191, and SDF_RADIUS_PX pixels of
// distance map to the full 0..255 range (so ~32 alpha units per pixel).
// The distance transform is the Felzenszwalb & Huttenlocher two-pass
// 1-D squared-distance algorithm, run once over the "outside" coverage
// and once over the "inside" coverage and combined into a signed value.

import type { AircraftShape } from "./aircraftShapes.generated";

// SDF canvas edge, in pixels. Larger than the old 64 for finer silhouette
// detail; the shape occupies the central SDF_SHAPE_PX, leaving a margin
// the distance field / halo needs. The margin (13 px each side) must be
// >= SDF_RADIUS_PX so the outside distance can ramp fully to 0 before it
// hits the canvas edge.
export const SDF_CANVAS_PX = 96;
const SDF_SHAPE_PX = 70;

// Distance, in canvas pixels, over which the field ramps from the edge
// value to fully saturated (0 outside / 255 inside). MapLibre reads SDF
// icons at a nominal 8 px field, and its `icon-halo-width` is expressed
// in the same units -- a 3 px halo needs at least 3 px of field outside
// the edge to render.
export const SDF_RADIUS_PX = 8;

// Fraction of the field that lies outside the edge. tiny-sdf's default;
// with it the edge lands at alpha 255 * (1 - 0.25) = ~191, matching the
// `(256 - 64) / 256` threshold MapLibre's SDF shader uses for the fill.
export const SDF_CUTOFF = 0.25;

/** Alpha value the silhouette edge sits at in a `coverageToSdf` result. */
export const SDF_EDGE_ALPHA = Math.round(255 * (1 - SDF_CUTOFF));

/** MapLibre image id for a shape key (`AIRCRAFT_SHAPES` key). */
export function shapeIconId(shapeKey: string): string {
  return `sf-ac-${shapeKey}`;
}

const INF = 1e20;

// One pass of the Felzenszwalb & Huttenlocher 1-D squared Euclidean
// distance transform along a single row or column of `grid` (stepping by
// `stride`, `length` samples starting at `offset`). Overwrites those
// samples in place with the squared distance to the nearest zero-valued
// sample. `f`, `v`, `z` are caller-provided scratch buffers sized to the
// longest line, reused across calls to avoid per-line allocation.
// Verbatim structure from @mapbox/tiny-sdf (ISC/MIT); this is the
// canonical lower-envelope form.
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
  // gridOuter: 0 at fully-covered texels, INF at fully-empty ones -> its
  // transform is the distance from each texel to the nearest covered one
  // (0 inside the shape, growing outside).
  // gridInner: the mirror -- distance to the nearest empty texel (0
  // outside, growing inside).
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
