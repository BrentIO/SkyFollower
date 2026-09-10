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

import type { AircraftShape } from "./aircraftShapes.generated";

// SDF canvas edge, in pixels. Larger than the old 64 for finer silhouette
// detail; the shape occupies the central SDF_SHAPE_PX, leaving a margin
// the distance field / halo needs.
export const SDF_CANVAS_PX = 96;
const SDF_SHAPE_PX = 70;

/** MapLibre image id for a shape key (`AIRCRAFT_SHAPES` key). */
export function shapeIconId(shapeKey: string): string {
  return `sf-ac-${shapeKey}`;
}

/** Fill one shape's silhouette to an ImageData, centred and scaled to a
 * uniform footprint. Throws if a 2D canvas context isn't available (jsdom
 * -- callers in the test suite don't exercise this path). */
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

  return ctx.getImageData(0, 0, SDF_CANVAS_PX, SDF_CANVAS_PX);
}
