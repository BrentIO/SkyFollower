// Generates and registers the rounded-rectangle "chat bubble" background
// image behind each info-box label (see components/MapView.tsx's
// INFO_BOX_LAYER_ID symbol layer, and lib/infoBoxSource.ts for the feature
// data it's paired with). Drawn once, offscreen, at map load -- not
// per-aircraft -- and registered as a MapLibre *stretchable* image
// (map.addImage's content/stretchX/stretchY options, MapLibre's "9-patch"
// support purpose-built for exactly this "background behind
// variable-length text" case): the layer's `icon-text-fit: 'both'` resizes
// this one shared image per feature to fit that feature's own rendered
// text bounds (ident/altitude/registration lines vary in width per
// aircraft), stretching only the marked interior region so the rounded
// corners are never distorted.
//
// See issue #1808's follow-up comment for the design decision this
// implements: "background icon" was picked over a halo-only or
// halo+scrim treatment as the closest visual match to the previous
// DOM-based `bg-black/40 rounded` box (InfoBoxLayer.tsx, now removed),
// at the cost of being the most implementation work of the three options
// compared.

import type * as maplibregl from "maplibre-gl";

export const INFO_BOX_ICON_ID = "sf-info-box-bg";

// Base bitmap size (px). Arbitrary relative to the final on-map size --
// icon-text-fit rescales the whole image to match each feature's own text
// -- only the *proportions* below (corner radius/inset vs. canvas size)
// affect how round the corners look once stretched.
const CANVAS_PX = 32;
const CORNER_RADIUS_PX = 8;
// Fixed-size margin on each edge that stretchX/stretchY exclude, so the
// rounded corners are never distorted by the interior stretch -- same
// "avoid the border" pattern as map.addImage's own documented example.
const CORNER_INSET_PX = 10;

/** MapLibre stretchable-image metadata for INFO_BOX_ICON_ID -- passed as
 * `map.addImage`'s third argument alongside the bitmap from
 * `buildInfoBoxIconImageData()`. Exported separately (rather than folded
 * into `registerInfoBoxIcon`) so a plain unit test can assert on the
 * actual stretch/content geometry without needing a canvas. */
export const INFO_BOX_ICON_STRETCH_X: Array<[number, number]> = [[CORNER_INSET_PX, CANVAS_PX - CORNER_INSET_PX]];
export const INFO_BOX_ICON_STRETCH_Y: Array<[number, number]> = [[CORNER_INSET_PX, CANVAS_PX - CORNER_INSET_PX]];
export const INFO_BOX_ICON_CONTENT: [number, number, number, number] = [
  CORNER_INSET_PX,
  CORNER_INSET_PX,
  CANVAS_PX - CORNER_INSET_PX,
  CANVAS_PX - CORNER_INSET_PX,
];

// Matches the previous DOM box's `bg-black/40` (Tailwind's black at 40%
// alpha) as closely as a flat fill can -- an exact pixel-level match isn't
// verifiable without a live render (see this PR's description).
const FILL_STYLE = "rgba(0, 0, 0, 0.4)";

function roundedRectPath(
  ctx: CanvasRenderingContext2D,
  x: number,
  y: number,
  w: number,
  h: number,
  r: number,
): void {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.lineTo(x + w - r, y);
  ctx.arcTo(x + w, y, x + w, y + r, r);
  ctx.lineTo(x + w, y + h - r);
  ctx.arcTo(x + w, y + h, x + w - r, y + h, r);
  ctx.lineTo(x + r, y + h);
  ctx.arcTo(x, y + h, x, y + h - r, r);
  ctx.lineTo(x, y + r);
  ctx.arcTo(x, y, x + r, y, r);
  ctx.closePath();
}

/** Draws the rounded-rectangle bitmap to an ImageData, for
 * `map.addImage(INFO_BOX_ICON_ID, ..., { content: INFO_BOX_ICON_CONTENT,
 * stretchX: INFO_BOX_ICON_STRETCH_X, stretchY: INFO_BOX_ICON_STRETCH_Y })`.
 * Throws if a 2D canvas context isn't available (jsdom -- callers in the
 * test suite don't exercise this path; same convention as
 * aircraftIcon.ts's buildShapeIconImageData). */
export function buildInfoBoxIconImageData(): ImageData {
  const canvas = document.createElement("canvas");
  canvas.width = CANVAS_PX;
  canvas.height = CANVAS_PX;
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    throw new Error("2D canvas context unavailable -- cannot build the info box background icon.");
  }

  ctx.clearRect(0, 0, CANVAS_PX, CANVAS_PX);
  ctx.fillStyle = FILL_STYLE;
  roundedRectPath(ctx, 0, 0, CANVAS_PX, CANVAS_PX, CORNER_RADIUS_PX);
  ctx.fill();

  return ctx.getImageData(0, 0, CANVAS_PX, CANVAS_PX);
}

/** Registers INFO_BOX_ICON_ID with the map, once -- a no-op if it's
 * already registered. Call before any INFO_BOX_LAYER_ID feature might
 * reference it (i.e. before the source can carry data), same ordering
 * rule as registerShapeImage in MapView.tsx. A canvas failure is
 * swallowed -- the layer's `icon-image` then just resolves to nothing
 * rather than crashing the map (the text would still render, just without
 * its background). */
export function registerInfoBoxIcon(map: maplibregl.Map): void {
  if (map.hasImage(INFO_BOX_ICON_ID)) return;
  try {
    map.addImage(INFO_BOX_ICON_ID, buildInfoBoxIconImageData(), {
      content: INFO_BOX_ICON_CONTENT,
      stretchX: INFO_BOX_ICON_STRETCH_X,
      stretchY: INFO_BOX_ICON_STRETCH_Y,
    });
  } catch (err) {
    console.warn("Could not build info box background icon:", err);
  }
}
