#!/usr/bin/env node
// Extracts a compact, runtime-friendly form of every vendored aircraft
// silhouette (src/assets/aircraft-shapes/*.svg, GPL-3.0 -- see that
// directory's LICENSE and the repo's THIRD-PARTY-NOTICES.md) into a single
// generated module, src/lib/aircraftShapes.generated.ts.
//
// The vendored files are full Inkscape SVGs. We only need, per shape:
//   - the outer silhouette path's `d` string (always the first <path> in
//     document order; the second, when present, is an "Accent" detail
//     layer -- thin/cosmetic and dropped for nearly every shape, but for a
//     rare outlier it carries real identifying detail worth keeping as a
//     cutout through the solid fill, see ACCENT_CUTOUT_RATIO_THRESHOLD),
//   - the geometry bounding box, so the runtime can centre the path on the
//     icon canvas (rotation pivot) and scale every shape to a uniform pixel
//     footprint in the SDF image,
//   - a relative on-map size multiplier derived from the real drawn size
//     (the shapes are drawn to a consistent real-world scale inside an
//     80x80-unit viewBox), clamped so a Cessna isn't a dot and an An-225
//     isn't a blob.
//
// Run automatically by the `predev` / `prebuild` npm scripts; the output is
// gitignored so the vendored SVGs stay the single source of truth.

import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { svgPathBbox } from "svg-path-bbox";
import svgpath from "svgpath";

// Source coordinates carry 6+ decimal places in an 80-unit space; at a
// <100px icon that precision is invisible. Rounding to 0.1 units roughly
// halves the generated module.
const COORD_PRECISION = 1;

// Icons are flat single-color SDF fills, so an Accent path can only be
// rendered as a cutout (a thin transparent gap punched through the solid
// fill) -- appropriate for a shape whose Accent layer draws real
// identifying detail, but wrong for the common case where it's a faint
// cosmetic highlight that would just look like a fill defect.
//
// We tell the two apart by the Accent/outline path-length ratio. Measured
// against the current vendored set (every shape with 2+ paths, n=164):
// BALL (balloon gore lines + basket) is a sole extreme outlier at ~11.4;
// every other shape is at or below ~2.5 (median ~0.36). A threshold of 5
// sits cleanly in the ~2.5-11.4 gap, so it isolates today's outlier(s)
// without catching any shape whose Accent layer is merely cosmetic. If a
// future vendored SVG lands with a similarly detail-bearing Accent layer,
// it will cross this threshold and pick up a cutout automatically.
const ACCENT_CUTOUT_RATIO_THRESHOLD = 5;

// Fallback stroke width (source units, in the SVG's 80x80-unit space) for
// an Accent path whose `style` has no parseable `stroke-width` -- shouldn't
// happen for the vendored set, but keeps generation from throwing on a
// future SVG with a differently-authored style attribute.
const ACCENT_STROKE_WIDTH_FALLBACK = 0.3;

const HERE = dirname(fileURLToPath(import.meta.url));
const SHAPES_DIR = join(HERE, "..", "src", "assets", "aircraft-shapes");
const OUT_FILE = join(HERE, "..", "src", "lib", "aircraftShapes.generated.ts");

// Approximates an SVG path's total drawn length by flattening every curve
// into short line segments and summing segment lengths -- accurate enough
// to compare an Accent path against its outline (see
// ACCENT_CUTOUT_RATIO_THRESHOLD above), without pulling in a dedicated
// path-length library for a build-time script.
function pathLength(d) {
  const path = svgpath(d).abs().unarc().unshort();
  let length = 0;
  path.iterate((seg, _index, x0, y0) => {
    const cmd = seg[0];
    switch (cmd) {
      case "M":
      case "Z":
        return;
      case "L":
        length += Math.hypot(seg[1] - x0, seg[2] - y0);
        return;
      case "H":
        length += Math.abs(seg[1] - x0);
        return;
      case "V":
        length += Math.abs(seg[1] - y0);
        return;
      case "C":
        length += sampleCubicBezierLength(x0, y0, seg[1], seg[2], seg[3], seg[4], seg[5], seg[6]);
        return;
      case "Q": {
        // Elevate the quadratic control point to an equivalent cubic so
        // one sampler covers both curve types.
        const cx1 = x0 + (2 / 3) * (seg[1] - x0);
        const cy1 = y0 + (2 / 3) * (seg[2] - y0);
        const cx2 = seg[3] + (2 / 3) * (seg[1] - seg[3]);
        const cy2 = seg[4] + (2 / 3) * (seg[2] - seg[4]);
        length += sampleCubicBezierLength(x0, y0, cx1, cy1, cx2, cy2, seg[3], seg[4]);
        return;
      }
      default:
        throw new Error(`pathLength: unhandled segment command ${cmd}`);
    }
  });
  return length;
}

function sampleCubicBezierLength(x0, y0, x1, y1, x2, y2, x3, y3, steps = 24) {
  let length = 0;
  let px = x0;
  let py = y0;
  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    const mt = 1 - t;
    const x = mt * mt * mt * x0 + 3 * mt * mt * t * x1 + 3 * mt * t * t * x2 + t * t * t * x3;
    const y = mt * mt * mt * y0 + 3 * mt * mt * t * y1 + 3 * mt * t * t * y2 + t * t * t * y3;
    length += Math.hypot(x - px, y - py);
    px = x;
    py = y;
  }
  return length;
}

function parseStrokeWidth(style) {
  const match = style?.match(/stroke-width:\s*([0-9.]+)/);
  if (!match) {
    return undefined;
  }
  const n = Number.parseFloat(match[1]);
  return Number.isFinite(n) ? n : undefined;
}

// Filename stem -> shape key: strip the extension, uppercase, drop the
// hyphen we substituted for the space in the four "<TYPE> fast/slow" swing-
// wing variants ("B1-fast.svg" -> "B1FAST"). Keeps the ICAO type
// designators (the majority) as-is: "A320.svg" -> "A320".
function shapeKey(filename) {
  return filename.replace(/\.svg$/i, "").replace(/-/g, "").toUpperCase();
}

const files = readdirSync(SHAPES_DIR).filter((f) => f.endsWith(".svg")).sort();
if (files.length === 0) {
  throw new Error(`No SVGs found in ${SHAPES_DIR}`);
}

const shapes = {};
const spans = [];

for (const file of files) {
  const svg = readFileSync(join(SHAPES_DIR, file), "utf8");
  // Every vendored <path> is self-closing; grabbing the full tags (rather
  // than just the first `d="..."`) lets us also reach the second path's
  // `d` and `style` for the Accent-cutout check below.
  const pathTags = [...svg.matchAll(/<path\b[^>]*\/>/gs)].map((m) => m[0]);
  const rawD = pathTags[0]?.match(/\bd="([^"]+)"/)?.[1];
  if (!rawD) {
    throw new Error(`${file}: no <path d="..."> found`);
  }
  let d;
  try {
    d = svgpath(rawD).round(COORD_PRECISION).toString();
  } catch (err) {
    throw new Error(`${file}: could not parse path: ${err.message}`);
  }
  let bbox;
  try {
    bbox = svgPathBbox(d);
  } catch (err) {
    throw new Error(`${file}: could not compute path bbox: ${err.message}`);
  }
  const [x0, y0, x1, y1] = bbox;
  const w = x1 - x0;
  const h = y1 - y0;
  const span = Math.max(w, h);
  if (!(span > 0) || !Number.isFinite(span)) {
    throw new Error(`${file}: degenerate bbox ${JSON.stringify(bbox)}`);
  }
  spans.push(span);

  // Accent cutout: only the second <path> is ever considered (the outer
  // silhouette is always the first), and only when its path-length ratio
  // against the outline crosses ACCENT_CUTOUT_RATIO_THRESHOLD -- see that
  // constant's comment. Same coordinate space as the outline, so no
  // bbox-centering is needed; rounded/transformed identically.
  let accentD;
  let accentStrokeWidth;
  const rawAccentD = pathTags[1]?.match(/\bd="([^"]+)"/)?.[1];
  if (rawAccentD) {
    let ratio;
    try {
      const outlineLength = pathLength(rawD);
      const accentLength = pathLength(rawAccentD);
      ratio = outlineLength > 0 ? accentLength / outlineLength : 0;
    } catch (err) {
      throw new Error(`${file}: could not compute accent/outline path-length ratio: ${err.message}`);
    }
    if (ratio > ACCENT_CUTOUT_RATIO_THRESHOLD) {
      try {
        accentD = svgpath(rawAccentD).round(COORD_PRECISION).toString().trim();
      } catch (err) {
        throw new Error(`${file}: could not parse accent path: ${err.message}`);
      }
      const accentStyle = pathTags[1]?.match(/\bstyle="([^"]+)"/)?.[1];
      accentStrokeWidth = round(parseStrokeWidth(accentStyle) ?? ACCENT_STROKE_WIDTH_FALLBACK);
    }
  }

  shapes[shapeKey(file)] = {
    d: d.trim(),
    // bbox centre -- the runtime translates the path here, then to the
    // canvas centre, so `icon-rotate` spins it about its own middle.
    cx: round(x0 + w / 2),
    cy: round(y0 + h / 2),
    span: round(span),
    ...(accentD ? { accentD, accentStrokeWidth } : {}),
  };
}

// Shapes with a compact and/or simple silhouette -- no airplane's
// elongated wing/tail cross-shape to fill the frame -- that antialias down
// into an unrecognizable blob at the general SCALE_MIN floor rather than
// reading as their real shape. These get their own higher floor below
// instead of raising SCALE_MIN globally, which would wrongly inflate other
// shapes that are correctly small.
const COMPACT_SILHOUETTE_KEYS = new Set([
  // Helicopters -- rotorcraft silhouette, no wingspan to fill the frame.
  // Keys taken from aircraftIconResolver.ts's TYPE_ALIASES "Helicopters"
  // section, plus CATEGORY_SHAPES.A7 / DESCRIPTION_SHAPES.H (both map to
  // "H60", already listed here).
  "EC20", "EC35", "EC45", "GAZL", "AS65", "AS32", "S61", "R44", "H60",
  "NH90", "LYNX", "MI24", "H47",
  // Balloon -- round envelope + basket, no wing/tail shape at all.
  "BALL",
  // Gyroplane -- short stub wings dwarfed by the rotor disc outline.
  "GYRO",
  // Glider/sailplane -- long wing but a very narrow fuselage, so the
  // overall silhouette is delicate rather than filling its footprint.
  "AS21",
  // UAV -- small, compact airframe by design.
  "Q4",
  // Motor glider -- same delicate, wing-dominated silhouette issue as AS21.
  "SF25",
]);
for (const key of COMPACT_SILHOUETTE_KEYS) {
  if (!shapes[key]) {
    throw new Error(`COMPACT_SILHOUETTE_KEYS references unknown shape key ${JSON.stringify(key)}`);
  }
}

// A shape's on-map size multiplier: its drawn span relative to the median,
// through a square-root curve so the spread from a Cessna to an An-225
// stays legible rather than the extremes dominating, then clamped. Real
// size still reads (a light single is visibly smaller than a widebody)
// without a fighter becoming a speck or an A380 a blob.
const sorted = [...spans].sort((a, b) => a - b);
const referenceSpan = sorted[Math.floor(sorted.length / 2)];
const SCALE_MIN = 0.6;
const SCALE_MIN_COMPACT_SILHOUETTE = 1.0;
const SCALE_MAX = 1.6;
for (const key of Object.keys(shapes)) {
  const raw = Math.sqrt(shapes[key].span / referenceSpan);
  const floor = COMPACT_SILHOUETTE_KEYS.has(key) ? SCALE_MIN_COMPACT_SILHOUETTE : SCALE_MIN;
  shapes[key].scale = round(Math.min(SCALE_MAX, Math.max(floor, raw)));
}

function round(n) {
  return Math.round(n * 1000) / 1000;
}

const orderedKeys = Object.keys(shapes).sort();
const body = orderedKeys
  .map((k) => {
    const s = shapes[k];
    const accentFields =
      s.accentD !== undefined
        ? `, accentD: ${JSON.stringify(s.accentD)}, accentStrokeWidth: ${s.accentStrokeWidth}`
        : "";
    return `  ${JSON.stringify(k)}: { d: ${JSON.stringify(s.d)}, cx: ${s.cx}, cy: ${s.cy}, span: ${s.span}, scale: ${s.scale}${accentFields} },`;
  })
  .join("\n");

const out = `// GENERATED by scripts/generate-aircraft-shapes.mjs -- do not edit.
// Source of truth: src/assets/aircraft-shapes/*.svg (GPL-3.0; see that
// directory's LICENSE and the repo's THIRD-PARTY-NOTICES.md).
// Regenerated on every \`npm run dev\` / \`npm run build\` (predev/prebuild).

export interface AircraftShape {
  /** Outer silhouette path 'd', in the source SVG's 80x80-unit space. */
  d: string;
  /** Path bounding-box centre (translate here, then to the icon centre). */
  cx: number;
  cy: number;
  /** max(bbox width, bbox height), in source units. */
  span: number;
  /**
   * On-map size multiplier vs. the median shape, clamped to
   * [${SCALE_MIN}, ${SCALE_MAX}], except compact/simple-silhouette shapes
   * (see COMPACT_SILHOUETTE_KEYS in generate-aircraft-shapes.mjs), which get
   * a higher [${SCALE_MIN_COMPACT_SILHOUETTE}, ${SCALE_MAX}] floor.
   */
  scale: number;
  /**
   * Accent (2nd-layer) path 'd', same coordinate space as \`d\` -- present
   * only when the shape's Accent/outline path-length ratio crosses
   * ACCENT_CUTOUT_RATIO_THRESHOLD in generate-aircraft-shapes.mjs, i.e. the
   * Accent layer carries real identifying detail (e.g. balloon gore lines)
   * rather than a cosmetic highlight. Rendered as a cutout through the
   * solid fill, not a second fill.
   */
  accentD?: string;
  /** Accent path's own stroke width, in source units; only set alongside \`accentD\`. */
  accentStrokeWidth?: number;
}

export const AIRCRAFT_SHAPES: Record<string, AircraftShape> = {
${body}
};

export type AircraftShapeKey = keyof typeof AIRCRAFT_SHAPES;
`;

writeFileSync(OUT_FILE, out);
console.log(
  `aircraft-shapes: generated ${orderedKeys.length} shapes -> ${OUT_FILE.replace(join(HERE, ".."), ".")} ` +
    `(reference span ${round(referenceSpan)})`,
);
