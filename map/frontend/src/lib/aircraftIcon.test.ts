import { describe, expect, it } from "vitest";
import {
  coverageToSdf,
  SDF_CUTOFF,
  SDF_EDGE_ALPHA,
  SDF_RADIUS_PX,
  shapeIconId,
} from "./aircraftIcon";

// `buildShapeIconImageData` itself needs a 2D canvas, which this project's
// "node" vitest environment doesn't provide -- so the distance transform
// is factored out as `coverageToSdf` and exercised here directly with a
// synthetic coverage mask. A filled axis-aligned rectangle has a known
// signed distance everywhere, so every ramp direction can be checked by
// hand.

const W = 32;
const H = 32;
const R0 = 8; // rectangle spans [8, 23] on both axes -> symmetric in a 32-wide grid
const R1 = 23;

const idx = (x: number, y: number) => y * W + x;

function filledRectCoverage(): Uint8ClampedArray {
  const a = new Uint8ClampedArray(W * H);
  for (let y = R0; y <= R1; y++) {
    for (let x = R0; x <= R1; x++) a[idx(x, y)] = 255;
  }
  return a;
}

describe("coverageToSdf", () => {
  const sdf = coverageToSdf(filledRectCoverage(), W, H);

  it("returns a Uint8ClampedArray of width*height", () => {
    expect(sdf).toBeInstanceOf(Uint8ClampedArray);
    expect(sdf.length).toBe(W * H);
  });

  it("sits at ~SDF_EDGE_ALPHA across the silhouette boundary", () => {
    // The true edge runs at x = 7.5 (between the last empty column 7 and
    // the first covered column 8). The two texels either side straddle it.
    const justInside = sdf[idx(R0, 16)];
    const justOutside = sdf[idx(R0 - 1, 16)];
    expect(justInside).toBeGreaterThan(SDF_EDGE_ALPHA);
    expect(justOutside).toBeLessThan(SDF_EDGE_ALPHA);
    // Their midpoint lands on the edge value (one texel = one ramp step
    // either way of ~255/SDF_RADIUS_PX alpha).
    expect((justInside + justOutside) / 2).toBeCloseTo(SDF_EDGE_ALPHA, 0);
    expect(Math.abs(justInside - SDF_EDGE_ALPHA)).toBeLessThanOrEqual(255 / SDF_RADIUS_PX + 1);
    expect(Math.abs(justOutside - SDF_EDGE_ALPHA)).toBeLessThanOrEqual(255 / SDF_RADIUS_PX + 1);
  });

  it("saturates to 255 well inside the shape (>= SDF_RADIUS_PX from any edge)", () => {
    // Grid centre: 8 texels from the nearest edge on every side.
    expect(sdf[idx(16, 16)]).toBe(255);
    expect(sdf[idx(15, 15)]).toBe(255);
  });

  it("saturates to 0 well outside the shape (>= SDF_RADIUS_PX past the edge)", () => {
    // Column 0 is 8 texels from the first covered column (8).
    expect(sdf[idx(0, 16)]).toBe(0);
    expect(sdf[idx(16, 0)]).toBe(0);
  });

  it("ramps monotonically from inside to outside along a scan line", () => {
    // Walking left from the centre toward the edge and out, alpha never
    // increases.
    for (let x = 16; x > 0; x--) {
      expect(sdf[idx(x, 16)]).toBeGreaterThanOrEqual(sdf[idx(x - 1, 16)]);
    }
  });

  it("crosses SDF_EDGE_ALPHA exactly once along that scan line, at the edge", () => {
    let crossings = 0;
    for (let x = 1; x <= 16; x++) {
      const a = sdf[idx(x - 1, 16)];
      const b = sdf[idx(x, 16)];
      if (a < SDF_EDGE_ALPHA && b >= SDF_EDGE_ALPHA) crossings++;
    }
    expect(crossings).toBe(1);
  });

  it("is symmetric about both axes for a symmetric shape", () => {
    for (let y = 0; y < H; y++) {
      for (let x = 0; x < W; x++) {
        expect(sdf[idx(x, y)]).toBe(sdf[idx(W - 1 - x, y)]);
        expect(sdf[idx(x, y)]).toBe(sdf[idx(x, H - 1 - y)]);
      }
    }
  });

  it("encodes roughly 255 / SDF_RADIUS_PX alpha units per pixel of distance", () => {
    // Two texels 2 px apart, both outside the shape and inside the ramp
    // band on the same row (edge at x = 7.5).
    const near = sdf[idx(R0 - 3, 16)]; // ~2.5 px outside the edge
    const far = sdf[idx(R0 - 5, 16)]; // ~4.5 px outside the edge
    expect(near - far).toBeCloseTo((2 * 255) / SDF_RADIUS_PX, -1);
  });

  it("respects the cutoff: the edge value is 255 * (1 - SDF_CUTOFF)", () => {
    expect(SDF_EDGE_ALPHA).toBe(Math.round(255 * (1 - SDF_CUTOFF)));
  });

  it("accepts a plain number[] as well as a typed array", () => {
    const asArray = coverageToSdf(Array.from(filledRectCoverage()), W, H);
    expect(Array.from(asArray)).toEqual(Array.from(sdf));
  });

  it("handles an anti-aliased (non-binary) edge without quantising to whole texels", () => {
    // A vertical half-plane: left half empty, a one-texel 50%-coverage
    // seam at x = 16, right half solid.
    const a = new Uint8ClampedArray(W * H);
    for (let y = 0; y < H; y++) {
      a[idx(16, y)] = 128;
      for (let x = 17; x < W; x++) a[idx(x, y)] = 255;
    }
    const field = coverageToSdf(a, W, H);
    // The 50% seam is the edge.
    expect(field[idx(16, 16)]).toBeCloseTo(SDF_EDGE_ALPHA, 0);
    expect(field[idx(17, 16)]).toBeGreaterThan(SDF_EDGE_ALPHA);
    expect(field[idx(15, 16)]).toBeLessThan(SDF_EDGE_ALPHA);
  });
});

describe("shapeIconId", () => {
  it("prefixes the shape key", () => {
    expect(shapeIconId("b738")).toBe("sf-ac-b738");
  });
});
