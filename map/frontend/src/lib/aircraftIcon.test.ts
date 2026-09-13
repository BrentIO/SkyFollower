import { afterAll, beforeAll, describe, expect, it } from "vitest";
import type { AircraftShape } from "./aircraftShapes.generated";
import {
  buildShapeIconImageData,
  coverageToSdf,
  SDF_CANVAS_PX,
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

describe("buildShapeIconImageData -- accent cutout", () => {
  // `buildShapeIconImageData` needs a 2D canvas, which this project's
  // "node" vitest environment doesn't provide (see the file-header note
  // above). Rather than pull in jsdom + a canvas polyfill, this installs a
  // minimal fake `document`/`Path2D`/`CanvasRenderingContext2D` for just
  // this suite -- only the handful of calls the function under test
  // actually makes (clearRect, setTransform, fill, stroke,
  // globalCompositeOperation, getImageData), and only geometry this suite
  // needs: an axis-aligned filled rectangle (a synthetic outline `d`) and
  // a straight horizontal line (a synthetic `accentD`). That's enough to
  // exercise the real destination-out cutout logic in aircraftIcon.ts
  // end-to-end (including the real `coverageToSdf`), without reimplementing
  // a general SVG path rasterizer.

  class FakePath2D {
    constructor(public readonly d: string) {}
  }

  function parsePoints(d: string): Array<[number, number]> {
    const points: Array<[number, number]> = [];
    const re = /[ML]\s*(-?[\d.]+)[,\s]+(-?[\d.]+)/g;
    for (const m of d.matchAll(re)) {
      points.push([Number(m[1]), Number(m[2])]);
    }
    return points;
  }

  class FakeCanvasRenderingContext2D {
    fillStyle = "#000000";
    lineWidth = 1;
    globalCompositeOperation: "source-over" | "destination-out" = "source-over";
    private transform = { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 };
    readonly data: Uint8ClampedArray;

    constructor(
      private readonly width: number,
      private readonly height: number,
    ) {
      this.data = new Uint8ClampedArray(width * height * 4);
    }

    clearRect(): void {
      this.data.fill(0);
    }

    setTransform(a: number, b: number, c: number, d: number, e: number, f: number): void {
      this.transform = { a, b, c, d, e, f };
    }

    private toDevice(x: number, y: number): [number, number] {
      const t = this.transform;
      return [t.a * x + t.c * y + t.e, t.b * x + t.d * y + t.f];
    }

    private paintPixel(x: number, y: number, isFill: boolean): void {
      if (x < 0 || y < 0 || x >= this.width || y >= this.height) return;
      const i = (y * this.width + x) * 4;
      if (this.globalCompositeOperation === "destination-out") {
        this.data[i + 3] = 0;
        return;
      }
      if (isFill) {
        this.data[i] = 0;
        this.data[i + 1] = 0;
        this.data[i + 2] = 0;
        this.data[i + 3] = 255;
      }
    }

    /** Axis-aligned bounding-box fill -- sufficient for this suite's
     * rectangular synthetic shapes. */
    fill(path: FakePath2D): void {
      const pts = parsePoints(path.d).map(([x, y]) => this.toDevice(x, y));
      const xs = pts.map((p) => p[0]);
      const ys = pts.map((p) => p[1]);
      const x0 = Math.min(...xs);
      const x1 = Math.max(...xs);
      const y0 = Math.min(...ys);
      const y1 = Math.max(...ys);
      for (let y = Math.ceil(y0); y < y1; y++) {
        for (let x = Math.ceil(x0); x < x1; x++) this.paintPixel(x, y, true);
      }
    }

    /** Straight axis-aligned (horizontal or vertical) line stroke -- this
     * suite's `accentD` is always one such segment. */
    stroke(path: FakePath2D): void {
      const pts = parsePoints(path.d).map(([x, y]) => this.toDevice(x, y));
      const [[x0, y0], [x1, y1]] = pts;
      const scale = Math.hypot(this.transform.a, this.transform.b) || 1;
      const halfWidth = (this.lineWidth * scale) / 2;
      if (y0 === y1) {
        for (let py = Math.floor(y0 - halfWidth); py <= Math.ceil(y0 + halfWidth); py++) {
          for (let px = Math.floor(Math.min(x0, x1)); px <= Math.ceil(Math.max(x0, x1)); px++) {
            this.paintPixel(px, py, false);
          }
        }
      } else if (x0 === x1) {
        for (let px = Math.floor(x0 - halfWidth); px <= Math.ceil(x0 + halfWidth); px++) {
          for (let py = Math.floor(Math.min(y0, y1)); py <= Math.ceil(Math.max(y0, y1)); py++) {
            this.paintPixel(px, py, false);
          }
        }
      } else {
        throw new Error("FakeCanvasRenderingContext2D.stroke: only axis-aligned segments are supported in tests");
      }
    }

    getImageData(_x: number, _y: number, w: number, h: number) {
      return { data: this.data, width: w, height: h, colorSpace: "srgb" as const };
    }
  }

  beforeAll(() => {
    (globalThis as unknown as { document: unknown }).document = {
      createElement(tag: string) {
        if (tag !== "canvas") throw new Error(`unsupported tag ${tag}`);
        const canvas = {
          width: 0,
          height: 0,
          getContext(type: string) {
            if (type !== "2d") return null;
            return new FakeCanvasRenderingContext2D(canvas.width, canvas.height);
          },
        };
        return canvas;
      },
    };
    (globalThis as unknown as { Path2D: unknown }).Path2D = FakePath2D;
  });

  afterAll(() => {
    delete (globalThis as { document?: unknown }).document;
    delete (globalThis as { Path2D?: unknown }).Path2D;
  });

  // A 10x10-unit square, centred at (5, 5) -- k = SDF_SHAPE_PX / span comes
  // out to a whole number of device pixels, keeping the fake rasterizer's
  // bbox-fill exact.
  const SQUARE_SHAPE: AircraftShape = {
    d: "M0,0 L10,0 L10,10 L0,10 Z",
    cx: 5,
    cy: 5,
    span: 10,
    scale: 1,
  };
  const SQUARE_SHAPE_WITH_ACCENT: AircraftShape = {
    ...SQUARE_SHAPE,
    // A straight horizontal line through the square's vertical centre.
    accentD: "M0,5 L10,5",
    accentStrokeWidth: 2,
  };

  const idx = (x: number, y: number) => y * SDF_CANVAS_PX + x;
  const center = SDF_CANVAS_PX / 2;

  it("a shape with accentD drops alpha below the solid-fill level along the accent line", () => {
    const imageData = buildShapeIconImageData(SQUARE_SHAPE_WITH_ACCENT);
    const alphaAt = (x: number, y: number) => imageData.data[idx(x, y) * 4 + 3];

    // Deep interior, away from every edge (including the accent line) --
    // the shape's solid-fill level.
    const solidFillLevel = alphaAt(center, center - 20);
    expect(solidFillLevel).toBe(255);

    // On the accent line, at the square's centre -- inside the cutout.
    const onAccentLine = alphaAt(center, center);
    expect(onAccentLine).toBeLessThan(solidFillLevel);
  });

  it("a shape without accentD is unaffected at the same coordinate", () => {
    const imageData = buildShapeIconImageData(SQUARE_SHAPE);
    const alphaAt = (x: number, y: number) => imageData.data[idx(x, y) * 4 + 3];

    const solidFillLevel = alphaAt(center, center - 20);
    const wouldBeAccentLine = alphaAt(center, center);
    expect(wouldBeAccentLine).toBe(solidFillLevel);
    expect(wouldBeAccentLine).toBe(255);
  });
});
