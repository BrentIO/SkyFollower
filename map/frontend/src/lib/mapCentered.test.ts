import { describe, expect, it } from "vitest";
import { CENTER_TOLERANCE_PX, isWithinCenterTolerance } from "./mapCentered";

describe("isWithinCenterTolerance", () => {
  it("is true when the two points are identical", () => {
    expect(isWithinCenterTolerance({ x: 500, y: 300 }, { x: 500, y: 300 })).toBe(true);
  });

  it("is true well within the default tolerance", () => {
    expect(isWithinCenterTolerance({ x: 501, y: 301 }, { x: 500, y: 300 })).toBe(true);
  });

  it("is true exactly at the boundary (distance == tolerance)", () => {
    // 3-4-5 triangle: distance is exactly 5px.
    expect(isWithinCenterTolerance({ x: 503, y: 304 }, { x: 500, y: 300 }, 5)).toBe(true);
  });

  it("is false just outside the boundary", () => {
    expect(isWithinCenterTolerance({ x: 503.1, y: 304 }, { x: 500, y: 300 }, 5)).toBe(false);
  });

  it("is false clearly outside tolerance", () => {
    expect(isWithinCenterTolerance({ x: 800, y: 300 }, { x: 500, y: 300 }, CENTER_TOLERANCE_PX)).toBe(false);
  });

  it("uses CENTER_TOLERANCE_PX as the default threshold", () => {
    const target = { x: 100, y: 100 };
    const justInside = { x: 100 + CENTER_TOLERANCE_PX - 0.5, y: 100 };
    const justOutside = { x: 100 + CENTER_TOLERANCE_PX + 0.5, y: 100 };
    expect(isWithinCenterTolerance(justInside, target)).toBe(true);
    expect(isWithinCenterTolerance(justOutside, target)).toBe(false);
  });

  it("is symmetric regardless of which point is 'current' vs 'target'", () => {
    const a = { x: 10, y: 20 };
    const b = { x: 12, y: 21 };
    expect(isWithinCenterTolerance(a, b)).toBe(isWithinCenterTolerance(b, a));
  });
});
