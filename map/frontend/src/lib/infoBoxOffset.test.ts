import { describe, expect, it } from "vitest";
import { infoBoxOffsetForZoom, MAX_INFO_BOX_OFFSET, MIN_INFO_BOX_OFFSET } from "./infoBoxOffset";

describe("infoBoxOffsetForZoom", () => {
  it("returns the max offset at and above the high-zoom threshold", () => {
    expect(infoBoxOffsetForZoom(10)).toBe(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(15)).toBe(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(20)).toBe(MAX_INFO_BOX_OFFSET);
  });

  it("returns the min offset at and below the low-zoom threshold", () => {
    expect(infoBoxOffsetForZoom(4)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(1)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(0)).toBe(MIN_INFO_BOX_OFFSET);
  });

  it("ramps monotonically between the thresholds, visibly smaller when zoomed out", () => {
    const zoomedOut = infoBoxOffsetForZoom(5);
    const mid = infoBoxOffsetForZoom(7);
    const zoomedIn = infoBoxOffsetForZoom(9);

    expect(zoomedOut).toBeGreaterThan(MIN_INFO_BOX_OFFSET);
    expect(zoomedOut).toBeLessThan(mid);
    expect(mid).toBeLessThan(zoomedIn);
    expect(zoomedIn).toBeLessThan(MAX_INFO_BOX_OFFSET);
  });

  it("never exceeds the max or drops below the min at fractional/extreme zooms", () => {
    expect(infoBoxOffsetForZoom(9.9)).toBeLessThanOrEqual(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(-5)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(100)).toBe(MAX_INFO_BOX_OFFSET);
  });
});
