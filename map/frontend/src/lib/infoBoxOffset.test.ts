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

  it("pins the cubic ease-in curve's values at representative zoom levels", () => {
    // A genuinely zoomed-out view (multi-state) keeps the existing small offset.
    expect(infoBoxOffsetForZoom(4)).toBeCloseTo(3, 2);

    // The "regional" 7-9 band -- where labels were reported floating --
    // still ramps in below the max, just against the smaller endpoints.
    expect(infoBoxOffsetForZoom(7)).toBeCloseTo(3.75, 2);
    expect(infoBoxOffsetForZoom(8)).toBeCloseTo(4.78, 2);
    expect(infoBoxOffsetForZoom(9)).toBeCloseTo(6.47, 2);

    // A close-in view keeps the full, still-tight anchored gap.
    expect(infoBoxOffsetForZoom(10)).toBeCloseTo(9, 2);
  });

  it("reduces the offset at the regional 7-9 band by more than a linear ramp would", () => {
    // A linear ramp spends its "budget" evenly across the whole zoom span,
    // so it would only be a 12-20% reduction off the max at the top of the
    // 7-9 band. The cubic curve reduces meaningfully more: at least 30% off
    // the max by zoom 8, and at least 15% off the max by zoom 9.
    const zoom8 = infoBoxOffsetForZoom(8);
    const zoom9 = infoBoxOffsetForZoom(9);

    expect(MAX_INFO_BOX_OFFSET - zoom8).toBeGreaterThan(0.3 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET));
    expect(MAX_INFO_BOX_OFFSET - zoom9).toBeGreaterThan(0.15 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET));
  });

  it("never exceeds the max or drops below the min at fractional/extreme zooms", () => {
    expect(infoBoxOffsetForZoom(9.9)).toBeLessThanOrEqual(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(-5)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(100)).toBe(MAX_INFO_BOX_OFFSET);
  });
});
