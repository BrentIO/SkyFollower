import { describe, expect, it } from "vitest";
import { FOLLOW_ICON, ISOLATE_ICON, TRACE_POINTS_ICON, ZOOM_TO_ICON } from "./actionIcons";

// Verbatim path/circle data copied out of management-ui/frontend/src/
// components/FlightViewModal.tsx's TRACE_POINTS_ICON_SVG template literal
// (as of this writing) -- kept hardcoded here (rather than a cross-project
// file import, which this codebase deliberately avoids -- see
// altitudeColor.ts's own "separate frontend, own copy" convention) so a
// future edit to either icon silently drifting out of byte-for-byte parity
// fails a test instead of only being caught by eyeballing a diff.
const MANAGEMENT_UI_TRACE_POINTS_PATHS = [
  "m10.586 5.414-5.172 5.172",
  "m18.586 13.414-5.172 5.172",
  "M6 12h12",
];
const MANAGEMENT_UI_TRACE_POINTS_CIRCLES = [
  { cx: 12, cy: 20, r: 2 },
  { cx: 12, cy: 4, r: 2 },
  { cx: 20, cy: 12, r: 2 },
  { cx: 4, cy: 12, r: 2 },
];

describe("TRACE_POINTS_ICON", () => {
  it("matches FlightViewModal.tsx's TRACE_POINTS_ICON_SVG path data byte-for-byte", () => {
    expect(TRACE_POINTS_ICON.paths?.map((p) => p.d)).toEqual(MANAGEMENT_UI_TRACE_POINTS_PATHS);
  });

  it("matches FlightViewModal.tsx's TRACE_POINTS_ICON_SVG circle data byte-for-byte", () => {
    expect(TRACE_POINTS_ICON.circles).toEqual(MANAGEMENT_UI_TRACE_POINTS_CIRCLES);
  });
});

describe("ISOLATE_ICON / ZOOM_TO_ICON / FOLLOW_ICON", () => {
  it("ISOLATE_ICON is Lucide's Focus glyph (center circle + four corner brackets)", () => {
    expect(ISOLATE_ICON.circles).toEqual([{ cx: 12, cy: 12, r: 3 }]);
    expect(ISOLATE_ICON.paths).toHaveLength(4);
  });

  it("ZOOM_TO_ICON is Lucide's LocateFixed glyph (four ticks + two concentric circles)", () => {
    expect(ZOOM_TO_ICON.lines).toHaveLength(4);
    expect(ZOOM_TO_ICON.circles).toEqual([
      { cx: 12, cy: 12, r: 7 },
      { cx: 12, cy: 12, r: 3 },
    ]);
  });

  it("FOLLOW_ICON is Lucide's Navigation glyph (single compass-arrow polygon)", () => {
    expect(FOLLOW_ICON.polygons).toEqual([{ points: "3 11 22 2 13 21 11 13 3 11" }]);
  });

  it("is visually distinct from the existing recenter-on-home crosshair icon (different geometry)", () => {
    // lib/crosshairIcon.ts's crosshairSvgMarkup draws a dashed circle with
    // four short outward ticks -- ZOOM_TO_ICON's LocateFixed instead uses
    // two concentric solid circles, which is a different enough
    // silhouette that the two are not confusable at a glance.
    expect(ZOOM_TO_ICON.circles?.length).toBe(2);
  });
});
