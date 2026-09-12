import { describe, expect, it } from "vitest";
import {
  FOLLOW_ICON,
  ISOLATE_ICON,
  RADAR_ICON,
  ROUTE_ICON,
  TAGS_ICON,
  TRACE_POINTS_ICON,
  TYPE_ICON,
  ZOOM_TO_ICON,
} from "./actionIcons";

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

// ROUTE_ICON/TAGS_ICON/TYPE_ICON/RADAR_ICON path/circle data copied
// byte-for-byte from lucide-icons/lucide's icons/route.svg, icons/tags.svg,
// icons/type.svg, and icons/radar.svg (as of this writing) -- same
// fetch-don't-guess convention ISOLATE_ICON/ZOOM_TO_ICON/FOLLOW_ICON
// already used above.
describe("ROUTE_ICON / TAGS_ICON / TYPE_ICON / RADAR_ICON", () => {
  it("ROUTE_ICON is Lucide's Route glyph (two endpoint circles joined by a winding path)", () => {
    expect(ROUTE_ICON.circles).toEqual([
      { cx: 6, cy: 19, r: 3 },
      { cx: 18, cy: 5, r: 3 },
    ]);
    expect(ROUTE_ICON.paths).toEqual([{ d: "M9 19h8.5a3.5 3.5 0 0 0 0-7h-11a3.5 3.5 0 0 1 0-7H15" }]);
  });

  it("TAGS_ICON is Lucide's Tags glyph (two tag paths + a filled punch-hole dot)", () => {
    expect(TAGS_ICON.paths).toEqual([
      {
        d: "M13.172 2a2 2 0 0 1 1.414.586l6.71 6.71a2.4 2.4 0 0 1 0 3.408l-4.592 4.592a2.4 2.4 0 0 1-3.408 0l-6.71-6.71A2 2 0 0 1 6 9.172V3a1 1 0 0 1 1-1z",
      },
      { d: "M2 7v6.172a2 2 0 0 0 .586 1.414l6.71 6.71a2.4 2.4 0 0 0 3.191.193" },
    ]);
    expect(TAGS_ICON.circles).toEqual([{ cx: 10.5, cy: 6.5, r: 0.5, filled: true }]);
  });

  it("TYPE_ICON is Lucide's Type glyph (a stylized capital A: three plain strokes)", () => {
    expect(TYPE_ICON.paths).toEqual([
      { d: "M12 4v16" },
      { d: "M4 7V5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v2" },
      { d: "M9 20h6" },
    ]);
  });

  it("TYPE_ICON is deliberately distinct from TAGS_ICON so the two label toggles don't look identical", () => {
    expect(TYPE_ICON.circles).toBeUndefined();
    expect(TAGS_ICON.circles).not.toBeUndefined();
  });

  it("RADAR_ICON is Lucide's Radar glyph (concentric arcs, a center dot, and a sweep needle)", () => {
    expect(RADAR_ICON.circles).toEqual([{ cx: 12, cy: 12, r: 2 }]);
    expect(RADAR_ICON.paths).toHaveLength(7);
    expect(RADAR_ICON.paths?.map((p) => p.d)).toContain("m13.41 10.59 5.66-5.66");
  });
});
