import { describe, expect, it } from "vitest";
import {
  FOLLOW_ICON,
  ISOLATE_ICON,
  MAXIMIZE_ICON,
  MINIMIZE_ICON,
  PAUSE_ICON,
  PLAY_ICON,
  RADAR_ICON,
  ROUTE_ICON,
  TAGS_ICON,
  TRACE_POINTS_ICON,
  TYPE_ICON,
  WEATHER_RADAR_ICON,
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

  it("is visually distinct from the existing recenter crosshair icon (different geometry)", () => {
    // lib/crosshairIcon.ts's crosshairSvgMarkup draws a dashed circle with
    // four short outward ticks -- ZOOM_TO_ICON's LocateFixed instead uses
    // two concentric solid circles, which is a different enough
    // silhouette that the two are not confusable at a glance.
    expect(ZOOM_TO_ICON.circles?.length).toBe(2);
  });
});

// ROUTE_ICON/TAGS_ICON/TYPE_ICON/RADAR_ICON path/circle/rect data copied
// byte-for-byte from lucide-icons/lucide's icons/route.svg,
// icons/square-text.svg, icons/type.svg, and icons/radar.svg (as of this
// writing) -- same fetch-don't-guess convention ISOLATE_ICON/ZOOM_TO_ICON/
// FOLLOW_ICON already used above.
describe("ROUTE_ICON / TAGS_ICON / TYPE_ICON / RADAR_ICON", () => {
  it("ROUTE_ICON is Lucide's Route glyph (two endpoint circles joined by a winding path)", () => {
    expect(ROUTE_ICON.circles).toEqual([
      { cx: 6, cy: 19, r: 3 },
      { cx: 18, cy: 5, r: 3 },
    ]);
    expect(ROUTE_ICON.paths).toEqual([{ d: "M9 19h8.5a3.5 3.5 0 0 0 0-7h-11a3.5 3.5 0 0 1 0-7H15" }]);
  });

  it("TAGS_ICON is Lucide's square-text glyph (rounded square + three text lines)", () => {
    expect(TAGS_ICON.rects).toEqual([{ x: 3, y: 3, width: 18, height: 18, rx: 2 }]);
    expect(TAGS_ICON.paths).toEqual([{ d: "M7 8h8" }, { d: "M7 12h10" }, { d: "M7 16h6" }]);
  });

  it("TYPE_ICON is Lucide's Type glyph (a stylized capital A: three plain strokes)", () => {
    expect(TYPE_ICON.paths).toEqual([
      { d: "M12 4v16" },
      { d: "M4 7V5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v2" },
      { d: "M9 20h6" },
    ]);
  });

  it("TYPE_ICON is deliberately distinct from TAGS_ICON so the two label toggles don't look identical", () => {
    expect(TYPE_ICON.rects).toBeUndefined();
    expect(TAGS_ICON.rects).not.toBeUndefined();
  });

  it("RADAR_ICON is Lucide's Radar glyph (concentric arcs, a center dot, and a sweep needle)", () => {
    expect(RADAR_ICON.circles).toEqual([{ cx: 12, cy: 12, r: 2 }]);
    expect(RADAR_ICON.paths).toHaveLength(7);
    expect(RADAR_ICON.paths?.map((p) => p.d)).toContain("m13.41 10.59 5.66-5.66");
  });
});

// WEATHER_RADAR_ICON/PLAY_ICON/PAUSE_ICON path/rect data copied
// byte-for-byte from lucide-icons/lucide's icons/cloud-rain.svg,
// icons/play.svg, and icons/pause.svg (as of this writing) -- same
// fetch-don't-guess convention as above.
describe("WEATHER_RADAR_ICON / PLAY_ICON / PAUSE_ICON", () => {
  it("WEATHER_RADAR_ICON is Lucide's CloudRain glyph (a cloud outline + three rain-drop strokes)", () => {
    expect(WEATHER_RADAR_ICON.paths).toEqual([
      { d: "M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242" },
      { d: "M16 14v6" },
      { d: "M8 14v6" },
      { d: "M12 16v6" },
    ]);
  });

  it("WEATHER_RADAR_ICON is deliberately distinct from RADAR_ICON, which already means Range Outline in this panel", () => {
    expect(WEATHER_RADAR_ICON.circles).toBeUndefined();
    expect(RADAR_ICON.circles).not.toBeUndefined();
  });

  it("PLAY_ICON is Lucide's Play glyph (single outlined triangle)", () => {
    expect(PLAY_ICON.paths).toEqual([
      { d: "M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z" },
    ]);
  });

  it("PAUSE_ICON is Lucide's Pause glyph (two vertical bars)", () => {
    expect(PAUSE_ICON.rects).toEqual([
      { x: 14, y: 3, width: 5, height: 18, rx: 1 },
      { x: 5, y: 3, width: 5, height: 18, rx: 1 },
    ]);
  });
});

// MAXIMIZE_ICON/MINIMIZE_ICON path data copied byte-for-byte from
// lucide-icons/lucide's icons/maximize.svg and icons/minimize.svg (as of
// this writing) -- same fetch-don't-guess convention as above.
describe("MAXIMIZE_ICON / MINIMIZE_ICON", () => {
  it("MAXIMIZE_ICON is Lucide's Maximize glyph (four outward-pointing corner brackets)", () => {
    expect(MAXIMIZE_ICON.paths).toEqual([
      { d: "M8 3H5a2 2 0 0 0-2 2v3" },
      { d: "M21 8V5a2 2 0 0 0-2-2h-3" },
      { d: "M3 16v3a2 2 0 0 0 2 2h3" },
      { d: "M16 21h3a2 2 0 0 0 2-2v-3" },
    ]);
    expect(MAXIMIZE_ICON.circles).toBeUndefined();
  });

  it("MINIMIZE_ICON is Lucide's Minimize glyph (the same four corners pointing inward)", () => {
    expect(MINIMIZE_ICON.paths).toEqual([
      { d: "M8 3v3a2 2 0 0 1-2 2H3" },
      { d: "M21 8h-3a2 2 0 0 1-2-2V3" },
      { d: "M3 16h3a2 2 0 0 1 2 2v3" },
      { d: "M16 21v-3a2 2 0 0 1 2-2h3" },
    ]);
    expect(MINIMIZE_ICON.circles).toBeUndefined();
  });

  it("MAXIMIZE_ICON and MINIMIZE_ICON are visually distinct (different geometry, not identical paths)", () => {
    expect(MAXIMIZE_ICON.paths?.map((p) => p.d)).not.toEqual(MINIMIZE_ICON.paths?.map((p) => p.d));
  });
});
