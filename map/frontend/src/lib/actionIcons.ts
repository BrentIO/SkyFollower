// Path/shape data for this map view's icon-only buttons. Kept as plain
// data -- rendered into an <svg> by components/IconButton.tsx's shared
// ActionIcon -- rather than as JSX directly, so the exact geometry is a
// plain, unit-testable value.
//
// Every icon below is a Lucide glyph, fetched byte-for-byte from
// lucide-icons/lucide rather than hand-drawn, except TRACE_POINTS_ICON,
// which is copied verbatim from management-ui/frontend's own
// TRACE_POINTS_ICON_SVG so the same feature uses the same icon in both
// frontends (verified against that source in actionIcons.test.ts).
// RADAR_ICON ("Range Outline") and WEATHER_RADAR_ICON ("weather radar
// overlay") are deliberately distinct glyphs despite both being
// radar-themed, since they're unrelated features.

export interface IconPath {
  d: string;
}
export interface IconCircle {
  cx: number;
  cy: number;
  r: number;
  /** True for a circle Lucide renders solid (`fill="currentColor"`) rather
   * than as an outline -- e.g. a small punch-hole dot within a larger
   * glyph. Absent/false preserves every existing icon's outline-only
   * rendering. */
  filled?: boolean;
}
export interface IconLine {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
}
export interface IconPolygon {
  points: string;
}
export interface IconRect {
  x: number;
  y: number;
  width: number;
  height: number;
  rx?: number;
}

// All optional -- a given icon only populates the shape kinds it uses.
export interface IconSpec {
  paths?: IconPath[];
  circles?: IconCircle[];
  lines?: IconLine[];
  polygons?: IconPolygon[];
  rects?: IconRect[];
}

export const ISOLATE_ICON: IconSpec = {
  circles: [{ cx: 12, cy: 12, r: 3 }],
  paths: [
    { d: "M3 7V5a2 2 0 0 1 2-2h2" },
    { d: "M17 3h2a2 2 0 0 1 2 2v2" },
    { d: "M21 17v2a2 2 0 0 1-2 2h-2" },
    { d: "M7 21H5a2 2 0 0 1-2-2v-2" },
  ],
};

export const ZOOM_TO_ICON: IconSpec = {
  lines: [
    { x1: 2, y1: 12, x2: 5, y2: 12 },
    { x1: 19, y1: 12, x2: 22, y2: 12 },
    { x1: 12, y1: 2, x2: 12, y2: 5 },
    { x1: 12, y1: 19, x2: 12, y2: 22 },
  ],
  circles: [
    { cx: 12, cy: 12, r: 7 },
    { cx: 12, cy: 12, r: 3 },
  ],
};

export const FOLLOW_ICON: IconSpec = {
  polygons: [{ points: "3 11 22 2 13 21 11 13 3 11" }],
};

export const TRACE_POINTS_ICON: IconSpec = {
  paths: [
    { d: "m10.586 5.414-5.172 5.172" },
    { d: "m18.586 13.414-5.172 5.172" },
    { d: "M6 12h12" },
  ],
  circles: [
    { cx: 12, cy: 20, r: 2 },
    { cx: 12, cy: 4, r: 2 },
    { cx: 20, cy: 12, r: 2 },
    { cx: 4, cy: 12, r: 2 },
  ],
};

export const ROUTE_ICON: IconSpec = {
  circles: [
    { cx: 6, cy: 19, r: 3 },
    { cx: 18, cy: 5, r: 3 },
  ],
  paths: [{ d: "M9 19h8.5a3.5 3.5 0 0 0 0-7h-11a3.5 3.5 0 0 1 0-7H15" }],
};

export const TAGS_ICON: IconSpec = {
  rects: [{ x: 3, y: 3, width: 18, height: 18, rx: 2 }],
  paths: [{ d: "M7 8h8" }, { d: "M7 12h10" }, { d: "M7 16h6" }],
};

export const TYPE_ICON: IconSpec = {
  paths: [{ d: "M12 4v16" }, { d: "M4 7V5a1 1 0 0 1 1-1h14a1 1 0 0 1 1 1v2" }, { d: "M9 20h6" }],
};

export const RADAR_ICON: IconSpec = {
  paths: [
    { d: "M19.07 4.93A10 10 0 0 0 6.99 3.34" },
    { d: "M4 6h.01" },
    { d: "M2.29 9.62A10 10 0 1 0 21.31 8.35" },
    { d: "M16.24 7.76A6 6 0 1 0 8.23 16.67" },
    { d: "M12 18h.01" },
    { d: "M17.99 11.66A6 6 0 0 1 15.77 16.67" },
    { d: "m13.41 10.59 5.66-5.66" },
  ],
  circles: [{ cx: 12, cy: 12, r: 2 }],
};

export const WEATHER_RADAR_ICON: IconSpec = {
  paths: [
    { d: "M4 14.899A7 7 0 1 1 15.71 8h1.79a4.5 4.5 0 0 1 2.5 8.242" },
    { d: "M16 14v6" },
    { d: "M8 14v6" },
    { d: "M12 16v6" },
  ],
};

export const PLAY_ICON: IconSpec = {
  paths: [{ d: "M5 5a2 2 0 0 1 3.008-1.728l11.997 6.998a2 2 0 0 1 .003 3.458l-12 7A2 2 0 0 1 5 19z" }],
};

export const PAUSE_ICON: IconSpec = {
  rects: [
    { x: 14, y: 3, width: 5, height: 18, rx: 1 },
    { x: 5, y: 3, width: 5, height: 18, rx: 1 },
  ],
};

export const MAXIMIZE_ICON: IconSpec = {
  paths: [
    { d: "M8 3H5a2 2 0 0 0-2 2v3" },
    { d: "M21 8V5a2 2 0 0 0-2-2h-3" },
    { d: "M3 16v3a2 2 0 0 0 2 2h3" },
    { d: "M16 21h3a2 2 0 0 0 2-2v-3" },
  ],
};

export const MINIMIZE_ICON: IconSpec = {
  paths: [
    { d: "M8 3v3a2 2 0 0 1-2 2H3" },
    { d: "M21 8h-3a2 2 0 0 1-2-2V3" },
    { d: "M3 16h3a2 2 0 0 1 2 2v3" },
    { d: "M16 21v-3a2 2 0 0 1 2-2h3" },
  ],
};

// Lucide's "Scaling" glyph -- deliberately distinct from
// MAXIMIZE_ICON/MINIMIZE_ICON above, which are about the whole page's
// fullscreen state, not per-element size.
export const DISPLAY_SCALE_ICON: IconSpec = {
  paths: [
    { d: "M12 3H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2v-7" },
    { d: "M14 15H9v-5" },
    { d: "M16 3h5v5" },
    { d: "M21 3 9 15" },
  ],
};

export const SETTINGS_ICON: IconSpec = {
  paths: [
    {
      d: "M9.671 4.136a2.34 2.34 0 0 1 4.659 0 2.34 2.34 0 0 0 3.319 1.915 2.34 2.34 0 0 1 2.33 4.033 2.34 2.34 0 0 0 0 3.831 2.34 2.34 0 0 1-2.33 4.033 2.34 2.34 0 0 0-3.319 1.915 2.34 2.34 0 0 1-4.659 0 2.34 2.34 0 0 0-3.32-1.915 2.34 2.34 0 0 1-2.33-4.033 2.34 2.34 0 0 0 0-3.831A2.34 2.34 0 0 1 6.35 6.051a2.34 2.34 0 0 0 3.319-1.915",
    },
  ],
  circles: [{ cx: 12, cy: 12, r: 3 }],
};
