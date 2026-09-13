// Path/shape data for this map view's icon-only buttons: both
// components/AircraftDetailPanel.tsx's action row (Isolate/Zoom To/Follow/
// Trace Points) and components/ControlsPanel.tsx's toggle row (History/
// Labels/Map Labels/Range Outline). Kept as plain data -- rendered into an
// <svg> by components/IconButton.tsx's shared ActionIcon -- rather than as
// JSX directly, so the exact geometry is a plain, unit-testable value
// (this project has no jsdom/component-render test setup -- see
// lib/config.test.ts's own note).
//
// TRACE_POINTS_ICON is Lucide's "Waypoints" glyph, copied byte-for-byte
// (identical path `d` and circle cx/cy/r values) from management-ui/
// frontend/src/components/FlightViewModal.tsx's TRACE_POINTS_ICON_SVG --
// same feature, same icon, in both frontends (see the issue this
// implements). Verified against that source in actionIcons.test.ts.
//
// ISOLATE_ICON/ZOOM_TO_ICON/FOLLOW_ICON are Lucide's "Focus"/
// "LocateFixed"/"Navigation" glyphs (fetched from lucide-icons/lucide) --
// no prior precedent anywhere in this codebase; picked per the issue's
// suggested icon set (Focus = "focus on this one", LocateFixed = "center/
// target this position" and visually distinct from lib/crosshairIcon.ts's
// recenter glyph, Navigation = the conventional map-app "follow me"
// compass-arrow).
//
// ROUTE_ICON/TAGS_ICON/TYPE_ICON/RADAR_ICON are Lucide's "Route"/
// "square-text"/"Type"/"Radar" glyphs (fetched byte-for-byte from
// lucide-icons/lucide, same convention as above) -- also no prior
// precedent in this codebase; picked per the issue's suggested set for
// ControlsPanel's toggle row (Route = two endpoints joined by a winding
// path, reading as "every flight's path"; square-text = a rounded square
// containing three text lines, reading as "aircraft info-box labels" --
// TAGS_ICON was originally Lucide's "Tags" glyph, swapped to square-text
// per Brent's preference with no functional change; Type = a stylized "A",
// deliberately distinct from square-text so the two label toggles don't
// look identical; Radar = concentric arcs + sweep needle, reading directly
// as reception range/coverage).
//
// MAXIMIZE_ICON/MINIMIZE_ICON are Lucide's "Maximize"/"Minimize" glyphs
// (fetched byte-for-byte from lucide-icons/lucide's icons/maximize.svg and
// icons/minimize.svg), same fetch-don't-guess convention as above. Used
// together as a pair by ControlsPanel's fullscreen toggle -- Maximize
// (four corner brackets not quite forming a closed square) when the page
// isn't fullscreen, swapping to Minimize (the same four corners pointing
// inward) once it is, per the issue's icon-state convention.

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
