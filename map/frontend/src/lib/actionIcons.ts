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
// ROUTE_ICON/TAGS_ICON/TYPE_ICON/RADAR_ICON are Lucide's "Route"/"Tags"/
// "Type"/"Radar" glyphs (fetched byte-for-byte from lucide-icons/lucide,
// same convention as above) -- also no prior precedent in this codebase;
// picked per the issue's suggested set for ControlsPanel's toggle row
// (Route = two endpoints joined by a winding path, reading as "every
// flight's path"; Tags = the generic label/tag glyph for aircraft info-box
// labels; Type = a stylized "A", deliberately distinct from Tags so the
// two label toggles don't look identical; Radar = concentric arcs + sweep
// needle, reading directly as reception range/coverage).

export interface IconPath {
  d: string;
}
export interface IconCircle {
  cx: number;
  cy: number;
  r: number;
  /** True for a circle Lucide renders solid (`fill="currentColor"`) rather
   * than as an outline -- e.g. TAGS_ICON's small punch-hole dot. Absent/
   * false preserves every existing icon's outline-only rendering. */
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

// All optional -- a given icon only populates the shape kinds it uses.
export interface IconSpec {
  paths?: IconPath[];
  circles?: IconCircle[];
  lines?: IconLine[];
  polygons?: IconPolygon[];
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
  paths: [
    {
      d: "M13.172 2a2 2 0 0 1 1.414.586l6.71 6.71a2.4 2.4 0 0 1 0 3.408l-4.592 4.592a2.4 2.4 0 0 1-3.408 0l-6.71-6.71A2 2 0 0 1 6 9.172V3a1 1 0 0 1 1-1z",
    },
    { d: "M2 7v6.172a2 2 0 0 0 .586 1.414l6.71 6.71a2.4 2.4 0 0 0 3.191.193" },
  ],
  circles: [{ cx: 10.5, cy: 6.5, r: 0.5, filled: true }],
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
