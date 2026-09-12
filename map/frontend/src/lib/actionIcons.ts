// Path/shape data for the aircraft detail panel's action-row icons
// (components/AircraftDetailPanel.tsx: Isolate/Zoom To/Follow/Trace
// Points). Kept as plain data -- rendered into an <svg> by the
// component's ActionIcon -- rather than as JSX directly, so the exact
// geometry is a plain, unit-testable value (this project has no jsdom/
// component-render test setup -- see lib/config.test.ts's own note).
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
// recenter-on-home glyph, Navigation = the conventional map-app "follow
// me" compass-arrow).

export interface IconPath {
  d: string;
}
export interface IconCircle {
  cx: number;
  cy: number;
  r: number;
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
