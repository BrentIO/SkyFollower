import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see ControlsPanel.test.ts/AircraftDetailPanel.test.ts's
// own use of this) -- no jsdom/component-render test setup in this project
// (see lib/config.test.ts), so structure/wiring is checked by reading the
// actual source text rather than rendering.
import panelSource from "./AircraftListPanel.tsx?raw";

describe("column definitions -- data-driven array, not hardcoded per-column JSX", () => {
  it("defines AIRCRAFT_LIST_COLUMNS as an array of { key, header, sortKey, render }", () => {
    expect(panelSource).toMatch(/const AIRCRAFT_LIST_COLUMNS: AircraftListColumn\[\] = \[/);
    expect(panelSource).toContain("sortKey:");
    expect(panelSource).toContain("render:");
  });

  it("renders the table body by mapping AIRCRAFT_LIST_COLUMNS, not one <td> per column", () => {
    expect(panelSource).toContain("AIRCRAFT_LIST_COLUMNS.map((column) => (");
    expect(panelSource).not.toMatch(/<td[^>]*>\{row\.ident/);
  });

  it("declares the six required columns, in order: Ident, Registration, Type, Desc, Altitude, Distance", () => {
    const keys = ["ident", "registration", "type", "desc", "altitude", "distance"];
    const indices = keys.map((key) => panelSource.indexOf(`key: "${key}"`));
    for (const index of indices) expect(index).toBeGreaterThan(-1);
    for (let i = 1; i < indices.length; i++) {
      expect(indices[i]).toBeGreaterThan(indices[i - 1]);
    }
    expect(panelSource).toContain('header: "Ident"');
    expect(panelSource).toContain('header: "Registration"');
    expect(panelSource).toContain('header: "Type"');
    expect(panelSource).toContain('header: "Desc"');
    expect(panelSource).toContain('header: "Altitude"');
    expect(panelSource).toContain('header: "Distance"');
  });
});

describe("Ident column -- Military/Special Livery pills", () => {
  it("reuses AircraftDetailPanel's BADGE_BASE/BADGE_CLASSES convention", () => {
    expect(panelSource).toContain('import { BADGE_BASE, BADGE_CLASSES } from "./AircraftDetailPanel"');
    expect(panelSource).toContain("${BADGE_BASE} ${BADGE_CLASSES.green}");
    expect(panelSource).toContain("${BADGE_BASE} ${BADGE_CLASSES.yellow}");
  });

  it("renders military (green M) before specialLivery (yellow S) before the ident text", () => {
    const identColumnStart = panelSource.indexOf('key: "ident"');
    const identColumnEnd = panelSource.indexOf('key: "registration"');
    const callSite = panelSource.slice(identColumnStart, identColumnEnd);
    const militaryIndex = callSite.indexOf("row.military");
    const liveryIndex = callSite.indexOf("row.specialLivery");
    const identTextIndex = callSite.indexOf("row.ident ?? ");
    expect(militaryIndex).toBeGreaterThan(-1);
    expect(liveryIndex).toBeGreaterThan(militaryIndex);
    expect(identTextIndex).toBeGreaterThan(liveryIndex);
  });

  it("sorts Ident on the plain ident string only -- the pills never participate", () => {
    const identColumnStart = panelSource.indexOf('key: "ident"');
    const identColumnEnd = panelSource.indexOf('key: "registration"');
    const callSite = panelSource.slice(identColumnStart, identColumnEnd);
    expect(callSite).toContain("sortKey: (row) => row.ident,");
  });
});

describe("Altitude column", () => {
  it("renders the precomputed altitudeDisplay (▼/▲ prefix handled in lib/aircraftListRow.ts)", () => {
    const altColumnStart = panelSource.indexOf('key: "altitude"');
    const altColumnEnd = panelSource.indexOf('key: "distance"');
    const callSite = panelSource.slice(altColumnStart, altColumnEnd);
    expect(callSite).toContain("row.altitudeDisplay");
    expect(callSite).toContain("sortKey: (row) => row.altitudeFt,");
  });
});

describe("Distance column", () => {
  it("renders the precomputed distanceDisplay, sorts on the numeric distanceNm", () => {
    const distColumnStart = panelSource.indexOf('key: "distance"');
    const callSite = panelSource.slice(distColumnStart, distColumnStart + 200);
    expect(callSite).toContain("row.distanceDisplay");
    expect(callSite).toContain("sortKey: (row) => row.distanceNm,");
  });
});

describe("row population -- via lib/aircraftListRow.ts", () => {
  it("builds rows via buildAircraftListRows (the AircraftMap.hidden filter)", () => {
    expect(panelSource).toContain('import { buildAircraftListRows, type AircraftListRow } from "../lib/aircraftListRow"');
    expect(panelSource).toContain("buildAircraftListRows(displayedAircraft, center)");
  });
});

describe("sorting -- header click toggles/switches, default Distance ascending", () => {
  it("defaults to Distance, ascending on first open", () => {
    expect(panelSource).toContain('const DEFAULT_SORT_STATE: AircraftListSortState = { columnKey: "distance", dir: "asc" };');
  });

  it("wires each header button to nextAircraftListSortState via handleSortChange", () => {
    expect(panelSource).toContain("nextAircraftListSortState(current, columnKey)");
    expect(panelSource).toContain("onClick={() => handleSortChange(column.key)}");
  });

  it("sets aria-sort on the header button, matching HistoryView's convention", () => {
    expect(panelSource).toMatch(/aria-sort=\{active \? \(sort\.dir === "asc" \? "ascending" : "descending"\) : "none"\}/);
  });

  it("shows the chevron only on the active column", () => {
    expect(panelSource).toContain("{active && <SortChevron direction={sort.dir} />}");
  });
});

describe("row banding / emergency highlight", () => {
  it("alternates banding by row index", () => {
    expect(panelSource).toContain("index % 2 === 0 ? ROW_BAND_EVEN : ROW_BAND_ODD");
  });

  it("overrides banding entirely (not blended) for an emergency squawk row", () => {
    expect(panelSource).toContain("row.emergency ? ROW_EMERGENCY :");
  });

  it("defines a distinct red background for the emergency row", () => {
    expect(panelSource).toContain('ROW_EMERGENCY = "bg-red-100 dark:bg-red-900/70"');
  });
});

describe("row click -- selects the aircraft", () => {
  it("wires row onClick to the onSelect prop with the row's icao_hex", () => {
    expect(panelSource).toContain("onClick={() => onSelect(row.icaoHex)}");
  });
});

describe("panel mechanics -- edge tab, vertical centering, z-index, opacity", () => {
  it("is vertically centered on the viewport edge, not top-aligned", () => {
    expect(panelSource).toContain("top-1/2 right-0");
    expect(panelSource).toContain("translateY(-50%)");
    expect(panelSource).not.toContain("top-4 right-4");
  });

  it("slides by exactly the panel's own width when closed, not the tab+panel combined width", () => {
    expect(panelSource).toContain("const PANEL_WIDTH_PX = 720;");
    expect(panelSource).toContain("translateX(${open ? 0 : PANEL_WIDTH_PX}px)");
  });

  it("uses a ◀/▶ edge-tab handle", () => {
    expect(panelSource).toContain("{open ? \"▶\" : \"◀\"}");
  });

  it("reuses MAX_LABEL_Z_INDEX rather than inventing a second stacking constant", () => {
    expect(panelSource).toContain('import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder"');
    expect(panelSource).toContain("zIndex: MAX_LABEL_Z_INDEX");
  });

  it("is fully opaque (no /90-style translucency) on the panel body", () => {
    const bodyIndex = panelSource.indexOf("pointer-events-auto flex max-h-[80vh]");
    expect(bodyIndex).toBeGreaterThan(-1);
    const callSite = panelSource.slice(bodyIndex, bodyIndex + 200);
    expect(callSite).not.toContain("/90");
    expect(callSite).toContain("bg-white");
  });
});

describe("header -- title, collapse handle, and relocated aircraft count only", () => {
  it("renders the aircraftCount prop, not the Search/Filters/Messages-per-second widgets", () => {
    expect(panelSource).toContain("{aircraftCount} aircraft");
    expect(panelSource).not.toContain("Search");
    expect(panelSource).not.toContain("Filters");
    expect(panelSource).not.toContain("Messages:");
  });
});

describe("performance -- throttled row rebuild", () => {
  it("reuses lib/syncThrottle.ts's throttle convention, at MAP_SYNC_THROTTLE_MS", () => {
    expect(panelSource).toContain('import { createTrailingThrottle, MAP_SYNC_THROTTLE_MS } from "../lib/syncThrottle"');
    expect(panelSource).toContain("createTrailingThrottle(MAP_SYNC_THROTTLE_MS)");
    expect(panelSource).toContain("throttleRef.current.request(");
  });

  it("cancels any pending throttled run on unmount", () => {
    expect(panelSource).toContain("throttleRef.current.cancel()");
  });
});
