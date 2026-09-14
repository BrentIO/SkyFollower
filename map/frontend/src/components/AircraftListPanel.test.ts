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

describe("panel mechanics -- push-layout flex sibling, edge tab, opacity (#1769)", () => {
  it("is a real flex sibling (push layout), not an absolute overlay", () => {
    // #1769: the drawer used to be an absolute top-1/2 overlay translated
    // by transform; it's now a flex item whose own width the map area
    // shrinks to accommodate, so it can never cover ControlsPanel and the
    // map keeps its center centered in whatever width remains.
    expect(panelSource).toContain("flex h-full flex-none items-stretch overflow-hidden");
    expect(panelSource).not.toContain("absolute top-1/2 right-0");
    expect(panelSource).not.toContain("transform:");
    expect(panelSource).not.toContain("translateY(-50%)");
  });

  it("transitions width (not transform) between the tab-only and tab+panel widths", () => {
    expect(panelSource).toContain("const TAB_WIDTH_PX = 24;");
    expect(panelSource).toContain("width: open ? TAB_WIDTH_PX + width : TAB_WIDTH_PX");
  });

  it("suppresses the open/close width transition while actively resize-dragging (#1784)", () => {
    expect(panelSource).toContain('resizing ? "" : "transition-[width] duration-200"');
  });

  it("keeps the tab vertically centered within the drawer's own full height, not top-aligned", () => {
    const tabWrapperIndex = panelSource.indexOf(
      'className="flex h-full flex-none items-center bg-white dark:bg-slate-900"',
    );
    expect(tabWrapperIndex).toBeGreaterThan(-1);
  });

  it("uses a ◀/▶ edge-tab handle", () => {
    expect(panelSource).toContain("{open ? \"▶\" : \"◀\"}");
  });

  it("spans the drawer's full height, no vertical-centering cap on the panel body", () => {
    expect(panelSource).not.toContain("max-h-[80vh]");
    expect(panelSource).toContain("flex h-full flex-none flex-col overflow-hidden rounded-l-md bg-white");
  });

  it("reuses MAX_LABEL_Z_INDEX (#1790, reversing #1769's assumption)", () => {
    // #1769 dropped this on the assumption that a flex sibling doesn't need
    // to out-stack anything. That assumption was wrong: the map area (this
    // panel's flex sibling) has no z-index of its own, so it doesn't scope
    // InfoBoxLayer's label z-indices to within itself -- they compete
    // directly against this panel in the shared outer stacking context,
    // and a positioned label painted over this plain flex child regardless
    // of DOM order. See the test above for the fix.
    expect(panelSource).toContain('import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder"');
    expect(panelSource).toContain("zIndex: MAX_LABEL_Z_INDEX + 1");
  });

  it("is fully opaque (no /90-style translucency) on the panel body", () => {
    const bodyIndex = panelSource.indexOf("flex h-full flex-none flex-col overflow-hidden rounded-l-md bg-white");
    expect(bodyIndex).toBeGreaterThan(-1);
    const callSite = panelSource.slice(bodyIndex, bodyIndex + 200);
    expect(callSite).not.toContain("/90");
    expect(callSite).toContain("bg-white");
  });

  it("gives the tab wrapper an opaque background spanning its full height, not just the button (#1803)", () => {
    // #1803: the outer wrapper's zIndex (MAX_LABEL_Z_INDEX + 1, tested above)
    // only wins paint order where it actually paints a pixel. InfoBoxLayer's
    // boxes are absolutely positioned with no overflow clipping on the map
    // area, so a box anchored near the map's right edge can visually spill
    // into this h-6-wide tab strip. Previously only the h-12 button (not the
    // full h-full wrapper around it) had a background, so a label landing
    // above/below the button -- in the wrapper's unpainted margin -- showed
    // straight through despite being "under" a higher z-index element.
    // Giving the wrapper itself an opaque background the button also sits
    // on top of closes that gap independent of any z-index.
    const tabWrapperIndex = panelSource.indexOf(
      'className="flex h-full flex-none items-center bg-white dark:bg-slate-900"',
    );
    expect(tabWrapperIndex).toBeGreaterThan(-1);
    const callSite = panelSource.slice(tabWrapperIndex, tabWrapperIndex + 300);
    expect(callSite).toContain("style={{ width: TAB_WIDTH_PX }}");
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

describe("drag-to-resize (#1784)", () => {
  it("seeds width from persisted storage and reads/writes through aircraftListPanelPersistence.ts", () => {
    expect(panelSource).toContain(
      'import {\n  clampPanelWidth,\n  loadPersistedPanelWidth,\n  savePersistedPanelWidth,\n} from "../lib/aircraftListPanelPersistence"',
    );
    expect(panelSource).toContain("useState(() => loadPersistedPanelWidth())");
    expect(panelSource).toContain("savePersistedPanelWidth(widthRef.current)");
  });

  it("only renders the resize handle while open", () => {
    const handleIndex = panelSource.indexOf('aria-label="Resize aircraft list"');
    expect(handleIndex).toBeGreaterThan(-1);
    const callSite = panelSource.slice(Math.max(0, handleIndex - 150), handleIndex);
    expect(callSite).toContain("{open && (");
  });

  it("wires the handle to pointer down/move/up/cancel, using pointer capture", () => {
    expect(panelSource).toContain("onPointerDown={handleResizeStart}");
    expect(panelSource).toContain("onPointerMove={handleResizeMove}");
    expect(panelSource).toContain("onPointerUp={handleResizeEnd}");
    expect(panelSource).toContain("onPointerCancel={handleResizeEnd}");
    expect(panelSource).toContain("e.currentTarget.setPointerCapture(e.pointerId)");
    expect(panelSource).toContain("e.currentTarget.releasePointerCapture(e.pointerId)");
  });

  it("computes the drag delta as startWidth + (startX - currentX), clamped", () => {
    expect(panelSource).toContain(
      "dragStartRef.current.startWidth + (dragStartRef.current.startX - e.clientX)",
    );
    expect(panelSource).toContain("clampPanelWidth(Math.min(raw, viewportMax))");
  });

  it("adds a viewport-aware ceiling so the map area keeps a minimum width during a drag", () => {
    expect(panelSource).toContain("const MIN_MAP_AREA_WIDTH_PX = 320;");
    expect(panelSource).toContain("window.innerWidth - TAB_WIDTH_PX - MIN_MAP_AREA_WIDTH_PX");
  });

  it("is a proper ARIA vertical separator, not an unlabeled div", () => {
    const handleIndex = panelSource.indexOf('aria-label="Resize aircraft list"');
    const callSite = panelSource.slice(handleIndex - 100, handleIndex + 100);
    expect(callSite).toContain('role="separator"');
    expect(callSite).toContain('aria-orientation="vertical"');
  });

  it("scrolls horizontally rather than overflowing when dragged narrower than the table's content", () => {
    expect(panelSource).toContain('className="overflow-auto"');
    expect(panelSource).not.toContain('className="overflow-y-auto"');
  });
});
