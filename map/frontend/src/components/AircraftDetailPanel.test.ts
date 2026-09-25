import { describe, expect, it } from "vitest";
import { BADGE_BASE, BADGE_CLASSES, PILL, PILL_GREEN, PILL_RED, SQUAWK_EMERGENCY_TEXT, TAG_PILL } from "./AircraftDetailPanel";
// Vite's `?raw` suffix (see MapView.test.ts's own use of this) -- the action
// row is built from four adjacent <ActionButton> JSX call sites, which is
// easiest to check for order/props by reading the actual source text rather
// than rendering (no jsdom in this project -- see lib/config.test.ts).
import panelSource from "./AircraftDetailPanel.tsx?raw";

// This project has no jsdom/component-render test setup (see
// lib/config.test.ts's own note on the constraint, and MapView.test.ts's
// raw-source-extraction approach for a comparable gap). Rather than render
// the panel, these assert the exact Tailwind class strings this component
// exports are byte-for-byte identical to the management-ui source they were
// copied from -- the thing the issue's acceptance criteria actually cares
// about ("verified identical ... not just visually similar"), and the one
// thing a render test wouldn't check any more rigorously than a plain
// string comparison would.

describe("Badge classes -- copied verbatim from management-ui/frontend/src/views/LookupView.tsx", () => {
  it("matches BADGE_CLASSES.green (Military) and its base classes exactly", () => {
    expect(BADGE_BASE).toBe("rounded px-2 py-0.5 text-xs font-semibold");
    expect(BADGE_CLASSES.green).toBe("bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200");
  });

  it("matches BADGE_CLASSES.yellow (Special Livery) exactly", () => {
    expect(BADGE_CLASSES.yellow).toBe("bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200");
  });

  it("matches BADGE_CLASSES.blue (UAT/External, #1901) exactly", () => {
    expect(BADGE_CLASSES.blue).toBe("bg-sky-100 text-sky-800 dark:bg-sky-900 dark:text-sky-200");
  });
});

describe("Route pill classes -- copied verbatim from management-ui/frontend/src/components/FlightViewModal.tsx", () => {
  it("matches PILL, PILL_GREEN, and PILL_RED exactly", () => {
    expect(PILL).toBe("rounded px-2 py-0.5 text-xs font-semibold");
    expect(PILL_GREEN).toBe("bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200");
    expect(PILL_RED).toBe("bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200");
  });

  it("derives the Squawk emergency-code text color from PILL_RED's own text classes", () => {
    for (const cls of SQUAWK_EMERGENCY_TEXT.split(" ")) {
      expect(PILL_RED).toContain(cls);
    }
  });
});

describe("Matched Rules / Receiver Sources pill class -- copied verbatim from FlightViewModal.tsx", () => {
  it("matches exactly", () => {
    expect(TAG_PILL).toBe(
      "rounded bg-slate-100 px-2 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-900 dark:text-slate-300",
    );
  });
});

describe("Sources / Matched Rules label -- shares the first line with pills (#1793)", () => {
  it("wraps the Sources label and pills in one flex-wrap row instead of stacking them in separate divs", () => {
    const sourcesIndex = panelSource.indexOf("data.sources.length > 0");
    const matchedRulesIndex = panelSource.indexOf("data.matchedRules.length > 0");
    const block = panelSource.slice(sourcesIndex, matchedRulesIndex);
    expect(block).toContain("flex flex-wrap items-baseline justify-between gap-3 px-4 py-2");
    expect(block).toContain("<span className={ROW_LABEL}>Sources</span>");
    // No longer a standalone label div forced onto its own line.
    expect(block).not.toContain("mb-1");
  });

  it("gives Matched Rules the identical treatment", () => {
    const matchedRulesIndex = panelSource.indexOf("data.matchedRules.length > 0");
    const block = panelSource.slice(matchedRulesIndex, matchedRulesIndex + 400);
    expect(block).toContain("flex flex-wrap items-baseline justify-between gap-3 px-4 py-2");
    expect(block).toContain("<span className={ROW_LABEL}>Matched Rules</span>");
    expect(block).not.toContain("mb-1");
  });
});

describe("country-of-registration flag -- header subline, same row as ICAO hex (#1891)", () => {
  it("imports countryFlag and renders it from data.countryCode, omitted (not a placeholder) when null", () => {
    expect(panelSource).toContain('import { countryFlag } from "../lib/countryFlag"');
    expect(panelSource).toContain("data.countryCode != null ? countryFlag(data.countryCode) : null");
  });

  it("places the flag span immediately after the icaoHex span, using the country/countryCode tooltip", () => {
    const icaoHexIndex = panelSource.indexOf("data.icaoHex != null &&");
    const nextRowIndex = panelSource.indexOf("</div>", icaoHexIndex);
    const block = panelSource.slice(icaoHexIndex, nextRowIndex);
    expect(block).toContain(
      "{flag != null && <span title={data.country ?? data.countryCode ?? undefined}>{flag}</span>}",
    );
  });
});

describe("action row -- Isolate/Zoom To/Follow/Trace Points buttons", () => {
  it("renders all four buttons in order: Isolate, Zoom To, Follow, Trace Points", () => {
    const labels = ["Isolate", "Zoom To", "Follow", "Trace Points"];
    const indices = labels.map((label) => panelSource.indexOf(`label="${label}"`));
    for (const index of indices) expect(index).toBeGreaterThan(-1);
    // Strictly increasing -- proves the ordering, not just presence.
    for (let i = 1; i < indices.length; i++) {
      expect(indices[i]).toBeGreaterThan(indices[i - 1]);
    }
  });

  it("wires each button to its own icon spec", () => {
    expect(panelSource).toContain('icon={ISOLATE_ICON}');
    expect(panelSource).toContain('icon={ZOOM_TO_ICON}');
    expect(panelSource).toContain('icon={FOLLOW_ICON}');
    expect(panelSource).toContain('icon={TRACE_POINTS_ICON}');
  });

  it("Isolate, Follow, and Trace Points are real toggles (active prop tracks panel state)", () => {
    expect(panelSource).toContain("active={isolateActive}");
    expect(panelSource).toContain("active={followActive}");
    expect(panelSource).toContain("active={tracePointsActive}");
  });

  it("Zoom To is one-shot -- always active={false}, never an 'active' state", () => {
    const zoomToCallIndex = panelSource.indexOf('label="Zoom To"');
    const callSite = panelSource.slice(zoomToCallIndex, zoomToCallIndex + 200);
    expect(callSite).toContain("active={false}");
  });

  it("renders each button via the shared IconButton component", () => {
    expect(panelSource).toContain('import { IconButton } from "./IconButton"');
    expect(panelSource).toContain("<IconButton");
  });
});

// The icon-rendering mechanism itself (ActionIcon's svg shape, the
// toggleButtonClass coloring) is shared with ControlsPanel's toggle row
// and lives in, and is tested by, IconButton.test.ts -- see that file's
// own note.

describe("panel size cap and scroll (#2003)", () => {
  it("caps width at 20vw and height at 80vh on the outer panel div", () => {
    const outerDivIndex = panelSource.indexOf("<div\n      className=\"absolute top-4 left-4");
    expect(outerDivIndex).toBeGreaterThan(-1);
    const outerDivClassName = panelSource.slice(outerDivIndex, panelSource.indexOf("\"", outerDivIndex + 40) + 1);
    expect(outerDivClassName).toContain("max-w-[20vw]");
    expect(outerDivClassName).toContain("max-h-[80vh]");
  });

  it("keeps w-80 as the preferred (pre-cap) width rather than dropping it", () => {
    // w-80 only binds below a ~1600px viewport where max-w-[20vw] takes
    // over -- above that, the panel should look identical to before #2003.
    const outerDivIndex = panelSource.indexOf("<div\n      className=\"absolute top-4 left-4");
    const outerDivClassName = panelSource.slice(outerDivIndex, panelSource.indexOf("\"", outerDivIndex + 40) + 1);
    expect(outerDivClassName).toContain("w-80");
  });

  it("scrolls overflowing content on the panel itself instead of clipping it (overflow-hidden removed)", () => {
    const outerDivIndex = panelSource.indexOf("<div\n      className=\"absolute top-4 left-4");
    const outerDivClassName = panelSource.slice(outerDivIndex, panelSource.indexOf("\"", outerDivIndex + 40) + 1);
    expect(outerDivClassName).toContain("overflow-y-auto");
    expect(outerDivClassName).not.toContain("overflow-hidden");
  });

  it("does not add a competing min-width floor that would fight the 20vw cap", () => {
    // A min-width wide enough to matter on a narrow viewport would win over
    // max-width in a conflict, breaking the "never exceeds 20vw" guarantee
    // -- see the rationale comment above the outer div.
    expect(panelSource).not.toMatch(/min-w-(?!0\b)\S/);
  });
});
