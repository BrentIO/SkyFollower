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
