import { describe, expect, it } from "vitest";
import { BADGE_BASE, BADGE_CLASSES, PILL, PILL_GREEN, PILL_RED, SQUAWK_EMERGENCY_TEXT, TAG_PILL } from "./AircraftDetailPanel";

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
