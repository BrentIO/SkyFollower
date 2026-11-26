import { describe, expect, it } from "vitest";
// Same ?raw source-text convention as SettingsPanel.test.ts.
import aircraftSource from "./AircraftDetailPanel.tsx?raw";
import panelSource from "./MetarDetailPanel.tsx?raw";

describe("MetarDetailPanel", () => {
  it("sits exactly where the aircraft card does", () => {
    const shell = /className="(absolute top-4 left-4 [^"]+)"/;
    expect(panelSource.match(shell)?.[1]).toBe(aircraftSource.match(shell)?.[1]);
    expect(panelSource).toContain("zIndex: MAX_LABEL_Z_INDEX + 1");
  });

  it("names the flight category on hover over the station code, not in a row", () => {
    expect(panelSource).toContain("title={category.label}");
    expect(panelSource).not.toContain("Flight Category");
  });

  it("renders outside text as text, never as HTML", () => {
    expect(panelSource).not.toContain("dangerouslySetInnerHTML");
  });

  it("has a close button", () => {
    expect(panelSource).toContain("onClick={onClose}");
    expect(panelSource).toContain('aria-label="Close"');
  });
});
