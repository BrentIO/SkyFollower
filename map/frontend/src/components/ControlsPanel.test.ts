import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see AircraftDetailPanel.test.ts's own use of this) --
// no jsdom/component-render test setup in this project (see
// lib/config.test.ts), so the toggle row's order/props/disabled-logic are
// checked by reading the actual source text rather than rendering.
import controlsPanelSource from "./ControlsPanel.tsx?raw";

describe("status box -- connection dot + aircraft count only", () => {
  it("no longer renders any of the four toggle buttons as text buttons", () => {
    expect(controlsPanelSource).not.toContain("History: All</button>");
    expect(controlsPanelSource).not.toContain(">Labels: All<");
    expect(controlsPanelSource).not.toContain(">Map Labels<");
    expect(controlsPanelSource).not.toContain(">Range Outline<");
  });

  it("still renders the connection dot and aircraft count", () => {
    expect(controlsPanelSource).toContain("PROCESSOR_STATUS_DOT_COLOR[overallStatus]");
    expect(controlsPanelSource).toContain("{aircraftCount} aircraft");
  });
});

describe("icon-button row -- History, Labels, Map Labels, Range Outline", () => {
  it("renders all four buttons in order below the recenter button", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Recenter on home"');
    const labels = ["History: All", "Labels: All", "Map Labels", "Range Outline"];
    const indices = labels.map((label) => controlsPanelSource.indexOf(`label="${label}"`));
    for (const index of indices) expect(index).toBeGreaterThan(recenterIndex);
    // Strictly increasing -- proves the ordering, not just presence.
    for (let i = 1; i < indices.length; i++) {
      expect(indices[i]).toBeGreaterThan(indices[i - 1]);
    }
  });

  it("wires each button to its own icon spec and existing toggle state/handler", () => {
    expect(controlsPanelSource).toContain('icon={ROUTE_ICON}');
    expect(controlsPanelSource).toContain("active={historyAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleHistoryAll}");

    expect(controlsPanelSource).toContain('icon={TAGS_ICON}');
    expect(controlsPanelSource).toContain("active={labelsAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleLabelsAll}");

    expect(controlsPanelSource).toContain('icon={TYPE_ICON}');
    expect(controlsPanelSource).toContain("active={mapLabelsOn}");
    expect(controlsPanelSource).toContain("onClick={onToggleMapLabels}");

    expect(controlsPanelSource).toContain('icon={RADAR_ICON}');
    expect(controlsPanelSource).toContain("active={rangeOutlineVisible}");
    expect(controlsPanelSource).toContain("onClick={onToggleRangeOutline}");
  });

  it("Range Outline keeps its existing disabled-when-no-home/center logic", () => {
    const rangeOutlineIndex = controlsPanelSource.indexOf('label="Range Outline"');
    const callSite = controlsPanelSource.slice(rangeOutlineIndex, rangeOutlineIndex + 200);
    expect(callSite).toContain("disabled={rangeOutlineDisabled}");
  });

  it("no other button passes a disabled prop", () => {
    const labels = ["History: All", "Labels: All", "Map Labels"];
    for (const label of labels) {
      const index = controlsPanelSource.indexOf(`label="${label}"`);
      const callSite = controlsPanelSource.slice(index, index + 150);
      expect(callSite).not.toContain("disabled=");
    }
  });

  it("renders via the shared IconButton component", () => {
    expect(controlsPanelSource).toContain('import { IconButton } from "./IconButton"');
    expect(controlsPanelSource).toContain("<IconButton");
  });
});
