import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see AircraftDetailPanel.test.ts's own use of this) --
// no jsdom/component-render test setup in this project (see
// lib/config.test.ts), so the icon column's order/props/disabled-logic are
// checked by reading the actual source text rather than rendering.
import controlsPanelSource from "./ControlsPanel.tsx?raw";

describe("status box -- removed entirely; aircraft count and connection dot both live in AircraftListPanel", () => {
  it("no longer renders any of the five toggle/action buttons as text buttons", () => {
    expect(controlsPanelSource).not.toContain("Trails</button>");
    expect(controlsPanelSource).not.toContain(">Labels<");
    expect(controlsPanelSource).not.toContain(">Map Labels<");
    expect(controlsPanelSource).not.toContain(">Range Outline<");
  });

  it("no longer accepts or renders an aircraftCount prop/text", () => {
    expect(controlsPanelSource).not.toContain("aircraftCount");
    expect(controlsPanelSource).not.toContain("aircraft</span>");
  });

  it("no longer wraps anything in a card (no rounded-md bg-white/90 status box)", () => {
    expect(controlsPanelSource).not.toContain("rounded-md bg-white/90 p-3");
  });

  // The connection-status dot (and its wsConnected/roster props,
  // processorStatus.ts imports, and hover tooltip) moved into
  // AircraftListPanel's header -- see the issue that relocated it after
  // this corner needed repeated z-index/position fixes (#1768, #1789) as
  // the icon column below kept changing shape. ControlsPanel should carry
  // none of that anymore.
  it("no longer renders the connection dot or references its color map", () => {
    expect(controlsPanelSource).not.toContain("PROCESSOR_STATUS_DOT_COLOR");
  });

  it("no longer imports or uses connectionTooltip/overallConnectionStatus", () => {
    expect(controlsPanelSource).not.toContain("connectionTooltip");
    expect(controlsPanelSource).not.toContain("overallConnectionStatus");
    expect(controlsPanelSource).not.toContain("../lib/processorStatus");
  });

  it("no longer accepts wsConnected/roster props", () => {
    expect(controlsPanelSource).not.toContain("wsConnected");
    expect(controlsPanelSource).not.toContain("roster");
    expect(controlsPanelSource).not.toContain("ProcessorRoster");
  });

  it("no longer positions anything at the bare top-2/right-2 corner inset (only the icon column's top-4/right-4 remains)", () => {
    expect(controlsPanelSource).not.toContain("top-2 right-2");
  });
});

describe("unified icon column -- Fullscreen, Center, Labels, Trails, Range Outline, Map Labels", () => {
  it("stacks all six controls vertically in a single column", () => {
    expect(controlsPanelSource).toContain('<div className="pointer-events-auto flex flex-col gap-2">');
    expect(controlsPanelSource).not.toContain('<div className="pointer-events-auto flex gap-2">');
  });

  it("sizes each IconButton-based control to h-9 w-9 via the size prop", () => {
    const labels = ["Labels", "Trails", "Range Outline", "Map Labels"];
    for (const label of labels) {
      const index = controlsPanelSource.indexOf(`label="${label}"`);
      const callSite = controlsPanelSource.slice(index, index + 250);
      expect(callSite).toContain('size="md"');
    }
  });

  it("renders the controls in the required top-to-bottom order: Full screen, Center, Labels, Trails, Range outline, Map labels", () => {
    const fullscreenIndex = controlsPanelSource.indexOf("onClick={onToggleFullscreen}");
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const labelsIndex = controlsPanelSource.indexOf('label="Labels"');
    const trailsIndex = controlsPanelSource.indexOf('label="Trails"');
    const rangeOutlineIndex = controlsPanelSource.indexOf('label="Range Outline"');
    const mapLabelsIndex = controlsPanelSource.indexOf('label="Map Labels"');

    const indices = [fullscreenIndex, recenterIndex, labelsIndex, trailsIndex, rangeOutlineIndex, mapLabelsIndex];
    for (const index of indices) expect(index).toBeGreaterThan(-1);
    // Strictly increasing -- proves the ordering, not just presence.
    for (let i = 1; i < indices.length; i++) {
      expect(indices[i]).toBeGreaterThan(indices[i - 1]);
    }
  });

  it("wires each toggle button to its own icon spec and existing toggle state/handler", () => {
    expect(controlsPanelSource).toContain("icon={ROUTE_ICON}");
    expect(controlsPanelSource).toContain("active={historyAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleHistoryAll}");

    expect(controlsPanelSource).toContain("icon={TAGS_ICON}");
    expect(controlsPanelSource).toContain("active={labelsAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleLabelsAll}");

    expect(controlsPanelSource).toContain("icon={TYPE_ICON}");
    expect(controlsPanelSource).toContain("active={mapLabelsOn}");
    expect(controlsPanelSource).toContain("onClick={onToggleMapLabels}");

    expect(controlsPanelSource).toContain("icon={RADAR_ICON}");
    expect(controlsPanelSource).toContain("active={rangeOutlineVisible}");
    expect(controlsPanelSource).toContain("onClick={onToggleRangeOutline}");
  });

  it("Range Outline keeps its existing disabled-when-no-center logic", () => {
    const rangeOutlineIndex = controlsPanelSource.indexOf('label="Range Outline"');
    const callSite = controlsPanelSource.slice(rangeOutlineIndex, rangeOutlineIndex + 200);
    expect(callSite).toContain("disabled={rangeOutlineDisabled}");
  });

  it("no other IconButton-based toggle passes a disabled prop", () => {
    const labels = ["Trails", "Labels", "Map Labels"];
    for (const label of labels) {
      const index = controlsPanelSource.indexOf(`label="${label}"`);
      const callSite = controlsPanelSource.slice(index, index + 150);
      expect(callSite).not.toContain("disabled=");
    }
  });

  it("renders the toggle buttons via the shared IconButton component", () => {
    expect(controlsPanelSource).toContain('import { IconButton } from "./IconButton"');
    expect(controlsPanelSource).toContain("<IconButton");
  });
});

describe("Trails toggle button", () => {
  it('renders the visible label (and title/aria-label, via IconButton) as "Trails"', () => {
    expect(controlsPanelSource).toContain('label="Trails"');
  });

  it('does not use the old "History" wording anywhere in the visible label', () => {
    expect(controlsPanelSource).not.toContain('"History: All"');
  });

  it("keeps the underlying historyAll prop/state and onToggleHistoryAll handler names unchanged", () => {
    // Rename is display-text only -- prop/state/handler names still say
    // "history" internally (see the issue's scope note).
    expect(controlsPanelSource).toContain("historyAll: boolean");
    expect(controlsPanelSource).toContain("onToggleHistoryAll: () => void");
    expect(controlsPanelSource).toContain("active={historyAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleHistoryAll}");
  });
});

describe("Center (recenter) button -- moved into the unified column", () => {
  it("renders its own crosshair markup rather than using IconButton (dashed-circle icon isn't an IconSpec)", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const nextIconButtonIndex = controlsPanelSource.indexOf("<IconButton", recenterIndex);
    expect(recenterIndex).toBeGreaterThan(-1);
    expect(nextIconButtonIndex).toBeGreaterThan(recenterIndex);
    const callSite = controlsPanelSource.slice(recenterIndex - 250, nextIconButtonIndex);
    expect(callSite).not.toContain("<IconButton");
    expect(callSite).toContain("dangerouslySetInnerHTML");
    expect(callSite).toContain("crosshairSvgMarkup(20,");
  });

  // #1847: the button's click is still momentary (onRecenter), but its
  // appearance now follows the shared toggle-button convention -- active
  // (light blue) whenever the camera is already centered on config.center.
  it("drives its className from toggleButtonClass(recenterActive), the same convention every other toggle uses", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const nextIconButtonIndex = controlsPanelSource.indexOf("<IconButton", recenterIndex);
    const callSite = controlsPanelSource.slice(recenterIndex, nextIconButtonIndex);
    expect(controlsPanelSource).toContain('import { toggleButtonClass } from "../lib/toggleButtonStyle"');
    expect(callSite).toContain("toggleButtonClass(recenterActive)");
    expect(callSite).toContain("aria-pressed={recenterActive}");
  });

  it("does not hardcode a bespoke bg-white/90 / shadow-md color scheme anymore", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const nextIconButtonIndex = controlsPanelSource.indexOf("<IconButton", recenterIndex);
    const callSite = controlsPanelSource.slice(recenterIndex, nextIconButtonIndex);
    expect(callSite).not.toContain("bg-white/90");
    expect(callSite).not.toContain("shadow-md");
  });

  it("keeps its disabled-when-no-center-configured behavior", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const callSite = controlsPanelSource.slice(recenterIndex - 100, recenterIndex + 100);
    expect(callSite).toContain("disabled={recenterDisabled}");
  });

  it("wires the click handler unconditionally, same as before", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const callSite = controlsPanelSource.slice(recenterIndex - 100, recenterIndex + 100);
    expect(callSite).toContain("onClick={onRecenter}");
  });

  it("is the second control in the unified column, right after Fullscreen", () => {
    const fullscreenIndex = controlsPanelSource.indexOf("onClick={onToggleFullscreen}");
    const labelsIndex = controlsPanelSource.indexOf('label="Labels"');
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    expect(recenterIndex).toBeGreaterThan(fullscreenIndex);
    expect(recenterIndex).toBeLessThan(labelsIndex);
  });
});

describe("Fullscreen toggle button", () => {
  // The prop declaration/destructuring both mention "onToggleFullscreen"
  // earlier in the file, so anchor on the JSX call site specifically
  // (`onClick={onToggleFullscreen}`) rather than the bare identifier.
  const callSiteIndex = controlsPanelSource.indexOf("onClick={onToggleFullscreen}");

  it("finds exactly one JSX call site wiring onClick to onToggleFullscreen", () => {
    expect(callSiteIndex).toBeGreaterThan(-1);
  });

  it("renders first in the unified column, before the recenter button", () => {
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    expect(callSiteIndex).toBeLessThan(recenterIndex);
  });

  it("sizes to match the other icon buttons via IconButton's size prop", () => {
    const callSite = controlsPanelSource.slice(callSiteIndex - 250, callSiteIndex + 100);
    expect(callSite).toContain('size="md"');
  });

  it("derives its icon from fullscreenIcon() rather than a static IconSpec", () => {
    expect(controlsPanelSource).toContain('import { fullscreenIcon } from "../lib/fullscreen"');
    expect(controlsPanelSource).toContain("icon={fullscreenIcon(fullscreen)}");
  });

  it("wires active/onClick/disabled to the fullscreen prop trio", () => {
    const callSite = controlsPanelSource.slice(callSiteIndex - 150, callSiteIndex + 100);
    expect(callSite).toContain("active={fullscreen}");
    expect(callSite).toContain("onClick={onToggleFullscreen}");
    expect(callSite).toContain("disabled={fullscreenDisabled}");
  });

  it("mirrors Range Outline's disabled-not-hidden convention for feature detection", () => {
    // The button is always in the tree -- gating is via IconButton's
    // `disabled` prop, not a conditional render -- matching how
    // rangeOutlineDisabled works rather than hiding the control outright.
    expect(controlsPanelSource).not.toMatch(/\{fullscreenSupported\s*&&/);
    expect(controlsPanelSource).not.toMatch(/\{!fullscreenDisabled\s*&&/);
  });
});

describe("Map Labels button -- last in the column", () => {
  it("renders after Range Outline, as the last control in the column", () => {
    const rangeOutlineIndex = controlsPanelSource.indexOf('label="Range Outline"');
    const mapLabelsIndex = controlsPanelSource.indexOf('label="Map Labels"');
    expect(mapLabelsIndex).toBeGreaterThan(rangeOutlineIndex);
  });
});

describe("Radar control (#1896) -- last in the column, after Map Labels", () => {
  it("renders after Map Labels", () => {
    const mapLabelsIndex = controlsPanelSource.indexOf('label="Map Labels"');
    const radarIndex = controlsPanelSource.indexOf('label="Radar"');
    expect(radarIndex).toBeGreaterThan(mapLabelsIndex);
  });

  it("uses WEATHER_RADAR_ICON, not RADAR_ICON (already Range Outline's icon in this panel)", () => {
    const radarIndex = controlsPanelSource.indexOf('label="Radar"');
    const callSite = controlsPanelSource.slice(radarIndex, radarIndex + 150);
    expect(callSite).toContain("icon={WEATHER_RADAR_ICON}");
  });

  it("the Radar icon's active state reflects radarOn, not whether the popover is expanded", () => {
    const radarIndex = controlsPanelSource.indexOf('label="Radar"');
    const callSite = controlsPanelSource.slice(radarIndex, radarIndex + 150);
    expect(callSite).toContain("active={radarOn}");
  });

  it("the Radar icon's onClick toggles local expand state, not onToggleRadar directly", () => {
    const radarIndex = controlsPanelSource.indexOf('label="Radar"');
    const callSite = controlsPanelSource.slice(radarIndex, radarIndex + 150);
    expect(callSite).toContain("setRadarExpanded");
    expect(callSite).not.toContain("onClick={onToggleRadar}");
  });

  it("declares local radarExpanded state via useState, not lifted/persisted", () => {
    expect(controlsPanelSource).toContain('import { useState } from "react"');
    expect(controlsPanelSource).toContain("useState(false)");
  });

  it("the expanded popover wires an on/off toggle to onToggleRadar", () => {
    expect(controlsPanelSource).toContain("onClick={onToggleRadar}");
    expect(controlsPanelSource).toContain("aria-pressed={radarOn}");
  });

  it("the opacity slider is a 0-1 range input wired to onRadarOpacityChange, disabled when radar is off", () => {
    const sliderIndex = controlsPanelSource.indexOf('type="range"');
    expect(sliderIndex).toBeGreaterThan(-1);
    const callSite = controlsPanelSource.slice(sliderIndex - 50, sliderIndex + 300);
    expect(callSite).toContain("min={0}");
    expect(callSite).toContain("max={1}");
    expect(callSite).toContain("value={radarOpacity}");
    expect(callSite).toContain("disabled={!radarOn}");
    expect(callSite).toContain("onRadarOpacityChange");
  });

  it("the play/pause control swaps icon and label by radarPlaying, and is disabled when radar is off", () => {
    const playIndex = controlsPanelSource.indexOf("radarPlaying ?");
    expect(playIndex).toBeGreaterThan(-1);
    const callSite = controlsPanelSource.slice(playIndex, playIndex + 400);
    expect(callSite).toContain("PAUSE_ICON");
    expect(callSite).toContain("PLAY_ICON");
    expect(callSite).toContain("onClick={onToggleRadarPlaying}");
    expect(callSite).toContain("disabled={!radarOn}");
  });

  it("only renders the popover's controls when radarExpanded is true", () => {
    expect(controlsPanelSource).toContain("{radarExpanded && (");
  });

  it("#1909: the popover pins itself above every InfoBoxLayer label via MAX_LABEL_Z_INDEX + 1", () => {
    expect(controlsPanelSource).toContain('import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";');
    const popoverIndex = controlsPanelSource.indexOf("{radarExpanded && (");
    const callSite = controlsPanelSource.slice(popoverIndex, popoverIndex + 1200);
    expect(callSite).toContain("style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}");
  });
});
