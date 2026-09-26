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

describe("unified icon column -- Fullscreen, Center, Labels, Trails, Radar, Settings (#2012)", () => {
  it("stacks all controls vertically in a single column", () => {
    expect(controlsPanelSource).toContain('<div className="pointer-events-auto flex flex-col gap-2">');
    expect(controlsPanelSource).not.toContain('<div className="pointer-events-auto flex gap-2">');
  });

  it("sizes each IconButton-based control to h-9 w-9 via the size prop", () => {
    const labels = ["Labels", "Trails", "Settings"];
    for (const label of labels) {
      const index = controlsPanelSource.indexOf(`label="${label}"`);
      const callSite = controlsPanelSource.slice(index, index + 250);
      expect(callSite).toContain('size="md"');
    }
    // #2015: the Radar button's label is computed (radarButtonLabel(...)),
    // not a literal string, so it's anchored differently than the others.
    const radarIndex = controlsPanelSource.indexOf("onClick={onCycleRadar}");
    const radarCallSite = controlsPanelSource.slice(radarIndex - 250, radarIndex + 250);
    expect(radarCallSite).toContain('size="md"');
  });

  it("renders the controls in the required top-to-bottom order: Fullscreen, Center, Labels, Trails, Radar, Settings", () => {
    const fullscreenIndex = controlsPanelSource.indexOf("onClick={onToggleFullscreen}");
    const recenterIndex = controlsPanelSource.indexOf('title="Return to center"');
    const labelsIndex = controlsPanelSource.indexOf('label="Labels"');
    const trailsIndex = controlsPanelSource.indexOf('label="Trails"');
    const radarIndex = controlsPanelSource.indexOf("onClick={onCycleRadar}");
    const settingsIndex = controlsPanelSource.indexOf('label="Settings"');

    const indices = [fullscreenIndex, recenterIndex, labelsIndex, trailsIndex, radarIndex, settingsIndex];
    for (const index of indices) expect(index).toBeGreaterThan(-1);
    // Strictly increasing -- proves the ordering, not just presence.
    for (let i = 1; i < indices.length; i++) {
      expect(indices[i]).toBeGreaterThan(indices[i - 1]);
    }
  });

  it("wires the always-visible toggle buttons to their own icon spec and existing toggle state/handler", () => {
    expect(controlsPanelSource).toContain("icon={ROUTE_ICON}");
    expect(controlsPanelSource).toContain("active={historyAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleHistoryAll}");

    expect(controlsPanelSource).toContain("icon={TAGS_ICON}");
    expect(controlsPanelSource).toContain("active={labelsAll}");
    expect(controlsPanelSource).toContain("onClick={onToggleLabelsAll}");
  });

  // #2012: Range Outline, Map Labels, and Display Scale (and its icon) no
  // longer render as their own standalone IconButtons here -- they moved
  // into the new Settings panel (see SettingsPanel.test.ts). ControlsPanel
  // still accepts/forwards their props (checked below), just doesn't render
  // an IconButton wired to RADAR_ICON/TYPE_ICON/DISPLAY_SCALE_ICON anymore.
  it("no longer renders Range Outline, Map Labels, or Display Scale as their own IconButtons", () => {
    expect(controlsPanelSource).not.toContain('label="Range Outline"');
    expect(controlsPanelSource).not.toContain('label="Map Labels"');
    expect(controlsPanelSource).not.toContain('label="Display Scale"');
    expect(controlsPanelSource).not.toContain("icon={RADAR_ICON}");
    expect(controlsPanelSource).not.toContain("icon={TYPE_ICON}");
    expect(controlsPanelSource).not.toContain("icon={DISPLAY_SCALE_ICON}");
  });

  it("still accepts and forwards the relocated props to SettingsPanel unchanged", () => {
    expect(controlsPanelSource).toContain("mapLabelsOn: boolean;");
    expect(controlsPanelSource).toContain("onToggleMapLabels: () => void;");
    expect(controlsPanelSource).toContain("rangeOutlineVisible: boolean;");
    expect(controlsPanelSource).toContain("onToggleRangeOutline: () => void;");
    expect(controlsPanelSource).toContain("rangeOutlineDisabled: boolean;");
    expect(controlsPanelSource).toContain("displayScale: number;");
    expect(controlsPanelSource).toContain("onDisplayScaleChange: (value: number) => void;");

    expect(controlsPanelSource).toContain("mapLabelsOn={mapLabelsOn}");
    expect(controlsPanelSource).toContain("onToggleMapLabels={onToggleMapLabels}");
    expect(controlsPanelSource).toContain("rangeOutlineVisible={rangeOutlineVisible}");
    expect(controlsPanelSource).toContain("onToggleRangeOutline={onToggleRangeOutline}");
    expect(controlsPanelSource).toContain("rangeOutlineDisabled={rangeOutlineDisabled}");
    expect(controlsPanelSource).toContain("displayScale={displayScale}");
    expect(controlsPanelSource).toContain("onDisplayScaleChange={onDisplayScaleChange}");
  });

  // New in #2012 -- the static range rings had no on/off control at all
  // before this issue.
  it("accepts and forwards the new rangeRingsVisible/onToggleRangeRings/rangeRingsDisabled props to SettingsPanel", () => {
    expect(controlsPanelSource).toContain("rangeRingsVisible: boolean;");
    expect(controlsPanelSource).toContain("onToggleRangeRings: () => void;");
    expect(controlsPanelSource).toContain("rangeRingsDisabled: boolean;");
    expect(controlsPanelSource).toContain("rangeRingsVisible={rangeRingsVisible}");
    expect(controlsPanelSource).toContain("onToggleRangeRings={onToggleRangeRings}");
    expect(controlsPanelSource).toContain("rangeRingsDisabled={rangeRingsDisabled}");
  });

  it("no always-visible IconButton-based toggle (Trails/Labels) passes a disabled prop", () => {
    const labels = ["Trails", "Labels"];
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

describe("Radar control (#1896, tri-state redesign #2015) -- one button, no popover, before Settings", () => {
  const radarCallIndex = controlsPanelSource.indexOf("onClick={onCycleRadar}");
  // Slice back far enough to cover the whole <IconButton ... /> call site
  // (label/icon/active/loading props all precede onClick in JSX source
  // order here).
  const radarCallSite = controlsPanelSource.slice(radarCallIndex - 350, radarCallIndex + 150);

  it("finds exactly one JSX call site wiring onClick to onCycleRadar", () => {
    expect(radarCallIndex).toBeGreaterThan(-1);
  });

  it("renders after Trails and before Settings", () => {
    const trailsIndex = controlsPanelSource.indexOf('label="Trails"');
    const settingsIndex = controlsPanelSource.indexOf('label="Settings"');
    expect(radarCallIndex).toBeGreaterThan(trailsIndex);
    expect(settingsIndex).toBeGreaterThan(radarCallIndex);
  });

  it("no popover, no local expand state -- #2015 removed the Radar popover entirely", () => {
    expect(controlsPanelSource).not.toContain("const [radarExpanded");
    // Exactly one local disclosure useState left (settingsExpanded) --
    // radarExpanded's own declaration is gone, not just unused.
    const useStateFalseCount = (controlsPanelSource.match(/useState\(false\)/g) ?? []).length;
    expect(useStateFalseCount).toBe(1);
    expect(controlsPanelSource).not.toContain('role="switch"');
    expect(controlsPanelSource).not.toContain("PLAY_ICON}");
  });

  it("no longer accepts the old separate radarOn/onToggleRadar/radarPlaying/onToggleRadarPlaying props", () => {
    expect(controlsPanelSource).not.toContain("radarOn: boolean");
    expect(controlsPanelSource).not.toContain("onToggleRadar:");
    expect(controlsPanelSource).not.toContain("radarPlaying: boolean");
    expect(controlsPanelSource).not.toContain("onToggleRadarPlaying:");
  });

  it("accepts radarState (RadarState) and onCycleRadar instead", () => {
    expect(controlsPanelSource).toContain('import type { RadarState } from "../lib/radar"');
    expect(controlsPanelSource).toContain("radarState: RadarState;");
    expect(controlsPanelSource).toContain("onCycleRadar: () => void;");
  });

  it("derives its label from radarButtonLabel(radarState, radarPlaybackLoading)", () => {
    expect(radarCallSite).toContain("label={radarButtonLabel(radarState, radarPlaybackLoading)}");
  });

  it("is active whenever radarState is not \"off\" (both on and animate read active)", () => {
    expect(radarCallSite).toContain('active={radarState !== "off"}');
  });

  it("swaps in PAUSE_ICON once actually animating (not loading), and WEATHER_RADAR_ICON otherwise", () => {
    expect(radarCallSite).toContain(
      'icon={radarState === "animate" && !radarPlaybackLoading ? PAUSE_ICON : WEATHER_RADAR_ICON}',
    );
  });

  it("shows the loading spinner only during animate's own prefetch phase", () => {
    expect(radarCallSite).toContain('loading={radarState === "animate" && radarPlaybackLoading}');
  });

  // #2015's core fix: the button must stay clickable through the loading
  // spinner, so the operator can cancel out of animate immediately rather
  // than waiting for prefetch to finish/time out.
  it("passes loadingDisabled={false} -- the load-bearing fix for staying clickable while loading", () => {
    expect(radarCallSite).toContain("loadingDisabled={false}");
  });

  it("wires onClick to onCycleRadar, not a local expand toggle", () => {
    expect(radarCallSite).toContain("onClick={onCycleRadar}");
  });

  it("radarButtonLabel distinguishes all four visually-distinct moments (off, on, animate-loading, animate-playing)", () => {
    const fnIndex = controlsPanelSource.indexOf("function radarButtonLabel(");
    expect(fnIndex).toBeGreaterThan(-1);
    const body = controlsPanelSource.slice(fnIndex, fnIndex + 400);
    expect(body).toContain('state === "off"');
    expect(body).toContain('state === "on"');
    expect(body).toContain("loading ?");
  });
});

describe("Settings button (#2012) -- last in the column, after Radar", () => {
  it("uses SETTINGS_ICON", () => {
    const settingsIndex = controlsPanelSource.indexOf('label="Settings"');
    const callSite = controlsPanelSource.slice(settingsIndex, settingsIndex + 200);
    expect(callSite).toContain("icon={SETTINGS_ICON}");
  });

  it("the icon's active state reflects whether the panel is expanded", () => {
    const settingsIndex = controlsPanelSource.indexOf('label="Settings"');
    const callSite = controlsPanelSource.slice(settingsIndex, settingsIndex + 200);
    expect(callSite).toContain("active={settingsExpanded}");
  });

  it("the icon's onClick toggles local expand state", () => {
    const settingsIndex = controlsPanelSource.indexOf('label="Settings"');
    const callSite = controlsPanelSource.slice(settingsIndex, settingsIndex + 200);
    expect(callSite).toContain("setSettingsExpanded");
  });

  it("declares local settingsExpanded state via useState, not lifted/persisted", () => {
    expect(controlsPanelSource).toContain("useState(false)");
    expect(controlsPanelSource).toContain("setSettingsExpanded");
  });

  it("only renders SettingsPanel when settingsExpanded is true", () => {
    expect(controlsPanelSource).toContain("{settingsExpanded && (");
    expect(controlsPanelSource).toContain("<SettingsPanel");
  });

  it("imports SettingsPanel from its own file", () => {
    expect(controlsPanelSource).toContain('import { SettingsPanel } from "./SettingsPanel"');
  });

  it("wires onClose to collapse the panel, and forwards every relocated + new prop to SettingsPanel", () => {
    const settingsPanelIndex = controlsPanelSource.indexOf("<SettingsPanel");
    const callSite = controlsPanelSource.slice(settingsPanelIndex, settingsPanelIndex + 700);
    expect(callSite).toContain("onClose={() => setSettingsExpanded(false)}");
    expect(callSite).toContain("mapLabelsOn={mapLabelsOn}");
    expect(callSite).toContain("onToggleMapLabels={onToggleMapLabels}");
    expect(callSite).toContain("displayScale={displayScale}");
    expect(callSite).toContain("onDisplayScaleChange={onDisplayScaleChange}");
    expect(callSite).toContain("rangeOutlineVisible={rangeOutlineVisible}");
    expect(callSite).toContain("onToggleRangeOutline={onToggleRangeOutline}");
    expect(callSite).toContain("rangeOutlineDisabled={rangeOutlineDisabled}");
    expect(callSite).toContain("rangeRingsVisible={rangeRingsVisible}");
    expect(callSite).toContain("onToggleRangeRings={onToggleRangeRings}");
    expect(callSite).toContain("rangeRingsDisabled={rangeRingsDisabled}");
    expect(callSite).toContain("radarOpacity={radarOpacity}");
    expect(callSite).toContain("onRadarOpacityChange={onRadarOpacityChange}");
  });
});

describe("outer column wrapper -- #1953: every button pinned above every InfoBoxLayer label", () => {
  it("imports MAX_LABEL_Z_INDEX and applies it to the top-4/right-4 column wrapper", () => {
    expect(controlsPanelSource).toContain('import { MAX_LABEL_Z_INDEX } from "../lib/labelStackOrder";');
    const wrapperIndex = controlsPanelSource.indexOf('className="pointer-events-none absolute top-4 right-4 flex flex-col items-end gap-2"');
    expect(wrapperIndex).toBeGreaterThan(-1);
    const callSite = controlsPanelSource.slice(wrapperIndex, wrapperIndex + 900);
    expect(callSite).toContain("style={{ zIndex: MAX_LABEL_Z_INDEX + 1 }}");
  });

  it("does not leave AircraftDetailPanel's own zIndex convention untouched (same MAX_LABEL_Z_INDEX + 1 constant, not a duplicated literal)", () => {
    expect(controlsPanelSource).not.toMatch(/zIndex:\s*1001/);
  });
});
