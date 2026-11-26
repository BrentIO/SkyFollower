import { describe, expect, it } from "vitest";
// Same ?raw source-text convention as ControlsPanel.test.ts/
// AircraftDetailPanel.test.ts -- no jsdom/component-render test setup in
// this project (see lib/config.test.ts).
import settingsPanelSource from "./SettingsPanel.tsx?raw";

describe("SettingsPanel (#2012) -- reuses AircraftDetailPanel's headed-section look", () => {
  it("imports SECTION_BAR/DIVIDER from AircraftDetailPanel rather than duplicating the classes", () => {
    expect(settingsPanelSource).toContain('import { DIVIDER, SECTION_BAR } from "./AircraftDetailPanel"');
  });

  it("is not a small ~192px popover -- uses a wider, taller, scrollable panel", () => {
    expect(settingsPanelSource).toContain("w-72");
    expect(settingsPanelSource).toContain("overflow-y-auto");
    expect(settingsPanelSource).not.toContain("w-48");
  });

  it("caps height against a real pixel margin, not a flat vh percentage (#2031)", () => {
    // A flat max-h-[NNvh] doesn't account for the panel's own top offset --
    // see ControlsPanel.test.ts's "anchors to the column's relative
    // wrapper" test for the other half of this fix.
    expect(settingsPanelSource).toContain("max-h-[calc(100vh-2rem)]");
    expect(settingsPanelSource).not.toMatch(/max-h-\[\d+vh\]/);
  });

  it("renders a close button wired to onClose", () => {
    expect(settingsPanelSource).toContain("onClick={onClose}");
    expect(settingsPanelSource).toContain('aria-label="Close"');
  });

  it("does not set its own zIndex -- relies on ControlsPanel's outer column wrapper (#1953)", () => {
    expect(settingsPanelSource).not.toContain("style={{ zIndex");
  });
});

describe("Map Labels section", () => {
  it("has a Map Labels section heading", () => {
    expect(settingsPanelSource).toContain(">Map Labels<");
  });

  it("renders mapLabelsOn as a phone-style toggle switch wired to onToggleMapLabels", () => {
    const sectionIndex = settingsPanelSource.indexOf(">Map Labels<");
    const callSite = settingsPanelSource.slice(sectionIndex, sectionIndex + 300);
    expect(callSite).toContain("checked={mapLabelsOn}");
    expect(callSite).toContain("onChange={onToggleMapLabels}");
  });
});

describe("Text & Icon Size section (#2000, relabeled)", () => {
  it("has a Text & Icon Size section heading", () => {
    expect(settingsPanelSource).toContain("Text &amp; Icon Size");
  });

  it("no longer uses the old 'Display Scale' wording as a rendered heading/label", () => {
    expect(settingsPanelSource).not.toContain(">Display Scale<");
    expect(settingsPanelSource).not.toContain("<span>Display Scale");
  });

  it("the slider spans 0.5-1.5 in 0.05 steps and is wired to onDisplayScaleChange, same as the popover it replaces", () => {
    const sectionIndex = settingsPanelSource.indexOf("Text &amp; Icon Size");
    const sliderIndex = settingsPanelSource.indexOf('type="range"', sectionIndex);
    expect(sliderIndex).toBeGreaterThan(-1);
    const callSite = settingsPanelSource.slice(sliderIndex - 50, sliderIndex + 300);
    expect(callSite).toContain("min={0.5}");
    expect(callSite).toContain("max={1.5}");
    expect(callSite).toContain("step={0.05}");
    expect(callSite).toContain("value={displayScale}");
    expect(callSite).toContain("onDisplayScaleChange");
  });

  it("shows the percentage right-aligned in the muted color, not embedded in the label text", () => {
    const sectionIndex = settingsPanelSource.indexOf("Text &amp; Icon Size</div>");
    const sliderIndex = settingsPanelSource.indexOf('type="range"', sectionIndex);
    const block = settingsPanelSource.slice(sectionIndex, sliderIndex);
    expect(block).not.toContain("Text &amp; Icon Size ({");
    expect(block).toContain("justify-between");
    expect(block).toContain(
      '<span className="text-slate-500 dark:text-slate-400">{Math.round(displayScale * 100)}%</span>',
    );
  });

  it("is never disabled -- unlike Radar's opacity slider, there's no on/off state gating this control", () => {
    const sectionIndex = settingsPanelSource.indexOf("Text &amp; Icon Size");
    const sliderIndex = settingsPanelSource.indexOf('type="range"', sectionIndex);
    const callSite = settingsPanelSource.slice(sliderIndex, sliderIndex + 300);
    expect(callSite).not.toContain("disabled=");
  });
});

describe("Range section -- two independent toggles under one heading", () => {
  it("has a single Range section heading", () => {
    expect(settingsPanelSource).toContain(">Range<");
  });

  it("Reception Outline toggle is wired to rangeOutlineVisible/onToggleRangeOutline, disabled when rangeOutlineDisabled", () => {
    const rowIndex = settingsPanelSource.indexOf("Reception Outline");
    expect(rowIndex).toBeGreaterThan(-1);
    const callSite = settingsPanelSource.slice(rowIndex, rowIndex + 300);
    expect(callSite).toContain("checked={rangeOutlineVisible}");
    expect(callSite).toContain("onChange={onToggleRangeOutline}");
    expect(callSite).toContain("disabled={rangeOutlineDisabled}");
  });

  it("Distance Rings toggle is wired to rangeRingsVisible/onToggleRangeRings, disabled when rangeRingsDisabled", () => {
    const rowIndex = settingsPanelSource.indexOf("Distance Rings");
    expect(rowIndex).toBeGreaterThan(-1);
    const callSite = settingsPanelSource.slice(rowIndex, rowIndex + 300);
    expect(callSite).toContain("checked={rangeRingsVisible}");
    expect(callSite).toContain("onChange={onToggleRangeRings}");
    expect(callSite).toContain("disabled={rangeRingsDisabled}");
  });

  it("Reception Outline renders before Distance Rings", () => {
    const outlineIndex = settingsPanelSource.indexOf("Reception Outline");
    const ringsIndex = settingsPanelSource.indexOf("Distance Rings");
    expect(ringsIndex).toBeGreaterThan(outlineIndex);
  });
});

describe("Weather section -- radar opacity slider", () => {
  it("has a Weather section heading in place of Radar", () => {
    expect(settingsPanelSource).toContain(">Weather<");
    expect(settingsPanelSource).not.toContain(">Radar<");
  });

  it("the opacity slider is a 0-1 range input wired to onRadarOpacityChange, matching the popover it replaced", () => {
    const sectionIndex = settingsPanelSource.indexOf(">Weather<");
    const sliderIndex = settingsPanelSource.indexOf('type="range"', sectionIndex);
    expect(sliderIndex).toBeGreaterThan(-1);
    const callSite = settingsPanelSource.slice(sliderIndex - 50, sliderIndex + 300);
    expect(callSite).toContain("min={0}");
    expect(callSite).toContain("max={1}");
    expect(callSite).toContain("value={radarOpacity}");
    expect(callSite).toContain("onRadarOpacityChange");
  });

  it("shows the opacity as a rounded percentage, right-aligned and muted, on the label line", () => {
    const sectionIndex = settingsPanelSource.indexOf(">Weather<");
    const sliderIndex = settingsPanelSource.indexOf('type="range"', sectionIndex);
    const row = settingsPanelSource.slice(sectionIndex, sliderIndex);
    expect(row).toContain("justify-between");
    expect(row).toContain("<span>Radar Opacity</span>");
    expect(row).toContain("text-slate-500 dark:text-slate-400");
    expect(row).toContain("Math.round(radarOpacity * 100)");
  });

  it("has no radar on/off switch or Play/Pause button -- radar stays on its own icon button", () => {
    expect(settingsPanelSource).not.toContain('feature="radar"');
    expect(settingsPanelSource).not.toContain("PLAY_ICON");
    expect(settingsPanelSource).not.toContain("PAUSE_ICON");
  });

  it("toggles METARs, disabled without a center", () => {
    const rowIndex = settingsPanelSource.indexOf(">METARs<");
    expect(rowIndex).toBeGreaterThan(settingsPanelSource.indexOf(">Weather<"));
    const callSite = settingsPanelSource.slice(rowIndex, rowIndex + 300);
    expect(callSite).toContain("checked={metarOn}");
    expect(callSite).toContain("onChange={onToggleMetar}");
    expect(callSite).toContain("disabled={metarDisabled}");
  });
});

describe("ToggleSwitch -- shared internal component for the switches above", () => {
  it("uses role=switch/aria-checked and the sliding-thumb translate-x idiom, same as ControlsPanel's pre-existing Radar switch", () => {
    expect(settingsPanelSource).toContain('role="switch"');
    expect(settingsPanelSource).toContain("aria-checked={checked}");
    expect(settingsPanelSource).toContain('checked ? "bg-blue-600"');
    expect(settingsPanelSource).toContain('checked ? "translate-x-4" : "translate-x-0.5"');
  });

  it("is used exactly four times (Map Labels, Reception Outline, Distance Rings, METARs)", () => {
    const matches = settingsPanelSource.match(/<ToggleSwitch/g) ?? [];
    expect(matches.length).toBe(4);
  });
});
