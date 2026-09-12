import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see AircraftDetailPanel.test.ts/MapView.test.ts's own
// use of this) -- no jsdom/component-render test setup in this project (see
// lib/config.test.ts), so the shared icon-button rendering mechanism is
// checked against its own source text rather than a rendered DOM.
import iconButtonSource from "./IconButton.tsx?raw";

describe("IconButton / ActionIcon -- shared icon-only button used by both AircraftDetailPanel's action row and ControlsPanel's toggle row", () => {
  it("colors via the shared toggleButtonClass helper, keyed off `active`", () => {
    expect(iconButtonSource).toContain("toggleButtonClass(active)");
  });

  it("carries the label as both title and aria-label, and aria-pressed for toggle state", () => {
    expect(iconButtonSource).toContain("title={label}");
    expect(iconButtonSource).toContain("aria-label={label}");
    expect(iconButtonSource).toContain("aria-pressed={active}");
  });

  it("supports an optional disabled prop, defaulting to false", () => {
    expect(iconButtonSource).toContain("disabled = false");
    expect(iconButtonSource).toContain("disabled={disabled}");
  });

  it("renders icons as stroke-based, 18x18, viewBox 0 0 24 24, stroke-width 2, round caps/joins", () => {
    const svgOpenIndex = iconButtonSource.indexOf("<svg");
    const svgTag = iconButtonSource.slice(svgOpenIndex, iconButtonSource.indexOf(">", svgOpenIndex) + 1);
    expect(svgTag).toContain('width="18"');
    expect(svgTag).toContain('height="18"');
    expect(svgTag).toContain('viewBox="0 0 24 24"');
    expect(svgTag).toContain('fill="none"');
    expect(svgTag).toContain('stroke="currentColor"');
    expect(svgTag).toContain('strokeWidth="2"');
    expect(svgTag).toContain('strokeLinecap="round"');
    expect(svgTag).toContain('strokeLinejoin="round"');
  });

  it("gives a `filled` circle an explicit currentColor fill, matching Lucide's own source", () => {
    expect(iconButtonSource).toContain('fill={c.filled ? "currentColor" : undefined}');
  });
});
