import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see IconButton.test.ts/AircraftDetailPanel.test.ts's
// own use of this) -- no jsdom/component-render test setup in this project
// (see lib/config.test.ts), so the arrow-glyph rendering change (#2001) is
// checked against its own source text rather than a rendered DOM.
import layerSource from "./InfoBoxLayer.tsx?raw";

describe("InfoBoxLayer -- #2001 vertical-speed trend glyph", () => {
  it("no longer embeds the Unicode trend arrow characters as text", () => {
    expect(layerSource).not.toContain("↑");
    expect(layerSource).not.toContain("↓");
  });

  it("renders the trend via the dedicated TrendGlyph component, driven by lines.altitudeSpeed.trend", () => {
    expect(layerSource).toContain("b.lines.altitudeSpeed.trend !== null && <TrendGlyph direction={b.lines.altitudeSpeed.trend} />");
  });

  it("TrendGlyph is a currentColor-fill SVG sized in ems (not px), so it scales with the surrounding text", () => {
    const glyphIndex = layerSource.indexOf("function TrendGlyph(");
    expect(glyphIndex).toBeGreaterThan(-1);
    const glyphBody = layerSource.slice(glyphIndex, glyphIndex + 600);
    expect(glyphBody).toContain('fill="currentColor"');
    expect(glyphBody).toContain("h-[0.7em]");
    expect(glyphBody).toContain("w-[0.7em]");
    expect(glyphBody).not.toMatch(/width="\d/);
    expect(glyphBody).not.toMatch(/height="\d/);
  });

  it("TrendGlyph picks a distinct shape for up vs. down", () => {
    const glyphIndex = layerSource.indexOf("function TrendGlyph(");
    const glyphBody = layerSource.slice(glyphIndex, glyphIndex + 600);
    expect(glyphBody).toContain('direction === "up" ?');
  });

  it("leaves the box's existing font/size/padding classes untouched (issue #2000's concern, not #2001's)", () => {
    expect(layerSource).toContain("rounded bg-black/40 px-1.5 py-1 font-mono leading-tight text-white");
    expect(layerSource).toContain('text-[12px] font-bold whitespace-nowrap');
    expect(layerSource).toContain('text-[10.5px] whitespace-nowrap');
  });
});
