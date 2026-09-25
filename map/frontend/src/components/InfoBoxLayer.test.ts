import { describe, expect, it } from "vitest";

// Vite's `?raw` suffix (see MapView.test.ts's own use of this) -- no
// jsdom/component-render test setup in this project (lib/config.test.ts),
// so both the displayScale style logic (#2000) and the arrow-glyph
// rendering change (#2001) are checked against the actual source text
// rather than a rendered DOM.
import infoBoxLayerSource from "./InfoBoxLayer.tsx?raw";

// #2000: displayScale (ControlsPanel's operator-facing "Display Scale"
// control, threaded through MapView.tsx) scales the whole info box via a
// CSS transform, sharing one multiplier with the aircraft icon's own
// icon-size expression (see MapView.test.ts's "display scale multiplier"
// describe block). Applied here rather than recomputing every font-size/
// padding value individually -- see the prop doc in InfoBoxLayer.tsx for
// why, and this file's #1851 history for why an *unconditional* transform
// (even a no-op scale(1)) isn't used: it would promote every rendered box
// to its own compositor layer for zero visual benefit at the default.

describe("InfoBoxLayer -- displayScale prop", () => {
  it("accepts and destructures a displayScale prop", () => {
    expect(infoBoxLayerSource).toContain("displayScale: number;");
    expect(infoBoxLayerSource).toContain(
      "export function InfoBoxLayer({ items, selected, showAll, hoveredId, displayScale }: InfoBoxLayerProps) {",
    );
  });

  it("only applies a transform when displayScale differs from the 1.0 no-op default", () => {
    const styleIndex = infoBoxLayerSource.indexOf("style={{");
    expect(styleIndex).toBeGreaterThan(-1);
    const styleBlock = infoBoxLayerSource.slice(styleIndex, infoBoxLayerSource.indexOf("}}", styleIndex) + 2);
    expect(styleBlock).toContain("displayScale !== 1");
    expect(styleBlock).toContain("transform: `scale(${displayScale})`");
  });

  it("scales from the top-left corner, matching the box's own left/top anchor point (infoBoxOffset.ts's near corner)", () => {
    expect(infoBoxLayerSource).toContain('transformOrigin: "top left"');
  });

  it("leaves the left/top position math untouched by the scale change", () => {
    expect(infoBoxLayerSource).toContain("left: b.item.x + b.item.offset,");
    expect(infoBoxLayerSource).toContain("top: b.item.y + b.item.offset,");
  });

  // Guards against this change accidentally touching #2001's territory
  // (font family / trend-arrow glyph), which was implemented concurrently
  // against this same file.
  it("does not touch the font-mono class or the existing text-size/padding classes", () => {
    expect(infoBoxLayerSource).toContain("font-mono");
    expect(infoBoxLayerSource).toContain("px-1.5 py-1");
    expect(infoBoxLayerSource).toContain("text-[12px]");
    expect(infoBoxLayerSource).toContain("text-[10.5px]");
  });
});

describe("InfoBoxLayer -- #2001 vertical-speed trend glyph", () => {
  it("no longer embeds the Unicode trend arrow characters as text", () => {
    expect(infoBoxLayerSource).not.toContain("↑");
    expect(infoBoxLayerSource).not.toContain("↓");
  });

  it("renders the trend via the dedicated TrendGlyph component, driven by lines.altitudeSpeed.trend", () => {
    expect(infoBoxLayerSource).toContain(
      "b.lines.altitudeSpeed.trend !== null && <TrendGlyph direction={b.lines.altitudeSpeed.trend} />",
    );
  });

  it("TrendGlyph is a currentColor-fill SVG sized in ems (not px), so it scales with the surrounding text", () => {
    const glyphIndex = infoBoxLayerSource.indexOf("function TrendGlyph(");
    expect(glyphIndex).toBeGreaterThan(-1);
    const glyphBody = infoBoxLayerSource.slice(glyphIndex, glyphIndex + 600);
    expect(glyphBody).toContain('fill="currentColor"');
    expect(glyphBody).toContain("h-[0.7em]");
    expect(glyphBody).toContain("w-[0.7em]");
    expect(glyphBody).not.toMatch(/width="\d/);
    expect(glyphBody).not.toMatch(/height="\d/);
  });

  it("TrendGlyph picks a distinct shape for up vs. down", () => {
    const glyphIndex = infoBoxLayerSource.indexOf("function TrendGlyph(");
    const glyphBody = infoBoxLayerSource.slice(glyphIndex, glyphIndex + 600);
    expect(glyphBody).toContain('direction === "up" ?');
  });

  it("leaves the box's existing font/size/padding classes untouched (issue #2000's concern, not #2001's)", () => {
    expect(infoBoxLayerSource).toContain("rounded bg-black/40 px-1.5 py-1 font-mono leading-tight text-white");
    expect(infoBoxLayerSource).toContain("text-[12px] font-bold whitespace-nowrap");
    expect(infoBoxLayerSource).toContain("text-[10.5px] whitespace-nowrap");
  });
});
