import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (see MapView.test.ts's own use of this) -- no
// jsdom/component-render test setup in this project (lib/config.test.ts),
// so the box's conditional style logic is checked by reading the actual
// source text rather than rendering.
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
  // (font family / trend-arrow glyph), which is being implemented
  // concurrently against this same file.
  it("does not touch the font-mono class or the existing text-size/padding classes", () => {
    expect(infoBoxLayerSource).toContain("font-mono");
    expect(infoBoxLayerSource).toContain("px-1.5 py-1");
    expect(infoBoxLayerSource).toContain("text-[12px]");
    expect(infoBoxLayerSource).toContain("text-[10.5px]");
  });
});
