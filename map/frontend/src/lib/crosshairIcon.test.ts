import { describe, expect, it } from "vitest";
import { crosshairSvgMarkup, MUTED_GRAY } from "./crosshairIcon";

describe("crosshairSvgMarkup", () => {
  it("defaults to the muted gray used by the on-map home marker", () => {
    const markup = crosshairSvgMarkup(20);
    expect(markup).toContain(`stroke="${MUTED_GRAY}"`);
    expect(markup).not.toContain("currentColor");
  });

  it("bakes an explicit color into every stroke attribute", () => {
    const markup = crosshairSvgMarkup(28, "#334155");
    expect(markup).toContain('stroke="#334155"');
    expect(markup).not.toContain(MUTED_GRAY);
  });

  it("accepts currentColor so a CSS text-color class can drive the icon (recenter button)", () => {
    const markup = crosshairSvgMarkup(20, "currentColor");
    // Every stroked element (circle + 4 tick lines) must use currentColor,
    // not a baked-in hex, so a Tailwind dark:text-* class actually reaches it.
    const strokeCount = (markup.match(/stroke="currentColor"/g) ?? []).length;
    expect(strokeCount).toBe(5);
    expect(markup).not.toContain("#334155");
  });
});
