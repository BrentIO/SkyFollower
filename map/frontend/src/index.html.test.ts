import { describe, expect, it } from "vitest";

// Vite's `?raw` suffix (see src/components/*.test.ts's own use of this) --
// no jsdom/component-render test setup in this project, so index.html is
// checked against its source text rather than a parsed DOM.
import indexHtml from "../index.html?raw";

describe("index.html -- Google Fonts stylesheet (#2053)", () => {
  it("preloads the stylesheet instead of blocking render on it", () => {
    expect(indexHtml).toContain('rel="preload"');
    expect(indexHtml).toContain('as="style"');
    expect(indexHtml).toContain("onload=\"this.onload=null;this.rel='stylesheet'\"");
  });

  it("still registers it as a stylesheet without JS via <noscript>", () => {
    const noscriptIndex = indexHtml.indexOf("<noscript>");
    expect(noscriptIndex).toBeGreaterThan(-1);
    const noscriptBlock = indexHtml.slice(noscriptIndex, indexHtml.indexOf("</noscript>", noscriptIndex));
    expect(noscriptBlock).toContain("rel=\"stylesheet\"");
    expect(noscriptBlock).toContain("fonts.googleapis.com/css2?family=JetBrains+Mono");
  });
});
