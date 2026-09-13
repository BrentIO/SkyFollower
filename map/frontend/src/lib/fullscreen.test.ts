import { describe, expect, it } from "vitest";
import { MAXIMIZE_ICON, MINIMIZE_ICON } from "./actionIcons";
import { fullscreenIcon } from "./fullscreen";

describe("fullscreenIcon", () => {
  it("returns MAXIMIZE_ICON when not fullscreen", () => {
    expect(fullscreenIcon(false)).toBe(MAXIMIZE_ICON);
  });

  it("returns MINIMIZE_ICON when fullscreen", () => {
    expect(fullscreenIcon(true)).toBe(MINIMIZE_ICON);
  });
});
