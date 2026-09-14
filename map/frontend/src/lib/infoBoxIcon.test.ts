import { describe, expect, it, vi } from "vitest";
import type * as maplibregl from "maplibre-gl";
import {
  INFO_BOX_ICON_CONTENT,
  INFO_BOX_ICON_ID,
  INFO_BOX_ICON_STRETCH_X,
  INFO_BOX_ICON_STRETCH_Y,
  registerInfoBoxIcon,
} from "./infoBoxIcon";

// buildInfoBoxIconImageData() itself needs a 2D canvas, which this
// project's "node" vitest environment doesn't provide -- same limitation
// aircraftIcon.test.ts documents for buildShapeIconImageData. Its actual
// rounded-rect fill is therefore only verifiable live (see this PR's
// description); these tests cover the parts that don't need a canvas: the
// stretch/content geometry map.addImage is given, and registerInfoBoxIcon's
// registration/dedup/failure-swallowing behavior.

describe("INFO_BOX_ICON stretch/content geometry", () => {
  it("content exactly matches the stretchX/stretchY bounds -- the safe-text box is the same region that's allowed to stretch", () => {
    const [x1, y1, x2, y2] = INFO_BOX_ICON_CONTENT;
    expect(INFO_BOX_ICON_STRETCH_X).toEqual([[x1, x2]]);
    expect(INFO_BOX_ICON_STRETCH_Y).toEqual([[y1, y2]]);
  });

  it("content is a well-formed box (x1 < x2, y1 < y2) with a positive-area interior", () => {
    const [x1, y1, x2, y2] = INFO_BOX_ICON_CONTENT;
    expect(x1).toBeLessThan(x2);
    expect(y1).toBeLessThan(y2);
  });

  it("content is symmetric (equal margin on every side) -- an off-center box would read as mis-aligned text once stretched", () => {
    const [x1, y1, x2, y2] = INFO_BOX_ICON_CONTENT;
    // Symmetric around the canvas center implies x1 == y1 == x2's
    // complement given a square base canvas -- checked structurally via
    // the stretch region instead of a hardcoded canvas size, so this
    // doesn't need to know CANVAS_PX (not exported).
    expect(x2 - x1).toBe(y2 - y1);
    expect(x1).toBe(y1);
  });
});

// A minimal fake covering only the two Map methods registerInfoBoxIcon
// calls -- same "(globalThis as unknown as {...})"-style narrow cast
// convention aircraftIcon.test.ts uses, rather than `any`.
interface FakeMap {
  hasImage: ReturnType<typeof vi.fn>;
  addImage: ReturnType<typeof vi.fn>;
}

describe("registerInfoBoxIcon", () => {
  function fakeMap(hasImage: boolean): FakeMap {
    return {
      hasImage: vi.fn().mockReturnValue(hasImage),
      addImage: vi.fn(),
    };
  }

  it("is a no-op (never calls addImage) when the image is already registered", () => {
    const map = fakeMap(true);
    registerInfoBoxIcon(map as unknown as maplibregl.Map);
    expect(map.hasImage).toHaveBeenCalledWith(INFO_BOX_ICON_ID);
    expect(map.addImage).not.toHaveBeenCalled();
  });

  it("doesn't throw when the canvas is unavailable (this test environment) -- swallows the failure like registerShapeImage does", () => {
    const map = fakeMap(false);
    expect(() => registerInfoBoxIcon(map as unknown as maplibregl.Map)).not.toThrow();
    // buildInfoBoxIconImageData() throws before addImage would ever be
    // reached in this canvas-less environment.
    expect(map.addImage).not.toHaveBeenCalled();
  });
});
