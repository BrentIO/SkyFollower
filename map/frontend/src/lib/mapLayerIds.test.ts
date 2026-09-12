import { describe, expect, it } from "vitest";
import {
  AIRCRAFT_LAYER_ID,
  RANGE_RING_LABEL_LAYER_ID,
  RANGE_RING_LAYER_ID,
  SELECTABLE_LAYER_IDS,
  TRAIL_HIT_AREA_LAYER_ID,
  TRAIL_LAYER_ID,
} from "./mapLayerIds";

describe("SELECTABLE_LAYER_IDS", () => {
  it("includes the aircraft icon layer", () => {
    expect(SELECTABLE_LAYER_IDS).toContain(AIRCRAFT_LAYER_ID);
  });

  it("includes the trail hit-area layer, not the cosmetic trail line", () => {
    expect(SELECTABLE_LAYER_IDS).toContain(TRAIL_HIT_AREA_LAYER_ID);
    expect(SELECTABLE_LAYER_IDS).not.toContain(TRAIL_LAYER_ID);
  });

  it("excludes the range ring layers", () => {
    expect(SELECTABLE_LAYER_IDS).not.toContain(RANGE_RING_LAYER_ID);
    expect(SELECTABLE_LAYER_IDS).not.toContain(RANGE_RING_LABEL_LAYER_ID);
  });
});
