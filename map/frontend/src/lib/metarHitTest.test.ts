import { describe, expect, it } from "vitest";
import type { Map as MapLibreMap } from "maplibre-gl";
import { nearestMetarStation } from "./metarHitTest";
import { METAR_LAYER_ID, SELECTABLE_LAYER_IDS } from "./mapLayerIds";

// Projects a coordinate straight to the same pixel.
function mapWith(dots: [string, number, number][], hasLayer = true): MapLibreMap {
  return {
    getLayer: () => (hasLayer ? {} : undefined),
    project: ([x, y]: [number, number]) => ({ x, y }),
    queryRenderedFeatures: () =>
      dots.map(([station_id, x, y]) => ({ geometry: { type: "Point", coordinates: [x, y] }, properties: { station_id } })),
  } as unknown as MapLibreMap;
}

describe("nearestMetarStation", () => {
  it("picks the dot closest to the click", () => {
    expect(nearestMetarStation(mapWith([["FAR", 8, 8], ["NEAR", 2, 1]]), { x: 0, y: 0 })).toBe("NEAR");
  });

  it("keeps the topmost of two dots at the same place", () => {
    expect(nearestMetarStation(mapWith([["TOP", 1, 1], ["UNDER", 1, 1]]), { x: 0, y: 0 })).toBe("TOP");
  });

  it("returns null with nothing there or before the layer exists", () => {
    expect(nearestMetarStation(mapWith([]), { x: 0, y: 0 })).toBeNull();
    expect(nearestMetarStation(mapWith([["A", 0, 0]], false), { x: 0, y: 0 })).toBeNull();
  });

  it("keeps weather dots out of aircraft selection", () => {
    expect(SELECTABLE_LAYER_IDS).not.toContain(METAR_LAYER_ID);
  });
});
