import { describe, expect, it } from "vitest";
import { basemapLabelLayerIds } from "./basemapLabels";
import { AIRCRAFT_LAYER_ID, RANGE_RING_LABEL_LAYER_ID, RANGE_RING_LAYER_ID, TRACE_POINTS_LABEL_LAYER_ID } from "./mapLayerIds";

describe("basemapLabelLayerIds", () => {
  it("matches symbol layers with a text-field", () => {
    const result = basemapLabelLayerIds([
      { id: "label_city", type: "symbol", layout: { "text-field": ["get", "name"] } },
      { id: "highway-name-major", type: "symbol", layout: { "text-field": ["get", "name"] } },
    ]);
    expect(result).toEqual(["label_city", "highway-name-major"]);
  });

  it("excludes symbol layers with no text-field", () => {
    const result = basemapLabelLayerIds([
      { id: AIRCRAFT_LAYER_ID, type: "symbol", layout: { "icon-image": "sf-ac-a320" } },
    ]);
    expect(result).toEqual([]);
  });

  it("excludes non-symbol layers entirely, even if they somehow carry a layout.text-field", () => {
    const result = basemapLabelLayerIds([{ id: RANGE_RING_LAYER_ID, type: "line", layout: { "text-field": "x" } }]);
    expect(result).toEqual([]);
  });

  it("excludes SkyFollower's own range-ring label layer", () => {
    const result = basemapLabelLayerIds([
      { id: RANGE_RING_LABEL_LAYER_ID, type: "symbol", layout: { "text-field": ["get", "label"] } },
      { id: "label_country_1", type: "symbol", layout: { "text-field": ["get", "name"] } },
    ]);
    expect(result).toEqual(["label_country_1"]);
  });

  it("excludes SkyFollower's own trace-points label layer", () => {
    const result = basemapLabelLayerIds([
      { id: TRACE_POINTS_LABEL_LAYER_ID, type: "symbol", layout: { "text-field": ["get", "label"] } },
      { id: "water_name_point_label", type: "symbol", layout: { "text-field": ["get", "name"] } },
    ]);
    expect(result).toEqual(["water_name_point_label"]);
  });

  it("returns an empty array for a style with no matching layers", () => {
    expect(basemapLabelLayerIds([])).toEqual([]);
  });

  it("returns an empty array when every text layer is SkyFollower's own", () => {
    const result = basemapLabelLayerIds([
      { id: RANGE_RING_LABEL_LAYER_ID, type: "symbol", layout: { "text-field": ["get", "label"] } },
      { id: TRACE_POINTS_LABEL_LAYER_ID, type: "symbol", layout: { "text-field": ["get", "label"] } },
    ]);
    expect(result).toEqual([]);
  });
});
