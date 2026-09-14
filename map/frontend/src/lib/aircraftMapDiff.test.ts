import { describe, expect, it } from "vitest";
import type { MapWsEvent } from "../api/types";
import { applySnapshot, applyWsEvent, applyWsEvents } from "./aircraftState";
import { diffAircraftMaps } from "./aircraftMapDiff";

describe("diffAircraftMaps", () => {
  it("reports nothing changed between a map and itself", () => {
    const state = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    expect(diffAircraftMaps(state, state)).toEqual(new Set());
  });

  it("reports a newly-added icao_hex", () => {
    const before = applySnapshot([]);
    const after = applyWsEvent(before, { type: "position", icao_hex: "NEW123", lat: 1, lon: 2 });
    expect(diffAircraftMaps(before, after)).toEqual(new Set(["NEW123"]));
  });

  it("reports a removed icao_hex", () => {
    const before = applyWsEvent(applySnapshot([]), { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    const after = applyWsEvent(before, { type: "remove", icao_hex: "A1B2C3" });
    expect(diffAircraftMaps(before, after)).toEqual(new Set(["A1B2C3"]));
  });

  it("reports an updated icao_hex (new reference), not any untouched sibling", () => {
    let state = applyWsEvent(applySnapshot([]), { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    state = applyWsEvent(state, { type: "position", icao_hex: "UNTOUCHED", lat: 3, lon: 4 });
    const before = state;
    const after = applyWsEvent(state, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    expect(diffAircraftMaps(before, after)).toEqual(new Set(["A1B2C3"]));
  });

  it("a batch touching several hexes reports exactly those, not the whole fleet", () => {
    let state = applySnapshot([
      { icao_hex: "AAAAAA", lat: 1, lon: 1 },
      { icao_hex: "BBBBBB", lat: 2, lon: 2 },
      { icao_hex: "CCCCCC", lat: 3, lon: 3 },
    ]);
    const before = state;
    const events: MapWsEvent[] = [
      { type: "position", icao_hex: "AAAAAA", lat: 1.1, lon: 1.1 },
      { type: "stale", icao_hex: "CCCCCC" },
    ];
    state = applyWsEvents(state, events);
    expect(diffAircraftMaps(before, state)).toEqual(new Set(["AAAAAA", "CCCCCC"]));
  });

  it("an empty-to-empty transition reports nothing", () => {
    expect(diffAircraftMaps(applySnapshot([]), applySnapshot([]))).toEqual(new Set());
  });
});
