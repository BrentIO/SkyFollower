import { describe, expect, it } from "vitest";
import { applySnapshot, applyWsEvent, type AircraftMap } from "./aircraftState";
import { aircraftFeatureCollection, trailFeatureCollection } from "./featureCollections";

function withOnePositionedAircraft(icaoHex = "A1B2C3"): AircraftMap {
  return applySnapshot([{ icao_hex: icaoHex, lat: 1, lon: 2, alt: 1000 }]);
}

describe("aircraftFeatureCollection", () => {
  it("includes a visible, positioned aircraft", () => {
    const aircraft = withOnePositionedAircraft();
    const fc = aircraftFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].properties?.icao_hex).toBe("A1B2C3");
  });

  it("excludes a hidden aircraft even though it still has a position", () => {
    const base = withOnePositionedAircraft();
    const hidden = applyWsEvent(base, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(hidden, new Set());
    expect(fc.features).toHaveLength(0);
  });

  it("excludes an aircraft with no known position regardless of hidden state", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3" }]);
    const fc = aircraftFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(0);
  });
});

describe("trailFeatureCollection", () => {
  it("includes trail segments for a visible aircraft in the visible-ids set", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features.length).toBeGreaterThan(0);
  });

  it("excludes a hidden aircraft's trail even though its trail data is retained", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    // The trail data itself must still be there client-side...
    expect(aircraft.A1B2C3.trail.length).toBeGreaterThan(0);
    // ...but it must not be drawn while hidden.
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features).toHaveLength(0);
  });

  it("excludes an aircraft not in the visible-ids set regardless of hidden state", () => {
    const aircraft = withOnePositionedAircraft();
    const fc = trailFeatureCollection(aircraft, new Set());
    expect(fc.features).toHaveLength(0);
  });

  it("re-includes the bridged trail once the aircraft un-hides via a new position event", () => {
    let aircraft = withOnePositionedAircraft();
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 5, lon: 6 });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    // One segment bridging the pre-gap point to the post-gap point.
    expect(fc.features).toHaveLength(1);
  });
});
