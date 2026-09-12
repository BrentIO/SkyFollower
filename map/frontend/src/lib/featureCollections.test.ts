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

describe("aircraftFeatureCollection -- Isolate (isolateId)", () => {
  it("hides every aircraft except the isolated one", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const fc = aircraftFeatureCollection(aircraft, new Set(), { isolateId: "A1B2C3" });
    expect(fc.features.map((f) => f.properties?.icao_hex)).toEqual(["A1B2C3"]);
  });

  it("shows everyone when isolateId is null/undefined", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    const fc = aircraftFeatureCollection(aircraft, new Set(), { isolateId: null });
    expect(fc.features).toHaveLength(2);
  });

  it("hides a newly-appearing aircraft too, same as any other aircraft while isolated", () => {
    const base = withOnePositionedAircraft("A1B2C3");
    const withNewArrival = applyWsEvent(base, { type: "position", icao_hex: "NEW123", lat: 9, lon: 9 });
    const fc = aircraftFeatureCollection(withNewArrival, new Set(), { isolateId: "A1B2C3" });
    expect(fc.features.map((f) => f.properties?.icao_hex)).toEqual(["A1B2C3"]);
  });
});

describe("aircraftFeatureCollection -- Follow-lost dimming", () => {
  it("keeps a hidden followed aircraft visible, marked stale (dimmed)", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "A1B2C3" });
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].properties?.stale).toBe(true);
  });

  it("does not affect a hidden aircraft that isn't the followed one", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "OTHER" });
    expect(fc.features).toHaveLength(0);
  });

  it("leaves an already-stale (not hidden) followed aircraft's stale flag as-is", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "stale", icao_hex: "A1B2C3" });
    const fc = aircraftFeatureCollection(aircraft, new Set(), { followId: "A1B2C3" });
    expect(fc.features[0].properties?.stale).toBe(true);
  });
});

describe("trailFeatureCollection -- Isolate (isolateId)", () => {
  it("hides every other aircraft's trail even if it's in the visible-ids set", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = { ...aircraft, ...withOnePositionedAircraft("D4E5F6") };
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "D4E5F6", lat: 1.1, lon: 2.1 });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3", "D4E5F6"]), { isolateId: "A1B2C3" });
    expect(fc.features.every((f) => f.properties?.icao_hex === "A1B2C3")).toBe(true);
    expect(fc.features.length).toBeGreaterThan(0);
  });
});

describe("trailFeatureCollection -- Follow-lost dimming", () => {
  it("keeps a hidden followed aircraft's trail visible and flags it dimmed", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    aircraft = applyWsEvent(aircraft, { type: "hide", icao_hex: "A1B2C3" });

    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]), { followId: "A1B2C3" });
    expect(fc.features.length).toBeGreaterThan(0);
    expect(fc.features.every((f) => f.properties?.dimmed === true)).toBe(true);
  });

  it("a normal (not hidden/followed) trail is not marked dimmed", () => {
    let aircraft = withOnePositionedAircraft("A1B2C3");
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1.1, lon: 2.1 });
    const fc = trailFeatureCollection(aircraft, new Set(["A1B2C3"]));
    expect(fc.features.every((f) => f.properties?.dimmed === false)).toBe(true);
  });
});
