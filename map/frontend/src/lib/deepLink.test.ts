import { describe, expect, it } from "vitest";
import { applySnapshot, applyWsEvent } from "./aircraftState";
import { deepLinkAircraftAvailable, deepLinkReadyToZoom } from "./deepLink";

describe("deepLinkAircraftAvailable", () => {
  it("is true when the aircraft is already in the initial snapshot", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    expect(deepLinkAircraftAvailable(aircraft, "A1B2C3")).toBe(true);
  });

  it("is false before the aircraft has appeared in tracked state", () => {
    const aircraft = applySnapshot([]);
    expect(deepLinkAircraftAvailable(aircraft, "A1B2C3")).toBe(false);
  });

  it("becomes true once a WS event adds the aircraft shortly after load", () => {
    let aircraft = applySnapshot([]);
    expect(deepLinkAircraftAvailable(aircraft, "A1B2C3")).toBe(false);

    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 1, lon: 2 });
    expect(deepLinkAircraftAvailable(aircraft, "A1B2C3")).toBe(true);
  });

  it("stays false indefinitely for an aircraft that never appears (landed/evicted/bogus id)", () => {
    let aircraft = applySnapshot([{ icao_hex: "OTHER", lat: 1, lon: 2 }]);
    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "OTHER", lat: 3, lon: 4 });
    expect(deepLinkAircraftAvailable(aircraft, "NEVERSEEN")).toBe(false);
  });
});

describe("deepLinkReadyToZoom", () => {
  it("is false while the map hasn't loaded yet, even with a known position", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", false)).toBe(false);
  });

  it("is false when the aircraft is tracked but has no position yet", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3" }]);
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", true)).toBe(false);
  });

  it("is false when the aircraft isn't tracked at all", () => {
    const aircraft = applySnapshot([]);
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", true)).toBe(false);
  });

  it("is true once the map is loaded and the aircraft has a known position", () => {
    const aircraft = applySnapshot([{ icao_hex: "A1B2C3", lat: 1, lon: 2 }]);
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", true)).toBe(true);
  });

  it("becomes true once a position arrives after identity-only tracking began", () => {
    let aircraft = applyWsEvent(applySnapshot([]), { type: "metadata", icao_hex: "A1B2C3", ident: "DAL2" });
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", true)).toBe(false);

    aircraft = applyWsEvent(aircraft, { type: "position", icao_hex: "A1B2C3", lat: 5, lon: 6 });
    expect(deepLinkReadyToZoom(aircraft, "A1B2C3", true)).toBe(true);
  });
});
