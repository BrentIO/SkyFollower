import { describe, expect, it } from "vitest";
import { topIcaoHex } from "./mapHitTest";

describe("topIcaoHex", () => {
  it("returns undefined when the query hit nothing", () => {
    expect(topIcaoHex([])).toBeUndefined();
  });

  it("returns the single feature's icao_hex", () => {
    expect(topIcaoHex([{ properties: { icao_hex: "A8AE7F" } }])).toBe("A8AE7F");
  });

  it("returns the first feature's icao_hex when the click hits an aircraft icon and its own trail hit-area at once", () => {
    // A trail's most recent segment terminates exactly at the aircraft's
    // current icon position, so a click there resolves both layers -- this
    // is the exact overlap from the double-fire bug. Both entries carry the
    // same icao_hex here (the aircraft's own trail), so either order
    // resolves to the same, correct aircraft.
    const features = [{ properties: { icao_hex: "A8AE7F" } }, { properties: { icao_hex: "A8AE7F" } }];
    expect(topIcaoHex(features)).toBe("A8AE7F");
  });

  it("returns undefined when the feature has no properties", () => {
    expect(topIcaoHex([{ properties: null }])).toBeUndefined();
    expect(topIcaoHex([{}])).toBeUndefined();
  });

  it("returns undefined when icao_hex is present but not a string", () => {
    expect(topIcaoHex([{ properties: { icao_hex: 12345 } }])).toBeUndefined();
  });

  it("ignores a later feature's icao_hex when the first has none", () => {
    // Guards the "act exactly once, on the first hit" contract even in the
    // (currently impossible, since every SELECTABLE_LAYER_IDS feature
    // carries icao_hex) case of a mixed-property result set.
    const features = [{ properties: {} }, { properties: { icao_hex: "B00001" } }];
    expect(topIcaoHex(features)).toBeUndefined();
  });
});
