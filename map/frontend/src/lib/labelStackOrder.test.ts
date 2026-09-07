import { describe, expect, it } from "vitest";
import { altitudeZIndex, sortByLabelStackOrder, UNKNOWN_ALTITUDE_Z_INDEX } from "./labelStackOrder";

describe("altitudeZIndex", () => {
  it("maps sea level to the lowest known-altitude z-index", () => {
    expect(altitudeZIndex(0)).toBe(1);
  });

  it("maps the top of the realistic altitude range to the highest z-index", () => {
    expect(altitudeZIndex(60_000)).toBe(1000);
  });

  it("increases monotonically with altitude", () => {
    expect(altitudeZIndex(10_000)).toBeLessThan(altitudeZIndex(35_000));
    expect(altitudeZIndex(35_000)).toBeLessThan(altitudeZIndex(45_000));
  });

  it("clamps altitudes outside the realistic range rather than extrapolating", () => {
    expect(altitudeZIndex(70_000)).toBe(1000);
    expect(altitudeZIndex(-500)).toBe(1);
  });

  it("gives unknown altitude the fixed floor value, below every known altitude", () => {
    expect(altitudeZIndex(null)).toBe(UNKNOWN_ALTITUDE_Z_INDEX);
    expect(altitudeZIndex(undefined)).toBe(UNKNOWN_ALTITUDE_Z_INDEX);
    expect(UNKNOWN_ALTITUDE_Z_INDEX).toBeLessThan(altitudeZIndex(0));
  });
});

describe("sortByLabelStackOrder", () => {
  it("orders ascending by altitude, highest last (drawn on top)", () => {
    const items = [
      { id: "AAAAAA", altitude: 35_000 },
      { id: "BBBBBB", altitude: 5_000 },
      { id: "CCCCCC", altitude: 41_000 },
    ];
    expect(sortByLabelStackOrder(items).map((i) => i.id)).toEqual(["BBBBBB", "AAAAAA", "CCCCCC"]);
  });

  it("puts unknown-altitude aircraft at the bottom of the stack", () => {
    const items = [
      { id: "AAAAAA", altitude: 1_000 },
      { id: "BBBBBB", altitude: null },
      { id: "CCCCCC", altitude: 35_000 },
    ];
    expect(sortByLabelStackOrder(items).map((i) => i.id)).toEqual(["BBBBBB", "AAAAAA", "CCCCCC"]);
  });

  it("breaks ties at the same altitude deterministically by icao_hex", () => {
    const items = [
      { id: "CCCCCC", altitude: 35_000 },
      { id: "AAAAAA", altitude: 35_000 },
      { id: "BBBBBB", altitude: 35_000 },
    ];
    expect(sortByLabelStackOrder(items).map((i) => i.id)).toEqual(["AAAAAA", "BBBBBB", "CCCCCC"]);
  });

  it("breaks ties between multiple unknown-altitude aircraft by icao_hex too", () => {
    const items = [
      { id: "BBBBBB", altitude: undefined },
      { id: "AAAAAA", altitude: null },
    ];
    expect(sortByLabelStackOrder(items).map((i) => i.id)).toEqual(["AAAAAA", "BBBBBB"]);
  });

  it("is stable across repeated calls for an unchanged, differently-ordered input", () => {
    const items = [
      { id: "AAAAAA", altitude: 12_000 },
      { id: "BBBBBB", altitude: 12_000 },
      { id: "CCCCCC", altitude: null },
    ];
    const reversed = [...items].reverse();
    expect(sortByLabelStackOrder(items).map((i) => i.id)).toEqual(sortByLabelStackOrder(reversed).map((i) => i.id));
  });

  it("does not mutate the input array", () => {
    const items = [
      { id: "BBBBBB", altitude: 20_000 },
      { id: "AAAAAA", altitude: 10_000 },
    ];
    const copy = [...items];
    sortByLabelStackOrder(items);
    expect(items).toEqual(copy);
  });
});
