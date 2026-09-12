import { describe, expect, it } from "vitest";
import { aircraftNeedingHistorySeed } from "./trailSeeding";

describe("aircraftNeedingHistorySeed", () => {
  it("returns nothing when historyAll is off, even with unseeded aircraft", () => {
    const result = aircraftNeedingHistorySeed(false, ["A1", "A2"], new Set());
    expect(result).toEqual([]);
  });

  it("returns every tracked aircraft on first pass with historyAll on", () => {
    const result = aircraftNeedingHistorySeed(true, ["A1", "A2"], new Set());
    expect(result).toEqual(["A1", "A2"]);
  });

  it("skips aircraft already in the seeded set", () => {
    const result = aircraftNeedingHistorySeed(true, ["A1", "A2"], new Set(["A1"]));
    expect(result).toEqual(["A2"]);
  });

  it("returns nothing once every tracked aircraft has been seeded", () => {
    const result = aircraftNeedingHistorySeed(true, ["A1", "A2"], new Set(["A1", "A2"]));
    expect(result).toEqual([]);
  });

  it("returns only the newly-appeared aircraft on a later pass", () => {
    const seeded = new Set(["A1", "A2"]);
    const result = aircraftNeedingHistorySeed(true, ["A1", "A2", "A3"], seeded);
    expect(result).toEqual(["A3"]);
  });
});
