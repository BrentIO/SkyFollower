import { describe, expect, it } from "vitest";

import { AIRCRAFT_SHAPES } from "./aircraftShapes.generated";
import {
  DESCRIPTION_SHAPES,
  FALLBACK_SHAPE,
  TYPE_ALIASES,
  resolveAircraftShape,
} from "./aircraftIconResolver";

describe("resolveAircraftShape", () => {
  it("uses the exact type designator when a silhouette exists for it", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "A320" })).toBe("A320");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "b738" })).toBe("B738");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "H60" })).toBe("H60");
  });

  it("aliases a designator with no dedicated art to the nearest available shape", () => {
    // A319ceo has no file; A320 is the nearest.
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "A319" })).toBe("A320");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "E190" })).toBe("E195");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "B77F" })).toBe("B77W");
  });

  it("falls back to the description code + WTC when the type is unknown", () => {
    expect(
      resolveAircraftShape({ icao_hex: "A", description_code: "L2J", wake_turbulence_category: "H" }),
    ).toBe("B772");
    expect(
      resolveAircraftShape({ icao_hex: "A", description_code: "L2J", wake_turbulence_category: "M" }),
    ).toBe("A320");
    expect(
      resolveAircraftShape({ icao_hex: "A", description_code: "L2J", wake_turbulence_category: "L" }),
    ).toBe("CRJ2");
  });

  it("uses the description code without a WTC", () => {
    expect(resolveAircraftShape({ icao_hex: "A", description_code: "H" })).toBe("H60");
    expect(resolveAircraftShape({ icao_hex: "A", description_code: "L1P" })).toBe("C172");
    expect(resolveAircraftShape({ icao_hex: "A", description_code: "L4J" })).toBe("B744");
  });

  it("falls back on the description-code category letter alone", () => {
    // An unrecognised full code, but the leading "H" still means helicopter.
    expect(resolveAircraftShape({ icao_hex: "A", description_code: "H2T" })).toBe("H60");
  });

  it("prefers the exact type over the description code", () => {
    expect(
      resolveAircraftShape({
        icao_hex: "A",
        type_designator: "C172",
        description_code: "L4J",
        wake_turbulence_category: "H",
      }),
    ).toBe("C172");
  });

  it("returns the fallback shape when nothing is known", () => {
    expect(resolveAircraftShape()).toBe(FALLBACK_SHAPE);
    expect(resolveAircraftShape(null)).toBe(FALLBACK_SHAPE);
    expect(resolveAircraftShape({ icao_hex: "A" })).toBe(FALLBACK_SHAPE);
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "ZZZZ" })).toBe(FALLBACK_SHAPE);
  });

  it("every table target and the fallback are real shape keys", () => {
    expect(AIRCRAFT_SHAPES[FALLBACK_SHAPE]).toBeDefined();
    for (const [designator, target] of Object.entries(TYPE_ALIASES)) {
      expect(AIRCRAFT_SHAPES[target], `TYPE_ALIASES[${designator}] -> ${target}`).toBeDefined();
    }
    for (const [code, target] of Object.entries(DESCRIPTION_SHAPES)) {
      expect(AIRCRAFT_SHAPES[target], `DESCRIPTION_SHAPES[${code}] -> ${target}`).toBeDefined();
    }
  });
});
