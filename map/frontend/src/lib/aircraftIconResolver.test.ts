import { describe, expect, it } from "vitest";

import { AIRCRAFT_SHAPES } from "./aircraftShapes.generated";
import {
  CATEGORY_SHAPES,
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

  it("aliases the Bombardier Challenger family to CRJ2 (#1903)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "CL60" })).toBe("CRJ2");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "CL30" })).toBe("CRJ2");
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "CL35" })).toBe("CRJ2");
  });

  // #1914 Finding 2: 31 designators that previously fell through to the
  // L2J/L3J/L4J description-code tier (rendering as a full-size airliner)
  // now have a TYPE_ALIASES entry, grouped by which existing shape their
  // real-world size lands closest to. One representative per group here;
  // the group's own comment in aircraftIconResolver.ts lists every member.
  it("aliases light bizjets missed from the existing Learjet/light-jet groups (#1914)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "BE40" })).toBe("LJ35"); // Beechjet 400
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "LJ25" })).toBe("LJ35"); // Learjet 25
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "PRM1" })).toBe("LJ35"); // Premier 1
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "C501" })).toBe("C25B"); // Citation 1SP
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "EA50" })).toBe("C25B"); // Eclipse 550
  });

  it("aliases the Cirrus SF50 Vision Jet to the light-jet shape instead of falling through to the fighter-jet fallback (#2014)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "SF50" })).toBe("LJ35");
  });

  it("aliases large-cabin Gulfstreams and Falcons missed from their existing groups (#1914)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "GA6C" })).toBe("GLF6"); // G600
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "GA8C" })).toBe("GLF6"); // G800
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "FA20" })).toBe("FA7X"); // Falcon 200
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "FA6X" })).toBe("FA7X"); // Falcon 6X
  });

  it("aliases mid-size bizjets to C750 (Citation X-class) and MD81 to the existing MD8x group (#1914)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "HA4T" })).toBe("C750"); // Hawker Horizon
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "GA4C" })).toBe("C750"); // G400
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "MD81" })).toBe("B712"); // MD-81
  });

  it("aliases the COMAC ARJ-21 regional jet to CRJX rather than the full-airliner default (#1914)", () => {
    expect(resolveAircraftShape({ icao_hex: "A", type_designator: "AJ27" })).toBe("CRJX");
  });

  it("leaves genuinely airliner/military-class designators on the description-code default (#1914)", () => {
    // Fokker 100: a real ~100-seat narrowbody airliner -- A320 is the
    // right general class, not the Finding 2 bug.
    expect(
      resolveAircraftShape({ icao_hex: "A", type_designator: "F100", description_code: "L2J" }),
    ).toBe("A320");
    // MiG-29: a fighter, not a bizjet miscategorization -- a different,
    // unrelated problem this issue doesn't address.
    expect(
      resolveAircraftShape({ icao_hex: "A", type_designator: "MG29", description_code: "L2J" }),
    ).toBe("A320");
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

  it("falls back on the raw emitter category when type and description are absent", () => {
    expect(resolveAircraftShape({ icao_hex: "A", emitter_category: "A7" })).toBe("H60");
    expect(resolveAircraftShape({ icao_hex: "A", emitter_category: "b2" })).toBe("BALL");
    expect(resolveAircraftShape({ icao_hex: "A", emitter_category: "A5" })).toBe("B772"); // #1906
    // Subcategory with no mapping (e.g. "A0"/"C3") -> plain fallback.
    expect(resolveAircraftShape({ icao_hex: "A", emitter_category: "A0" })).toBe(FALLBACK_SHAPE);
    expect(resolveAircraftShape({ icao_hex: "A", emitter_category: "C3" })).toBe(FALLBACK_SHAPE);
  });

  it("prefers the type designator and the description code over the emitter category", () => {
    // Type designator wins.
    expect(
      resolveAircraftShape({ icao_hex: "A", type_designator: "A320", emitter_category: "A7" }),
    ).toBe("A320");
    // Description code wins.
    expect(
      resolveAircraftShape({ icao_hex: "A", description_code: "H", emitter_category: "A5" }),
    ).toBe("H60");
    // Description-code category letter alone still wins over the emitter category.
    expect(
      resolveAircraftShape({ icao_hex: "A", description_code: "G7X", emitter_category: "A5" }),
    ).toBe("GYRO");
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
    for (const [category, target] of Object.entries(CATEGORY_SHAPES)) {
      expect(AIRCRAFT_SHAPES[target], `CATEGORY_SHAPES[${category}] -> ${target}`).toBeDefined();
    }
  });
});

describe("compact/simple-silhouette shape scale floor", () => {
  // Mirrors COMPACT_SILHOUETTE_KEYS in scripts/generate-aircraft-shapes.mjs
  // (issue #1742): the unique shape-key targets of TYPE_ALIASES's
  // "Helicopters" section above (which already covers CATEGORY_SHAPES.A7 /
  // DESCRIPTION_SHAPES.H's "H60"), plus the balloon/gyroplane/glider/UAV/
  // motor-glider shapes the issue's own follow-up comment broadened the fix
  // to cover -- any silhouette without an airplane's elongated wing/tail
  // cross-shape, which reads as an unrecognizable blob at the general
  // SCALE_MIN floor. Keep this list in sync with the generator script's.
  const COMPACT_SILHOUETTE_SHAPE_KEYS = [
    "EC20", "EC35", "EC45", "GAZL", "AS65", "AS32", "S61", "R44", "H60",
    "NH90", "LYNX", "MI24", "H47",
    "BALL", "GYRO", "AS21", "Q4", "SF25",
  ];
  const SCALE_MIN_COMPACT_SILHOUETTE = 1.0;

  it("gives every shape in the broadened set at least the higher scale floor", () => {
    for (const key of COMPACT_SILHOUETTE_SHAPE_KEYS) {
      expect(AIRCRAFT_SHAPES[key], `AIRCRAFT_SHAPES[${key}]`).toBeDefined();
      expect(AIRCRAFT_SHAPES[key].scale, `${key}.scale`).toBeGreaterThanOrEqual(
        SCALE_MIN_COMPACT_SILHOUETTE,
      );
    }
  });

  it("leaves other correctly-small shapes at the general, lower floor", () => {
    // A light GA single is intentionally near the general 0.6 floor and
    // must stay there -- only the broadened set's floor should move.
    expect(AIRCRAFT_SHAPES.C172.scale).toBeLessThan(SCALE_MIN_COMPACT_SILHOUETTE);
  });
});

describe("accent-detail icon cutouts (ACCENT_CUTOUT_RATIO_THRESHOLD)", () => {
  // Mirrors ACCENT_CUTOUT_RATIO_THRESHOLD = 2 in
  // scripts/generate-aircraft-shapes.mjs: BALL (balloon) is the sole shape
  // whose Accent/outline ratio crosses the threshold and still renders as a
  // cutout. EC35 also crosses the ratio threshold (~2.5) but is explicitly
  // excluded via ACCENT_CUTOUT_SKIP_KEYS in the generator -- its Accent
  // layer is a rotor-blade cross spanning the whole fuselage, which
  // fragments the cabin into an unrecognizable lattice as a cutout rather
  // than reading as detail (see #1884). Keep this list in sync with the
  // generator script's own threshold/skip-list.
  const SHAPES_WITH_ACCENT_CUTOUT = ["BALL"];

  it("gives exactly the expected shapes an accentD cutout, no others", () => {
    for (const key of SHAPES_WITH_ACCENT_CUTOUT) {
      expect(AIRCRAFT_SHAPES[key], `AIRCRAFT_SHAPES[${key}]`).toBeDefined();
      expect(AIRCRAFT_SHAPES[key].accentD, `${key}.accentD`).toBeTruthy();
    }
    const actualKeysWithAccent = Object.keys(AIRCRAFT_SHAPES).filter((key) => AIRCRAFT_SHAPES[key].accentD);
    expect(actualKeysWithAccent.sort()).toEqual([...SHAPES_WITH_ACCENT_CUTOUT].sort());
  });

  it("BALL's existing cutout is unaffected by the threshold change", () => {
    expect(AIRCRAFT_SHAPES.BALL.accentD).toBeTruthy();
    expect(AIRCRAFT_SHAPES.BALL.accentStrokeWidth).toBeGreaterThan(0);
  });

  it("EC35 renders as a plain filled silhouette, no accentD cutout", () => {
    expect(AIRCRAFT_SHAPES.EC35.accentD).toBeUndefined();
    expect(AIRCRAFT_SHAPES.EC35.accentStrokeWidth).toBeUndefined();
  });
});
