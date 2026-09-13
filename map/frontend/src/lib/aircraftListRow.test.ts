import { describe, expect, it } from "vitest";
import { applySnapshot, type AircraftRecord } from "./aircraftState";
import { buildAircraftListRow, buildAircraftListRows } from "./aircraftListRow";
import type { CenterPoint } from "./config";

// Builds a real AircraftRecord via applySnapshot (same convention as
// lib/featureCollections.test.ts's withOnePositionedAircraft) rather than
// a hand-rolled partial -- AircraftRecord carries several fields
// (trail/tracePoints/shape/iconScale) that only applySnapshot knows how to
// derive correctly. `overrides` is applied on top so AircraftRecord-only
// fields (hidden/stale) can still be set directly.
function baseAircraft(overrides: Partial<AircraftRecord> = {}): AircraftRecord {
  const icaoHex = overrides.icao_hex ?? "A2C9E4";
  const built = applySnapshot([{ ...overrides, icao_hex: icaoHex }])[icaoHex];
  return { ...built, ...overrides };
}

const CENTER: CenterPoint = { latitude: 33.9425, longitude: -118.4081 };

describe("buildAircraftListRow -- Ident/Registration/Type/Desc", () => {
  it("populates each field when known", () => {
    const row = buildAircraftListRow(
      baseAircraft({
        ident: "DAL659",
        aircraft: {
          icao_hex: "A2C9E4",
          registration: "N727JF",
          type_designator: "B772",
          description_code: "L2J",
        },
      }),
      null,
    );
    expect(row.ident).toBe("DAL659");
    expect(row.registration).toBe("N727JF");
    expect(row.typeDesignator).toBe("B772");
    expect(row.descriptionCode).toBe("L2J");
  });

  it("renders blank (null), never a placeholder string, when a field is unknown", () => {
    const row = buildAircraftListRow(baseAircraft(), null);
    expect(row.ident).toBeNull();
    expect(row.registration).toBeNull();
    expect(row.typeDesignator).toBeNull();
    expect(row.descriptionCode).toBeNull();
  });

  it("treats a whitespace-only ident as unknown -- same convention as aircraftDetail.ts", () => {
    const row = buildAircraftListRow(baseAircraft({ ident: "   " }), null);
    expect(row.ident).toBeNull();
  });
});

describe("buildAircraftListRow -- Military/Special Livery", () => {
  it("is false/null when neither applies", () => {
    const row = buildAircraftListRow(baseAircraft(), null);
    expect(row.military).toBe(false);
    expect(row.specialLivery).toBeNull();
  });

  it("carries military and specialLivery through independently", () => {
    const row = buildAircraftListRow(
      baseAircraft({ aircraft: { icao_hex: "A2C9E4", military: true, special_livery: "Retro" } }),
      null,
    );
    expect(row.military).toBe(true);
    expect(row.specialLivery).toBe("Retro");
  });
});

describe("buildAircraftListRow -- Altitude trend prefix", () => {
  it("is blank when altitude is unknown", () => {
    const row = buildAircraftListRow(baseAircraft(), null);
    expect(row.altitudeFt).toBeNull();
    expect(row.altitudeDisplay).toBeNull();
  });

  it("shows the plain altitude, no arrow, when level (within the ±500 ft/min threshold)", () => {
    const row = buildAircraftListRow(baseAircraft({ alt: 35000, vs: 200 }), null);
    expect(row.altitudeDisplay).toBe("35000");
  });

  it("shows the plain altitude, no arrow, when vertical speed is unknown", () => {
    const row = buildAircraftListRow(baseAircraft({ alt: 35000 }), null);
    expect(row.altitudeDisplay).toBe("35000");
  });

  it("prefixes trendArrow()'s descending glyph, matching the worked example's placement", () => {
    const row = buildAircraftListRow(baseAircraft({ alt: 22750, vs: -1200 }), null);
    expect(row.altitudeDisplay).toBe("↓ 22750");
  });

  it("prefixes trendArrow()'s climbing glyph", () => {
    const row = buildAircraftListRow(baseAircraft({ alt: 37000, vs: 1500 }), null);
    expect(row.altitudeDisplay).toBe("↑ 37000");
  });

  it("carries the raw feet value for sorting, independent of the display string", () => {
    const row = buildAircraftListRow(baseAircraft({ alt: 22750.4, vs: -1200 }), null);
    expect(row.altitudeFt).toBe(22750.4);
  });
});

describe("buildAircraftListRow -- Distance", () => {
  it("is blank when there is no center configured", () => {
    const row = buildAircraftListRow(baseAircraft({ lat: 34.05, lon: -118.25 }), null);
    expect(row.distanceDisplay).toBeNull();
    expect(row.distanceNm).toBeNull();
  });

  it("is blank when the aircraft's own position isn't known yet", () => {
    const row = buildAircraftListRow(baseAircraft(), CENTER);
    expect(row.distanceDisplay).toBeNull();
    expect(row.distanceNm).toBeNull();
  });

  it("matches AircraftDetailPanel's own Distance row computation exactly (reused, not reimplemented)", () => {
    const row = buildAircraftListRow(baseAircraft({ lat: 34.05, lon: -118.25 }), CENTER);
    expect(row.distanceDisplay).toBe("10.2");
    expect(row.distanceNm).toBeCloseTo(10.2, 5);
  });
});

describe("buildAircraftListRow -- emergency squawk", () => {
  it("is false for a normal squawk or unknown squawk", () => {
    expect(buildAircraftListRow(baseAircraft({ squawk: "1200" }), null).emergency).toBe(false);
    expect(buildAircraftListRow(baseAircraft(), null).emergency).toBe(false);
  });

  it("is true for each of the four emergency codes", () => {
    for (const squawk of ["7500", "7600", "7700", "7777"]) {
      expect(buildAircraftListRow(baseAircraft({ squawk }), null).emergency).toBe(true);
    }
  });
});

describe("buildAircraftListRows -- population rule", () => {
  it("includes every non-hidden aircraft, stale ones too", () => {
    const rows = buildAircraftListRows(
      {
        a: baseAircraft({ icao_hex: "AAAAAA" }),
        b: baseAircraft({ icao_hex: "BBBBBB", stale: true }),
      },
      null,
    );
    const hexes = rows.map((r) => r.icaoHex).sort();
    expect(hexes).toEqual(["AAAAAA", "BBBBBB"]);
  });

  it("excludes hidden aircraft -- same filter as featureCollections.ts", () => {
    const rows = buildAircraftListRows(
      {
        a: baseAircraft({ icao_hex: "AAAAAA" }),
        b: baseAircraft({ icao_hex: "BBBBBB", hidden: true }),
      },
      null,
    );
    expect(rows.map((r) => r.icaoHex)).toEqual(["AAAAAA"]);
  });
});
