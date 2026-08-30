import { describe, expect, it } from "vitest";
import {
  IMPORT_COORDINATE_PRECISION,
  importAreasBatch,
  parseAndValidate,
  resolveFeatureIdentity,
  roundGeometryPrecision,
  type ImportedFeature,
} from "./areaImport";

function polygon(props: Record<string, unknown>): ImportedFeature {
  return {
    type: "Feature",
    geometry: { type: "Polygon", coordinates: [[[0, 0], [1, 0], [1, 1], [0, 0]]] },
    properties: props,
  };
}

function featureCollection(features: unknown[]): string {
  return JSON.stringify({ type: "FeatureCollection", features });
}

describe("parseAndValidate — structural whole-file gate", () => {
  it("blank input is the pristine no-op state, not an error", () => {
    expect(parseAndValidate("")).toEqual({ error: null, features: [] });
    expect(parseAndValidate("   \n ")).toEqual({ error: null, features: [] });
  });

  it("rejects malformed JSON in its entirety", () => {
    const result = parseAndValidate("{not json");
    expect(result.error).toBe("Not valid GeoJSON.");
    expect(result.features).toEqual([]);
  });

  it("rejects a non-FeatureCollection root in its entirety", () => {
    const bareFeature = JSON.stringify({
      type: "Feature",
      geometry: { type: "Point", coordinates: [0, 0] },
      properties: {},
    });
    const result = parseAndValidate(bareFeature);
    expect(result.error).toBe("Not valid GeoJSON.");
    expect(result.features).toEqual([]);
  });

  it("rejects an empty FeatureCollection", () => {
    expect(parseAndValidate(featureCollection([])).error).toBe(
      "The FeatureCollection has no features.",
    );
  });

  it("accepts a single-feature FeatureCollection (single-feature path is unchanged)", () => {
    const result = parseAndValidate(featureCollection([polygon({ identifier: "alpha", name: "Alpha" })]));
    expect(result.error).toBeNull();
    expect(result.features).toHaveLength(1);
  });

  it("accepts a multi-feature FeatureCollection once every geometry type is supported", () => {
    const result = parseAndValidate(
      featureCollection([
        polygon({ identifier: "a" }),
        { type: "Feature", geometry: { type: "LineString", coordinates: [[0, 0], [1, 1]] }, properties: {} },
        { type: "Feature", geometry: { type: "Point", coordinates: [0, 0] }, properties: {} },
      ]),
    );
    expect(result.error).toBeNull();
    expect(result.features).toHaveLength(3);
  });

  it("rejects the whole file when even one feature has an unsupported geometry type", () => {
    const result = parseAndValidate(
      featureCollection([
        polygon({ identifier: "a" }),
        { type: "Feature", geometry: { type: "MultiPolygon", coordinates: [] }, properties: {} },
        polygon({ identifier: "c" }),
      ]),
    );
    expect(result.error).toBe('Feature 2: unsupported geometry type "MultiPolygon".');
    expect(result.features).toEqual([]);
  });

  it("rejects the whole file when a feature is missing its geometry entirely", () => {
    const result = parseAndValidate(
      featureCollection([polygon({ identifier: "a" }), { type: "Feature", properties: {} }]),
    );
    expect(result.error).toBe('Feature 2: unsupported geometry type "unknown".');
    expect(result.features).toEqual([]);
  });
});

describe("roundGeometryPrecision — Terra Draw coordinatePrecision guard", () => {
  const places = (n: number): number => {
    const s = String(n);
    const dot = s.indexOf(".");
    return dot === -1 ? 0 : s.length - dot - 1;
  };

  it("rounds a Polygon's 15-decimal-place coordinates to 5 (the case that was silently dropped)", () => {
    const rounded = roundGeometryPrecision({
      type: "Polygon",
      coordinates: [
        [
          [-80.123456789012345, 28.622211227100983],
          [-80.223456789012345, 28.722211227100983],
          [-80.323456789012345, 28.822211227100983],
          [-80.123456789012345, 28.622211227100983],
        ],
      ],
    });
    for (const [lng, lat] of (rounded as { coordinates: number[][][] }).coordinates[0]) {
      expect(places(lng)).toBeLessThanOrEqual(IMPORT_COORDINATE_PRECISION);
      expect(places(lat)).toBeLessThanOrEqual(IMPORT_COORDINATE_PRECISION);
    }
    expect((rounded as { coordinates: number[][][] }).coordinates[0][0]).toEqual([-80.12346, 28.62221]);
  });

  it("rounds a LineString and a Point the same way", () => {
    expect(
      roundGeometryPrecision({ type: "LineString", coordinates: [[1.123456789, 2.987654321]] }),
    ).toEqual({ type: "LineString", coordinates: [[1.12346, 2.98765]] });
    expect(roundGeometryPrecision({ type: "Point", coordinates: [28.622211227100983, -80.5] })).toEqual({
      type: "Point",
      coordinates: [28.62221, -80.5],
    });
  });

  it("leaves already-low-precision coordinates untouched and preserves a Z ordinate", () => {
    expect(roundGeometryPrecision({ type: "Point", coordinates: [1.5, 2.25, 137.4] })).toEqual({
      type: "Point",
      coordinates: [1.5, 2.25, 137.4],
    });
  });

  it("does not mutate the input geometry", () => {
    const input: ImportedFeature["geometry"] = {
      type: "Point",
      coordinates: [28.622211227100983, -80.123456789],
    };
    roundGeometryPrecision(input);
    expect(input.coordinates).toEqual([28.622211227100983, -80.123456789]);
  });
});

describe("resolveFeatureIdentity — client-side conflict auto-resolution", () => {
  it("passes a clean, unused identifier/name through untouched", () => {
    const taken = new Set<string>();
    expect(resolveFeatureIdentity(polygon({ identifier: "ramp", name: "Ramp" }), 1, taken)).toEqual({
      identifier: "ramp",
      name: "Ramp",
      locked: false,
    });
    expect(taken.has("ramp")).toBe(true);
  });

  it("preserves properties.locked", () => {
    const resolved = resolveFeatureIdentity(
      polygon({ identifier: "ramp", name: "Ramp", locked: true }),
      1,
      new Set(),
    );
    expect(resolved.locked).toBe(true);
  });

  it("appends _2 / (2) when the identifier already exists on another area", () => {
    const taken = new Set(["ramp"]);
    expect(resolveFeatureIdentity(polygon({ identifier: "ramp", name: "Ramp" }), 1, taken)).toEqual({
      identifier: "ramp_2",
      name: "Ramp (2)",
      locked: false,
    });
  });

  it("also collides against identifiers resolved earlier in the same batch", () => {
    const taken = new Set(["ramp"]);
    const first = resolveFeatureIdentity(polygon({ identifier: "ramp", name: "Ramp" }), 1, taken);
    const second = resolveFeatureIdentity(polygon({ identifier: "ramp", name: "Ramp" }), 2, taken);
    expect(first.identifier).toBe("ramp_2");
    expect(second.identifier).toBe("ramp_3");
    expect(second.name).toBe("Ramp (3)");
  });

  it("derives an identifier from the name when properties.identifier is missing or has spaces", () => {
    expect(resolveFeatureIdentity(polygon({ name: "North Ramp" }), 1, new Set()).identifier).toBe(
      "North_Ramp",
    );
    expect(
      resolveFeatureIdentity(polygon({ name: "North Ramp", identifier: "north ramp" }), 1, new Set())
        .identifier,
    ).toBe("North_Ramp");
  });

  it("synthesizes 'Imported {geometry} {index}' when neither name nor identifier is present", () => {
    expect(resolveFeatureIdentity(polygon({}), 3, new Set())).toEqual({
      identifier: "Imported_polygon_3",
      name: "Imported polygon 3",
      locked: false,
    });
    const line: ImportedFeature = {
      type: "Feature",
      geometry: { type: "LineString", coordinates: [[0, 0], [1, 1]] },
      properties: {},
    };
    expect(resolveFeatureIdentity(line, 2, new Set()).name).toBe("Imported line 2");
  });
});

describe("importAreasBatch", () => {
  it("round-trips an export-all file: every feature created, identifiers preserved", async () => {
    const exported = [
      polygon({ identifier: "alpha", name: "Alpha", locked: false }),
      polygon({ identifier: "bravo", name: "Bravo", locked: true }),
      polygon({ identifier: "charlie", name: "Charlie", locked: false }),
    ];
    const created: string[] = [];
    const result = await importAreasBatch(exported, [], async (identity) => {
      created.push(identity.identifier);
      return true;
    });
    expect(created).toEqual(["alpha", "bravo", "charlie"]);
    expect(result.created.map((c) => c.identifier)).toEqual(["alpha", "bravo", "charlie"]);
    expect(result.failed).toEqual([]);
  });

  it("re-importing while the originals still exist auto-suffixes instead of blocking", async () => {
    const file = [polygon({ identifier: "alpha", name: "Alpha" }), polygon({ identifier: "bravo", name: "Bravo" })];
    const result = await importAreasBatch(file, ["alpha", "bravo"], async () => true);
    expect(result.created.map((c) => c.identifier)).toEqual(["alpha_2", "bravo_2"]);
    expect(result.failed).toEqual([]);
  });

  it("creates the rest of the batch when one feature fails at createArea, and reports it", async () => {
    const file = [
      polygon({ identifier: "a", name: "A" }),
      polygon({ identifier: "b", name: "B" }),
      polygon({ identifier: "c", name: "C" }),
    ];
    const result = await importAreasBatch(file, [], async (identity) => identity.identifier !== "b");
    expect(result.created.map((c) => c.identifier)).toEqual(["a", "c"]);
    expect(result.failed.map((f) => f.identifier)).toEqual(["b"]);
  });

  it("treats a thrown error from createArea (network failure) as a per-feature failure", async () => {
    const file = [polygon({ identifier: "a", name: "A" }), polygon({ identifier: "b", name: "B" })];
    const result = await importAreasBatch(file, [], async (identity) => {
      if (identity.identifier === "a") throw new Error("network down");
      return true;
    });
    expect(result.created.map((c) => c.identifier)).toEqual(["b"]);
    expect(result.failed.map((f) => f.identifier)).toEqual(["a"]);
  });

  it("synthesizes names for a file of geometry-only features", async () => {
    const file = [polygon({}), polygon({}), polygon({})];
    const result = await importAreasBatch(file, [], async () => true);
    expect(result.created.map((c) => c.name)).toEqual([
      "Imported polygon 1",
      "Imported polygon 2",
      "Imported polygon 3",
    ]);
  });
});
