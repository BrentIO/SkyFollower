import { describe, expect, it } from "vitest";
import {
  destinationPoint,
  rangeRingLabelsFeatureCollection,
  rangeRingsFeatureCollection,
  ringCoordinates,
} from "./rangeRings";

// Reference values computed independently (Python, same spherical "reckon"
// formula, mean earth radius 3440.065nm) -- not derived from the
// implementation under test.
describe("destinationPoint", () => {
  it("moving north from the equator increases latitude, keeps longitude", () => {
    const [lon, lat] = destinationPoint({ latitude: 0, longitude: 0 }, 0, 100);
    expect(lon).toBeCloseTo(0, 9);
    expect(lat).toBeCloseTo(1.6655435148197002, 9);
  });

  it("moving south from the equator decreases latitude by the same amount", () => {
    const [lon, lat] = destinationPoint({ latitude: 0, longitude: 0 }, 180, 100);
    expect(lon).toBeCloseTo(0, 9);
    expect(lat).toBeCloseTo(-1.6655435148197002, 9);
  });

  it("moving east from the equator increases longitude, keeps latitude", () => {
    const [lon, lat] = destinationPoint({ latitude: 0, longitude: 0 }, 90, 100);
    expect(lon).toBeCloseTo(1.6655435148197002, 9);
    expect(lat).toBeCloseTo(0, 9);
  });

  it("is geodesically correct at mid-latitude, not a naive flat-projection circle", () => {
    const home = { latitude: 40.0, longitude: -75.0 };

    const [southLon, southLat] = destinationPoint(home, 180, 150);
    expect(southLon).toBeCloseTo(-75.0, 9);
    expect(southLat).toBeCloseTo(37.501684727770446, 9);

    const [northLon, northLat] = destinationPoint(home, 0, 150);
    expect(northLon).toBeCloseTo(-75.0, 9);
    expect(northLat).toBeCloseTo(42.49831527222954, 9);

    // Moving east at 40N sweeps more longitude than the same distance
    // would at the equator (~1.665deg/100nm scaled to 150nm = ~2.498deg) --
    // a flat/naive circle would get this wrong, since it ignores the
    // cos(latitude) compression of longitude away from the equator.
    const [eastLon, eastLat] = destinationPoint(home, 90, 150);
    expect(eastLon).toBeCloseTo(-71.74013459841028, 9);
    expect(eastLat).toBeCloseTo(39.95431839299283, 9);
    expect(Math.abs(eastLon - home.longitude)).toBeGreaterThan(2.498);
  });
});

describe("ringCoordinates", () => {
  it("returns a closed loop (first and last vertex identical)", () => {
    const coords = ringCoordinates({ latitude: 40, longitude: -75 }, 100);
    expect(coords.length).toBeGreaterThan(3);
    expect(coords[0][0]).toBeCloseTo(coords[coords.length - 1][0], 12);
    expect(coords[0][1]).toBeCloseTo(coords[coords.length - 1][1], 12);
  });

  it("its southernmost vertex matches the bearing-180 destination point", () => {
    const home = { latitude: 40, longitude: -75 };
    const radiusNm = 150;
    const coords = ringCoordinates(home, radiusNm);
    const minLat = Math.min(...coords.map(([, lat]) => lat));
    const [, expectedLat] = destinationPoint(home, 180, radiusNm);
    expect(minLat).toBeCloseTo(expectedLat, 6);
  });
});

describe("rangeRingsFeatureCollection", () => {
  it("returns no features when there's no home point", () => {
    expect(rangeRingsFeatureCollection(null).features).toHaveLength(0);
  });

  it("returns one LineString feature per radius, tagged with its radius", () => {
    const fc = rangeRingsFeatureCollection({ latitude: 40, longitude: -75 }, [100, 150, 200]);
    expect(fc.features).toHaveLength(3);
    expect(fc.features.map((f) => f.properties?.radiusNm)).toEqual([100, 150, 200]);
    for (const feature of fc.features) {
      expect(feature.geometry.type).toBe("LineString");
    }
  });
});

describe("rangeRingLabelsFeatureCollection", () => {
  it("returns no features when there's no home point", () => {
    expect(rangeRingLabelsFeatureCollection(null).features).toHaveLength(0);
  });

  it("places one labeled point per radius at its southernmost point", () => {
    const home = { latitude: 40, longitude: -75 };
    const fc = rangeRingLabelsFeatureCollection(home, [100, 150, 200]);
    expect(fc.features).toHaveLength(3);
    expect(fc.features.map((f) => f.properties?.label)).toEqual(["100 nmi", "150 nmi", "200 nmi"]);
    for (const feature of fc.features) {
      expect(feature.geometry.type).toBe("Point");
      const [lon, lat] = (feature.geometry as GeoJSON.Point).coordinates;
      expect(lon).toBeCloseTo(home.longitude, 6);
      expect(lat).toBeLessThan(home.latitude);
    }
  });
});
