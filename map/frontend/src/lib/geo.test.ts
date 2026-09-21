import { describe, expect, it } from "vitest";
import { greatCircleNm, initialBearing } from "./geo";

// Reference values mirrored from map/tests/test_geo.py's great_circle_nm
// suite -- this is a port of the same haversine formula, so the same
// input/output pairs must hold here too.
describe("greatCircleNm", () => {
  it("is about 60.04nm per degree of latitude, independent of longitude", () => {
    expect(
      greatCircleNm({ latitude: 0, longitude: 0 }, { latitude: 1, longitude: 0 }),
    ).toBeCloseTo(60.04, 1);
    expect(
      greatCircleNm({ latitude: 45, longitude: 10 }, { latitude: 46, longitude: 10 }),
    ).toBeCloseTo(60.04, 1);
  });

  it("is zero for two identical points", () => {
    expect(
      greatCircleNm({ latitude: 33.9, longitude: -118.4 }, { latitude: 33.9, longitude: -118.4 }),
    ).toBe(0);
  });

  it("is half the earth's circumference for antipodal points", () => {
    const half = Math.PI * 3440.065;
    expect(
      greatCircleNm({ latitude: 0, longitude: 0 }, { latitude: 0, longitude: 180 }),
    ).toBeCloseTo(half, 6);
  });
});

// Reference values mirrored from map/tests/test_geo.py's initial_bearing
// suite -- this is a port of the same formula, so the same input/output
// pairs must hold here too.
describe("initialBearing", () => {
  it("gives the four cardinal directions exactly", () => {
    expect(initialBearing({ latitude: 0, longitude: 0 }, { latitude: 1, longitude: 0 })).toBe(0); // due north
    expect(
      initialBearing({ latitude: 0, longitude: 0 }, { latitude: 0, longitude: 1 }),
    ).toBeCloseTo(90, 6); // due east
    expect(
      initialBearing({ latitude: 0, longitude: 0 }, { latitude: -1, longitude: 0 }),
    ).toBeCloseTo(180, 6); // due south
    expect(
      initialBearing({ latitude: 0, longitude: 0 }, { latitude: 0, longitude: -1 }),
    ).toBeCloseTo(270, 6); // due west
  });

  it("is normalised to [0, 360)", () => {
    // London -> New York, roughly WNW.
    const b = initialBearing({ latitude: 51.5, longitude: -0.1 }, { latitude: 40.7, longitude: -74.0 });
    expect(b).toBeGreaterThanOrEqual(0);
    expect(b).toBeLessThan(360);
    expect(b).toBeGreaterThan(280);
    expect(b).toBeLessThan(300);
  });

  it("is zero (a degenerate result, not NaN) for two identical points", () => {
    expect(
      initialBearing({ latitude: 33.9, longitude: -118.4 }, { latitude: 33.9, longitude: -118.4 }),
    ).toBe(0);
  });
});
