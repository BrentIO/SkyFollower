import { describe, expect, it } from "vitest";
import { greatCircleNm } from "./geo";

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
