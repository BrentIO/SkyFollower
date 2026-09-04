import { describe, expect, it } from "vitest";
import { airportLocation, altitudeColor, boundsOf, formatDuration, segmentFeatures } from "./flightView";

describe("formatDuration", () => {
  it("shows all three units when all are non-zero", () => {
    expect(formatDuration("2026-03-14T08:00:00Z", "2026-03-14T09:17:12Z")).toBe("1h 17m 12s");
  });

  it("drops hours when zero", () => {
    expect(formatDuration("2026-03-14T08:00:00Z", "2026-03-14T08:17:12Z")).toBe("17m 12s");
  });

  it("drops minutes when zero, keeping hours and seconds", () => {
    expect(formatDuration("2026-03-14T08:00:00Z", "2026-03-15T10:00:05Z")).toBe("26h 5s");
  });

  it("drops seconds when zero", () => {
    expect(formatDuration("2026-03-14T08:00:00Z", "2026-03-14T09:17:00Z")).toBe("1h 17m");
  });

  it("never converts hours to days for long durations", () => {
    expect(formatDuration("2026-03-14T00:00:00Z", "2026-03-15T06:04:00Z")).toBe("30h 4m");
  });

  it("shows 0s rather than an empty string for a zero-length duration", () => {
    expect(formatDuration("2026-03-14T08:00:00Z", "2026-03-14T08:00:00Z")).toBe("0s");
  });
});

describe("altitudeColor", () => {
  it("returns a neutral gray for unknown altitude", () => {
    expect(altitudeColor(null)).toBe("hsl(0, 0%, 55%)");
  });

  it("returns red (hue 0) at or below the ground floor", () => {
    expect(altitudeColor(-2000)).toBe("hsl(0, 85%, 50%)");
  });

  it("clamps to violet (hue 300) at or above the ceiling", () => {
    expect(altitudeColor(45000)).toBe("hsl(300, 85%, 50%)");
    expect(altitudeColor(100000)).toBe("hsl(300, 85%, 50%)");
  });

  it("interpolates hue linearly between the floor and ceiling", () => {
    // Midpoint: (-2000 + 45000) / 2 = 21500 -> hue 150
    expect(altitudeColor(21500)).toBe("hsl(150, 85%, 50%)");
  });
});

describe("segmentFeatures", () => {
  it("produces one fewer segment than there are points", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 1000],
      [-86.0, 35.0, 2000],
    ];
    const fc = segmentFeatures(coords);
    expect(fc.features).toHaveLength(2);
  });

  it("colors each segment by the average altitude of its two endpoints", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 2000],
    ];
    const fc = segmentFeatures(coords);
    expect(fc.features[0].properties.color).toBe(altitudeColor(1000));
  });

  it("falls back to a single known endpoint's altitude when the other is missing", () => {
    const coords = [
      [-84.0, 33.0, 3000],
      [-85.0, 34.0],
    ];
    const fc = segmentFeatures(coords);
    expect(fc.features[0].properties.color).toBe(altitudeColor(3000));
  });

  it("uses the neutral color when neither endpoint has an altitude", () => {
    const coords = [
      [-84.0, 33.0],
      [-85.0, 34.0],
    ];
    const fc = segmentFeatures(coords);
    expect(fc.features[0].properties.color).toBe(altitudeColor(null));
  });
});

describe("boundsOf", () => {
  it("returns the min/max lon/lat bounding box", () => {
    const coords = [
      [-84.0, 33.0],
      [-86.0, 35.0],
      [-85.0, 34.0],
    ];
    expect(boundsOf(coords)).toEqual([
      [-86.0, 33.0],
      [-84.0, 35.0],
    ]);
  });
});

describe("airportLocation", () => {
  it("joins city, region, and country when all are present", () => {
    expect(airportLocation({ icao_code: "KDFW", city: "Dallas", region: "TX", country: "US" })).toBe(
      "Dallas, TX, US",
    );
  });

  it("joins only the parts that are present", () => {
    expect(airportLocation({ icao_code: "ETAR", city: "Ramstein-Miesenbach", country: "DE" })).toBe(
      "Ramstein-Miesenbach, DE",
    );
  });

  it("returns null when none of the location parts are present", () => {
    expect(airportLocation({ icao_code: "KDOV" })).toBeNull();
  });
});
