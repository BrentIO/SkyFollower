import { describe, expect, it } from "vitest";
import {
  airportLocation,
  altitudeColor,
  boundsOf,
  flightPathFeature,
  formatDuration,
  lineGradientExpression,
} from "./flightView";

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

describe("flightPathFeature", () => {
  it("produces a single LineString feature containing every coordinate in order", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 1000],
      [-86.0, 35.0, 2000],
    ];
    const fc = flightPathFeature(coords);
    expect(fc.features).toHaveLength(1);
    expect(fc.features[0].geometry.type).toBe("LineString");
    expect(fc.features[0].geometry.coordinates).toEqual([
      [-84.0, 33.0],
      [-85.0, 34.0],
      [-86.0, 35.0],
    ]);
  });

  it("strips altitude, keeping only [lon, lat]", () => {
    const fc = flightPathFeature([
      [-84.0, 33.0, 5000],
      [-85.0, 34.0, 6000],
    ]);
    expect(fc.features[0].geometry.coordinates[0]).toHaveLength(2);
  });
});

describe("lineGradientExpression", () => {
  it("starts with the interpolate/linear/line-progress expression head", () => {
    const expr = lineGradientExpression([
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 1000],
    ]);
    expect(expr.slice(0, 3)).toEqual(["interpolate", ["linear"], ["line-progress"]]);
  });

  it("emits one progress/color stop pair per point, colored by that point's own altitude", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 2000],
      [-86.0, 35.0, 4000],
    ];
    const expr = lineGradientExpression(coords);
    const stops = expr.slice(3);
    expect(stops).toHaveLength(coords.length * 2);
    expect(stops[0]).toBe(0); // first point is always at progress 0
    expect(stops[1]).toBe(altitudeColor(0));
    expect(stops[stops.length - 2]).toBe(1); // last point is always at progress 1
    expect(stops[stops.length - 1]).toBe(altitudeColor(4000));
    // Middle stop colored by its own altitude, not an average of neighbors.
    expect(stops[3]).toBe(altitudeColor(2000));
  });

  it("produces strictly increasing progress stops", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-84.5, 33.2, 500],
      [-85.0, 34.0, 1000],
      [-86.0, 35.0, 2000],
    ];
    const expr = lineGradientExpression(coords);
    const stops = expr.slice(3);
    const progresses: number[] = [];
    for (let i = 0; i < stops.length; i += 2) progresses.push(stops[i] as number);
    for (let i = 1; i < progresses.length; i++) {
      expect(progresses[i]).toBeGreaterThan(progresses[i - 1]);
    }
  });

  it("nudges apart consecutive duplicate/zero-distance points instead of producing equal stops", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-84.0, 33.0, 100], // duplicate position, would collapse to the same progress
      [-85.0, 34.0, 2000],
    ];
    const expr = lineGradientExpression(coords);
    const stops = expr.slice(3);
    const progresses = [stops[0], stops[2], stops[4]] as number[];
    expect(progresses[1]).toBeGreaterThan(progresses[0]);
    expect(progresses[2]).toBeGreaterThan(progresses[1]);
  });

  it("handles a single-point path without throwing", () => {
    const expr = lineGradientExpression([[-84.0, 33.0, 1000]]);
    const stops = expr.slice(3);
    expect(stops[1]).toBe(altitudeColor(1000));
    expect(stops[3]).toBe(altitudeColor(1000));
  });

  it("handles an empty path without throwing", () => {
    const expr = lineGradientExpression([]);
    expect(expr[0]).toBe("interpolate");
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
