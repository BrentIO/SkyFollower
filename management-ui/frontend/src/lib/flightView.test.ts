import { describe, expect, it } from "vitest";
import {
  airportLocation,
  altitudeColor,
  boundsOf,
  darkenColor,
  flightPathFeature,
  formatDuration,
  formatTraceLabel,
  lineGradientExpression,
  traceLabelSortKey,
  tracePointsFeatureCollection,
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
  it("returns pure black for unknown/null altitude", () => {
    expect(altitudeColor(null)).toBe("hsl(0, 0%, 0%)");
  });

  it("matches tar1090's ported table at 16000ft (verified reference value)", () => {
    // https://github.com/wiedehopf/tar1090 ColorByAlt.air, interpolated at
    // the raw (unquantized) altitude -- this is the value the issue calls
    // out as matching tar1090's own #0bc09c sample at 16000ft.
    expect(altitudeColor(16000)).toBe("hsl(167.6, 88.0%, 40.0%)");
  });

  it("is at or below ground level (0ft) with a low, orange-brown hue", () => {
    expect(altitudeColor(0)).toBe("hsl(20.0, 88.0%, 50.0%)");
    // Altitudes at or below the table's floor don't extrapolate past it.
    expect(altitudeColor(-2000)).toBe("hsl(20.0, 88.0%, 50.0%)");
  });

  it("interpolates smoothly between table breakpoints rather than banding", () => {
    // 1125ft falls between the 0ft/2000ft h-breakpoints and the 20/32
    // l-breakpoints -- neither an exact table value.
    expect(altitudeColor(1125)).toBe("hsl(27.0, 88.0%, 52.3%)");
  });

  it("reaches a teal/cyan hue at cruise-adjacent mid altitudes", () => {
    expect(altitudeColor(21500)).toBe("hsl(197.9, 88.0%, 49.9%)");
  });

  it("extrapolates flat beyond the table's highest breakpoint (51000ft)", () => {
    // Both altitudes fall on/after the last h-breakpoint (51000ft, val
    // 360 -> wraps to hue 0), so the color no longer changes past there.
    expect(altitudeColor(51000)).toBe(altitudeColor(100000));
  });
});

describe("darkenColor", () => {
  it("reduces lightness by 10 percentage points, keeping hue/saturation", () => {
    expect(darkenColor("hsl(167.6, 88.0%, 40.0%)")).toBe("hsl(167.6, 88.0%, 30.0%)");
  });

  it("clamps lightness at 0 rather than going negative", () => {
    expect(darkenColor("hsl(0, 0%, 5.0%)")).toBe("hsl(0, 0%, 0.0%)");
  });

  it("returns the input unchanged if it isn't a recognizable hsl() string", () => {
    expect(darkenColor("not-a-color")).toBe("not-a-color");
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

  it("drops a part equal to the one immediately before it", () => {
    // Real-world case: ELLX (Luxembourg-Findel) has city/region/country
    // all literally "Luxembourg" once region is resolved from a name
    // rather than left as a raw ISO code.
    expect(
      airportLocation({ icao_code: "ELLX", city: "Luxembourg", region: "Luxembourg", country: "Luxembourg" }),
    ).toBe("Luxembourg");
  });

  it("keeps non-adjacent repeats (only adjacent duplicates are dropped)", () => {
    expect(airportLocation({ icao_code: "KDFW", city: "Dallas", region: "TX", country: "Dallas" })).toBe(
      "Dallas, TX, Dallas",
    );
  });
});

describe("traceLabelSortKey", () => {
  it("gives the first and last point the top priority (0)", () => {
    expect(traceLabelSortKey(0, 10)).toBe(0);
    expect(traceLabelSortKey(9, 10)).toBe(0);
  });

  it("gives the midpoint the next priority tier", () => {
    // 9 points, indices 0-8: midpoint is index 4.
    expect(traceLabelSortKey(4, 9)).toBe(1);
  });

  it("gives quarter-points a lower priority than the midpoint", () => {
    const mid = traceLabelSortKey(4, 9);
    const quarter = traceLabelSortKey(2, 9);
    expect(quarter).toBeGreaterThan(mid);
  });

  it("is symmetric around the midpoint", () => {
    // 9 points: index 2 and index 6 are equidistant from the midpoint (4).
    expect(traceLabelSortKey(2, 9)).toBe(traceLabelSortKey(6, 9));
  });

  it("handles a single point without dividing by zero", () => {
    expect(traceLabelSortKey(0, 1)).toBe(0);
  });

  it("every index gets a finite, non-negative key (never loops forever)", () => {
    const total = 37; // odd, deliberately awkward for bisection
    for (let i = 0; i < total; i++) {
      const key = traceLabelSortKey(i, total);
      expect(Number.isFinite(key)).toBe(true);
      expect(key).toBeGreaterThanOrEqual(0);
    }
  });
});

describe("formatTraceLabel", () => {
  it("renders both measurements and the time on two lines", () => {
    // 1785499200 = 2026-07-31T12:00:00Z; formatting is locale/zone-dependent
    // (matches the rest of the modal's toLocaleTimeString() usage), so only
    // the first line -- which doesn't depend on the viewer's zone -- is
    // asserted verbatim.
    const label = formatTraceLabel(416, 16050, 1785499200);
    const lines = label.split("\n");
    expect(lines[0]).toBe("416 kt  16050 ft");
    expect(lines).toHaveLength(2);
  });

  it("omits the speed segment when speed is unavailable", () => {
    expect(formatTraceLabel(null, 16050, null)).toBe("16050 ft");
  });

  it("omits the altitude segment when altitude is unavailable", () => {
    expect(formatTraceLabel(416, null, null)).toBe("416 kt");
  });

  it("omits the time line entirely when there is no timestamp", () => {
    const label = formatTraceLabel(416, 16050, null);
    expect(label.includes("\n")).toBe(false);
  });

  it("renders just an empty first line when both measurements are unavailable", () => {
    expect(formatTraceLabel(null, null, null)).toBe("");
  });
});

describe("tracePointsFeatureCollection", () => {
  it("builds one Point feature per coordinate, 2D (color/label live in properties, not geometry)", () => {
    const coords = [
      [-84.0, 33.0, 1000],
      [-85.0, 34.0, 2000],
    ];
    const fc = tracePointsFeatureCollection(coords, [1785499200, 1785499210], [400, 420]);
    expect(fc.type).toBe("FeatureCollection");
    expect(fc.features).toHaveLength(2);
    expect(fc.features[0].geometry).toEqual({ type: "Point", coordinates: [-84.0, 33.0] });
  });

  it("colors each point from its own altitude, not a shared color", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 40000],
    ];
    const fc = tracePointsFeatureCollection(coords, [null, null], [null, null]);
    expect(fc.features[0].properties.color).not.toBe(fc.features[1].properties.color);
    expect(fc.features[0].properties.color).toBe(altitudeColor(0));
    expect(fc.features[1].properties.color).toBe(altitudeColor(40000));
  });

  it("gives each point a strokeColor that is a darker shade of its own color", () => {
    const coords = [
      [-84.0, 33.0, 0],
      [-85.0, 34.0, 40000],
    ];
    const fc = tracePointsFeatureCollection(coords, [null, null], [null, null]);
    expect(fc.features[0].properties.strokeColor).toBe(darkenColor(altitudeColor(0)));
    expect(fc.features[1].properties.strokeColor).toBe(darkenColor(altitudeColor(40000)));
    // Not a flat near-black outline shared across every point.
    expect(fc.features[0].properties.strokeColor).not.toBe(fc.features[1].properties.strokeColor);
  });

  it("handles a 2D-only coordinate (no altitude) without crashing", () => {
    const fc = tracePointsFeatureCollection([[-84.0, 33.0]], [null], [400]);
    expect(fc.features[0].properties.label).toBe("400 kt");
    expect(fc.features[0].properties.color).toBe(altitudeColor(null));
  });

  it("assigns sortKey consistent with traceLabelSortKey", () => {
    const coords = [
      [-84.0, 33.0, 1000],
      [-85.0, 34.0, 2000],
      [-86.0, 35.0, 3000],
    ];
    const fc = tracePointsFeatureCollection(coords, [1, 2, 3], [100, 200, 300]);
    expect(fc.features.map((f) => f.properties.sortKey)).toEqual([
      traceLabelSortKey(0, 3),
      traceLabelSortKey(1, 3),
      traceLabelSortKey(2, 3),
    ]);
  });

  it("treats a missing coordTimes/coordSpeeds entry the same as null (index out of range)", () => {
    const fc = tracePointsFeatureCollection([[-84.0, 33.0, 1000]], [], []);
    expect(fc.features[0].properties.label).toBe("1000 ft");
  });
});
