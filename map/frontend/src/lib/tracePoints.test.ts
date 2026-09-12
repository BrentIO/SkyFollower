import { describe, expect, it } from "vitest";
import type { TracePoint } from "./aircraftState";
import { altitudeColor, darkenColor } from "./altitudeColor";
import { formatTraceLabel, traceLabelSortKey, tracePointsFeatureCollection } from "./tracePoints";

// Reference values mirrored from management-ui/frontend/src/lib/
// flightView.test.ts's traceLabelSortKey/formatTraceLabel/
// tracePointsFeatureCollection suites -- same input/output pairs must hold
// here too, since the rendering logic is a verbatim port (see this
// module's docstring for why tracePointsFeatureCollection's *signature*
// differs -- one TracePoint[] instead of three parallel arrays -- while
// its behavior does not).

describe("traceLabelSortKey", () => {
  it("gives the first and last point the top priority (0)", () => {
    expect(traceLabelSortKey(0, 10)).toBe(0);
    expect(traceLabelSortKey(9, 10)).toBe(0);
  });

  it("gives the midpoint the next priority tier", () => {
    expect(traceLabelSortKey(4, 9)).toBe(1);
  });

  it("gives quarter-points a lower priority than the midpoint", () => {
    const mid = traceLabelSortKey(4, 9);
    const quarter = traceLabelSortKey(2, 9);
    expect(quarter).toBeGreaterThan(mid);
  });

  it("is symmetric around the midpoint", () => {
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
    // (matches this frontend's other toLocaleTimeString() usage), so only
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

function point(overrides: Partial<TracePoint>): TracePoint {
  return { latitude: 33.0, longitude: -84.0, altitude: null, velocity: null, epochSeconds: 0, ...overrides };
}

describe("tracePointsFeatureCollection", () => {
  it("builds one Point feature per sample, 2D (color/label live in properties, not geometry)", () => {
    const points: TracePoint[] = [
      point({ latitude: 33.0, longitude: -84.0, altitude: 1000 }),
      point({ latitude: 34.0, longitude: -85.0, altitude: 2000 }),
    ];
    const fc = tracePointsFeatureCollection(points);
    expect(fc.type).toBe("FeatureCollection");
    expect(fc.features).toHaveLength(2);
    expect(fc.features[0].geometry).toEqual({ type: "Point", coordinates: [-84.0, 33.0] });
  });

  it("colors each point from its own altitude, not a shared color", () => {
    const points: TracePoint[] = [point({ altitude: 0 }), point({ altitude: 40000 })];
    const fc = tracePointsFeatureCollection(points);
    expect(fc.features[0].properties.color).not.toBe(fc.features[1].properties.color);
    expect(fc.features[0].properties.color).toBe(altitudeColor(0));
    expect(fc.features[1].properties.color).toBe(altitudeColor(40000));
  });

  it("gives each point a strokeColor that is a darker shade of its own color", () => {
    const points: TracePoint[] = [point({ altitude: 0 }), point({ altitude: 40000 })];
    const fc = tracePointsFeatureCollection(points);
    expect(fc.features[0].properties.strokeColor).toBe(darkenColor(altitudeColor(0)));
    expect(fc.features[1].properties.strokeColor).toBe(darkenColor(altitudeColor(40000)));
    // Not a flat near-black outline shared across every point.
    expect(fc.features[0].properties.strokeColor).not.toBe(fc.features[1].properties.strokeColor);
  });

  it("handles a sample with no altitude without crashing", () => {
    // epochSeconds is always a real number on a TracePoint (see
    // aircraftState.ts's pushTracePoint -- unlike altitude/velocity, it's
    // never actually null in this frontend's own data), so the label's
    // first line is asserted rather than the whole string (formatTraceLabel's
    // own test suite covers the no-timestamp case directly).
    const fc = tracePointsFeatureCollection([point({ altitude: null, velocity: 400 })]);
    expect(fc.features[0].properties.label.split("\n")[0]).toBe("400 kt");
    expect(fc.features[0].properties.color).toBe(altitudeColor(null));
  });

  it("assigns sortKey consistent with traceLabelSortKey", () => {
    const points: TracePoint[] = [
      point({ altitude: 1000 }),
      point({ altitude: 2000 }),
      point({ altitude: 3000 }),
    ];
    const fc = tracePointsFeatureCollection(points);
    expect(fc.features.map((f) => f.properties.sortKey)).toEqual([
      traceLabelSortKey(0, 3),
      traceLabelSortKey(1, 3),
      traceLabelSortKey(2, 3),
    ]);
  });

  it("returns an empty FeatureCollection for no samples", () => {
    const fc = tracePointsFeatureCollection([]);
    expect(fc.features).toEqual([]);
  });
});
