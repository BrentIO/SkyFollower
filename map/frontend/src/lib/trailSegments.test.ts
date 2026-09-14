import { describe, expect, it } from "vitest";
import { altitudeColor } from "./altitudeColor";
import type { TrailPoint } from "./aircraftState";
import { buildTrailRuns } from "./trailSegments";

describe("buildTrailRuns", () => {
  it("produces no runs for an empty trail", () => {
    expect(buildTrailRuns([])).toEqual([]);
  });

  it("produces no runs for a single-point trail", () => {
    const trail: TrailPoint[] = [{ latitude: 1, longitude: 2, altitude: 1000 }];
    expect(buildTrailRuns(trail)).toEqual([]);
  });

  it("groups a steady-altitude trail (no color changes) into exactly one run spanning every point", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 35000 },
      { latitude: 2, longitude: 2, altitude: 35000 },
      { latitude: 3, longitude: 3, altitude: 35000 },
      { latitude: 4, longitude: 4, altitude: 35000 },
    ];
    const runs = buildTrailRuns(trail);
    expect(runs).toHaveLength(1);
    expect(runs[0].coordinates).toEqual([
      [1, 1],
      [2, 2],
      [3, 3],
      [4, 4],
    ]);
    expect(runs[0].color).toBe(altitudeColor(35000));
  });

  it("splits into a new run only where the color actually changes, not on every point", () => {
    // #1820: this is the whole point -- N points at the same altitude
    // bucket must never produce N-1 separate features.
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 5000 },
      { latitude: 3, longitude: 3, altitude: 15000 },
      { latitude: 4, longitude: 4, altitude: 35000 },
    ];
    const runs = buildTrailRuns(trail);
    // Every consecutive altitude here lands in a different altitudeColor()
    // bucket, so this degenerates to the old one-run-per-segment shape --
    // asserting that explicitly, alongside the steady-altitude case above,
    // pins both ends of the behavior.
    expect(runs).toHaveLength(3);
  });

  it("colors each run by its starting point's altitude, not its ending one", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 35000 },
    ];
    const [run] = buildTrailRuns(trail);
    expect(run.color).toBe(altitudeColor(1000));
    expect(run.color).not.toBe(altitudeColor(35000));
  });

  it("builds run coordinates in [longitude, latitude] order", () => {
    const trail: TrailPoint[] = [
      { latitude: 33.94, longitude: -118.4, altitude: 1000 },
      { latitude: 34.05, longitude: -118.2, altitude: 1000 },
    ];
    const [run] = buildTrailRuns(trail);
    expect(run.coordinates).toEqual([
      [-118.4, 33.94],
      [-118.2, 34.05],
    ]);
  });

  it("produces a black run where the earlier point has a null altitude", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: null },
      { latitude: 2, longitude: 2, altitude: 5000 },
    ];
    const [run] = buildTrailRuns(trail);
    expect(run.color).toBe(altitudeColor(null));
    expect(run.color).toBe("hsl(0, 0%, 0%)");
  });

  it("colors a transition into a null-altitude point by the earlier (known) point, and starts a new run at it", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 5000 },
      { latitude: 2, longitude: 2, altitude: null },
      { latitude: 3, longitude: 3, altitude: 6000 },
    ];
    const runs = buildTrailRuns(trail);
    // Only 2 segments exist in a 3-point trail (p0->p1, p1->p2), each
    // colored by its *earlier* point -- there's no 4th point for p2's own
    // altitude to color anything by itself.
    expect(runs.map((r) => r.color)).toEqual([altitudeColor(5000), altitudeColor(null)]);
  });

  it("reflects a climb across several runs (each colored by where the aircraft actually was)", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 500 },
      { latitude: 2, longitude: 2, altitude: 10000 },
      { latitude: 3, longitude: 3, altitude: 35000 },
    ];
    const runs = buildTrailRuns(trail);
    expect(runs.map((r) => r.color)).toEqual([altitudeColor(500), altitudeColor(10000)]);
  });

  it("shares the boundary point between the run that ends there and the run that starts there -- grouping must not drop or duplicate a rendered coordinate's connectivity", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 1000 }, // still low -- same run as above
      { latitude: 3, longitude: 3, altitude: 35000 }, // climbs -- new run starts here
      { latitude: 4, longitude: 4, altitude: 35000 },
    ];
    const runs = buildTrailRuns(trail);
    expect(runs).toHaveLength(2);
    expect(runs[0].coordinates.at(-1)).toEqual([3, 3]);
    expect(runs[1].coordinates[0]).toEqual([3, 3]);
  });

  it("does not mutate the input trail", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 1000 },
    ];
    const copy = JSON.parse(JSON.stringify(trail));
    buildTrailRuns(trail);
    expect(trail).toEqual(copy);
  });
});
