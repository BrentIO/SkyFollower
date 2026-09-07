import { describe, expect, it } from "vitest";
import { altitudeColor } from "./altitudeColor";
import type { TrailPoint } from "./aircraftState";
import { buildTrailSegments } from "./trailSegments";

describe("buildTrailSegments", () => {
  it("produces no segments for an empty trail", () => {
    expect(buildTrailSegments([])).toEqual([]);
  });

  it("produces no segments for a single-point trail", () => {
    const trail: TrailPoint[] = [{ latitude: 1, longitude: 2, altitude: 1000 }];
    expect(buildTrailSegments(trail)).toEqual([]);
  });

  it("produces N-1 two-point segments for an N-point trail", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 5000 },
      { latitude: 3, longitude: 3, altitude: 15000 },
      { latitude: 4, longitude: 4, altitude: 35000 },
    ];
    const segments = buildTrailSegments(trail);
    expect(segments).toHaveLength(3);
  });

  it("colors each segment by its earlier point's altitude, not the later one", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 35000 },
    ];
    const [segment] = buildTrailSegments(trail);
    expect(segment.color).toBe(altitudeColor(1000));
    expect(segment.color).not.toBe(altitudeColor(35000));
  });

  it("builds segment coordinates as [from, to] in [longitude, latitude] order", () => {
    const trail: TrailPoint[] = [
      { latitude: 33.94, longitude: -118.4, altitude: 1000 },
      { latitude: 34.05, longitude: -118.2, altitude: 2000 },
    ];
    const [segment] = buildTrailSegments(trail);
    expect(segment.coordinates).toEqual([
      [-118.4, 33.94],
      [-118.2, 34.05],
    ]);
  });

  it("produces a black segment where the earlier point has a null altitude", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: null },
      { latitude: 2, longitude: 2, altitude: 5000 },
    ];
    const [segment] = buildTrailSegments(trail);
    expect(segment.color).toBe(altitudeColor(null));
    expect(segment.color).toBe("hsl(0, 0%, 0%)");
  });

  it("colors a transition into a null-altitude point by the earlier (known) point, and the segment after it black", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 5000 },
      { latitude: 2, longitude: 2, altitude: null },
      { latitude: 3, longitude: 3, altitude: 6000 },
    ];
    const segments = buildTrailSegments(trail);
    expect(segments[0].color).toBe(altitudeColor(5000));
    expect(segments[1].color).toBe(altitudeColor(null));
  });

  it("reflects a climb across several segments (each colored by where the aircraft actually was)", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 500 },
      { latitude: 2, longitude: 2, altitude: 10000 },
      { latitude: 3, longitude: 3, altitude: 35000 },
    ];
    const segments = buildTrailSegments(trail);
    expect(segments.map((s) => s.color)).toEqual([altitudeColor(500), altitudeColor(10000)]);
  });

  it("does not mutate the input trail", () => {
    const trail: TrailPoint[] = [
      { latitude: 1, longitude: 1, altitude: 1000 },
      { latitude: 2, longitude: 2, altitude: 2000 },
    ];
    const copy = JSON.parse(JSON.stringify(trail));
    buildTrailSegments(trail);
    expect(trail).toEqual(copy);
  });
});
