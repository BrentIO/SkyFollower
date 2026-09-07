import { describe, expect, it } from "vitest";
import { DEFAULT_INFO_BOX_OFFSET, placeInfoBoxes, type AircraftBoxInput } from "./placement";

function rectsOverlap(
  a: { x: number; y: number; width: number; height: number },
  b: { x: number; y: number; width: number; height: number },
): boolean {
  return !(a.x + a.width <= b.x || b.x + b.width <= a.x || a.y + a.height <= b.y || b.y + b.height <= a.y);
}

describe("placeInfoBoxes", () => {
  it("places a single aircraft's box at the default south-east offset, not nudged", () => {
    const inputs: AircraftBoxInput[] = [{ id: "A1B2C3", x: 100, y: 100, width: 120, height: 50 }];
    const [placement] = placeInfoBoxes(inputs);
    expect(placement.x).toBe(100 + DEFAULT_INFO_BOX_OFFSET);
    expect(placement.y).toBe(100 + DEFAULT_INFO_BOX_OFFSET);
    expect(placement.nudged).toBe(false);
    expect(placement.anchorX).toBe(100);
    expect(placement.anchorY).toBe(100);
  });

  it("leaves two far-apart aircraft both at their default offset", () => {
    const inputs: AircraftBoxInput[] = [
      { id: "AAAAAA", x: 0, y: 0, width: 120, height: 50 },
      { id: "BBBBBB", x: 800, y: 800, width: 120, height: 50 },
    ];
    const placements = placeInfoBoxes(inputs);
    expect(placements.every((p) => !p.nudged)).toBe(true);
  });

  it("nudges a colliding box further out, and the two no longer overlap", () => {
    // Two aircraft icons close enough together that their default-offset
    // boxes would overlap.
    const inputs: AircraftBoxInput[] = [
      { id: "AAAAAA", x: 100, y: 100, width: 120, height: 50 },
      { id: "AAAAAB", x: 110, y: 110, width: 120, height: 50 },
    ];
    const placements = placeInfoBoxes(inputs);
    expect(placements).toHaveLength(2);
    expect(rectsOverlap(placements[0], placements[1])).toBe(false);
    // The first-sorted (AAAAAA) box stays at its default offset; the
    // second (AAAAAB) is the one that gets nudged out of its way.
    const first = placements.find((p) => p.id === "AAAAAA")!;
    const second = placements.find((p) => p.id === "AAAAAB")!;
    expect(first.nudged).toBe(false);
    expect(second.nudged).toBe(true);
  });

  it("never hides a box -- every input produces exactly one placement", () => {
    const inputs: AircraftBoxInput[] = Array.from({ length: 12 }, (_, i) => ({
      id: `HEX${String(i).padStart(3, "0")}`,
      // Clustered tightly so every box collides with several others.
      x: 200 + (i % 4) * 5,
      y: 200 + Math.floor(i / 4) * 5,
      width: 120,
      height: 50,
    }));
    const placements = placeInfoBoxes(inputs);
    expect(placements).toHaveLength(inputs.length);
    expect(new Set(placements.map((p) => p.id)).size).toBe(inputs.length);
  });

  it("resolves a tightly-clustered group so no two boxes overlap", () => {
    const inputs: AircraftBoxInput[] = Array.from({ length: 8 }, (_, i) => ({
      id: `HEX${String(i).padStart(3, "0")}`,
      x: 300 + i * 3,
      y: 300 + i * 3,
      width: 120,
      height: 50,
    }));
    const placements = placeInfoBoxes(inputs);
    for (let i = 0; i < placements.length; i++) {
      for (let j = i + 1; j < placements.length; j++) {
        expect(rectsOverlap(placements[i], placements[j])).toBe(false);
      }
    }
  });

  it("is deterministic regardless of input array order", () => {
    const a: AircraftBoxInput = { id: "AAAAAA", x: 100, y: 100, width: 120, height: 50 };
    const b: AircraftBoxInput = { id: "BBBBBB", x: 105, y: 105, width: 120, height: 50 };
    const order1 = placeInfoBoxes([a, b]);
    const order2 = placeInfoBoxes([b, a]);
    const byId = (arr: typeof order1) => Object.fromEntries(arr.map((p) => [p.id, p]));
    expect(byId(order1)).toEqual(byId(order2));
  });

  it("draws a leader line origin at the aircraft icon's own position, not the box", () => {
    const inputs: AircraftBoxInput[] = [
      { id: "AAAAAA", x: 100, y: 100, width: 120, height: 50 },
      { id: "AAAAAB", x: 105, y: 105, width: 120, height: 50 },
    ];
    const placements = placeInfoBoxes(inputs);
    const nudgedOne = placements.find((p) => p.nudged);
    expect(nudgedOne).toBeDefined();
    const source = inputs.find((i) => i.id === nudgedOne!.id)!;
    expect(nudgedOne!.anchorX).toBe(source.x);
    expect(nudgedOne!.anchorY).toBe(source.y);
  });

  it("returns an empty array for no aircraft", () => {
    expect(placeInfoBoxes([])).toEqual([]);
  });
});
