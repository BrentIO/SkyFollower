import { createExpression, v8 as styleSpecV8 } from "@maplibre/maplibre-gl-style-spec";
import { describe, expect, it } from "vitest";
import {
  infoBoxOffsetForZoom,
  infoBoxTextOffsetZoomExpression,
  INFO_BOX_TEXT_OFFSET_REFERENCE_PX,
  MAX_INFO_BOX_OFFSET,
  MIN_INFO_BOX_OFFSET,
} from "./infoBoxOffset";

describe("infoBoxOffsetForZoom", () => {
  it("returns the max offset at and above the high-zoom threshold", () => {
    expect(infoBoxOffsetForZoom(10)).toBe(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(15)).toBe(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(20)).toBe(MAX_INFO_BOX_OFFSET);
  });

  it("returns the min offset at and below the low-zoom threshold", () => {
    expect(infoBoxOffsetForZoom(4)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(1)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(0)).toBe(MIN_INFO_BOX_OFFSET);
  });

  it("ramps monotonically between the thresholds, visibly smaller when zoomed out", () => {
    const zoomedOut = infoBoxOffsetForZoom(5);
    const mid = infoBoxOffsetForZoom(7);
    const zoomedIn = infoBoxOffsetForZoom(9);

    expect(zoomedOut).toBeGreaterThan(MIN_INFO_BOX_OFFSET);
    expect(zoomedOut).toBeLessThan(mid);
    expect(mid).toBeLessThan(zoomedIn);
    expect(zoomedIn).toBeLessThan(MAX_INFO_BOX_OFFSET);
  });

  it("pins the cubic ease-in curve's values at representative zoom levels", () => {
    // A genuinely zoomed-out view (multi-state) keeps the existing small offset.
    expect(infoBoxOffsetForZoom(4)).toBeCloseTo(3, 2);

    // The "regional" 7-9 band -- where labels were reported floating --
    // still ramps in below the max, just against the smaller endpoints.
    expect(infoBoxOffsetForZoom(7)).toBeCloseTo(3.75, 2);
    expect(infoBoxOffsetForZoom(8)).toBeCloseTo(4.78, 2);
    expect(infoBoxOffsetForZoom(9)).toBeCloseTo(6.47, 2);

    // A close-in view keeps the full, still-tight anchored gap.
    expect(infoBoxOffsetForZoom(10)).toBeCloseTo(9, 2);
  });

  it("reduces the offset at the regional 7-9 band by more than a linear ramp would", () => {
    // A linear ramp spends its "budget" evenly across the whole zoom span,
    // so it would only be a 12-20% reduction off the max at the top of the
    // 7-9 band. The cubic curve reduces meaningfully more: at least 30% off
    // the max by zoom 8, and at least 15% off the max by zoom 9.
    const zoom8 = infoBoxOffsetForZoom(8);
    const zoom9 = infoBoxOffsetForZoom(9);

    expect(MAX_INFO_BOX_OFFSET - zoom8).toBeGreaterThan(0.3 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET));
    expect(MAX_INFO_BOX_OFFSET - zoom9).toBeGreaterThan(0.15 * (MAX_INFO_BOX_OFFSET - MIN_INFO_BOX_OFFSET));
  });

  it("never exceeds the max or drops below the min at fractional/extreme zooms", () => {
    expect(infoBoxOffsetForZoom(9.9)).toBeLessThanOrEqual(MAX_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(-5)).toBe(MIN_INFO_BOX_OFFSET);
    expect(infoBoxOffsetForZoom(100)).toBe(MAX_INFO_BOX_OFFSET);
  });
});

// #1808: components/MapView.tsx's INFO_BOX_LAYER_ID symbol layer has no
// per-frame JS callback to call infoBoxOffsetForZoom() from (unlike the
// removed DOM InfoBoxLayer.tsx, which called it once per throttled "move"
// tick) -- this builds the equivalent MapLibre `text-offset` zoom
// expression instead, evaluated GPU/style-engine-side.
describe("infoBoxTextOffsetZoomExpression", () => {
  const expr = infoBoxTextOffsetZoomExpression();

  it("is an interpolate/linear/zoom expression", () => {
    expect(expr[0]).toBe("interpolate");
    expect(expr[1]).toEqual(["linear"]);
    expect(expr[2]).toEqual(["zoom"]);
  });

  it("has an even number of stop entries (zoom, [dx, dy] pairs) after the three header elements", () => {
    const stops = expr.slice(3);
    expect(stops.length % 2).toBe(0);
    expect(stops.length).toBeGreaterThan(0);
  });

  it("stop zoom values are strictly increasing -- required by MapLibre's interpolate expression", () => {
    const stops = expr.slice(3);
    const zooms = stops.filter((_, i) => i % 2 === 0) as number[];
    for (let i = 1; i < zooms.length; i++) {
      expect(zooms[i]).toBeGreaterThan(zooms[i - 1]);
    }
  });

  // #1815: each stop's output is ["literal", [em, em]], not a bare
  // [em, em] -- MapLibre's expression parser treats every nested array as
  // a sub-expression unless wrapped in "literal", so a bare array output
  // failed style validation entirely and silently dropped the whole
  // INFO_BOX_LAYER_ID layer (see the "is a valid MapLibre expression"
  // test below, and infoBoxOffset.ts's own comment on this function).
  function stopOutput(stopValue: unknown): [number, number] {
    const [tag, value] = stopValue as ["literal", [number, number]];
    expect(tag).toBe("literal");
    return value;
  }

  it("every stop's [dx, dy] em value equals infoBoxOffsetForZoom(zoom)/referencePx, with dx === dy (diagonal down-right offset)", () => {
    const stops = expr.slice(3);
    for (let i = 0; i < stops.length; i += 2) {
      const zoom = stops[i] as number;
      const [dx, dy] = stopOutput(stops[i + 1]);
      const expectedEm = infoBoxOffsetForZoom(zoom) / INFO_BOX_TEXT_OFFSET_REFERENCE_PX;
      expect(dx).toBeCloseTo(expectedEm, 6);
      expect(dy).toBeCloseTo(expectedEm, 6);
    }
  });

  it("covers at least the MIN_OFFSET_ZOOM..MAX_OFFSET_ZOOM ramp's endpoints (zoom 4 and zoom 10)", () => {
    const stops = expr.slice(3);
    const zooms = stops.filter((_, i) => i % 2 === 0) as number[];
    expect(Math.min(...zooms)).toBeLessThanOrEqual(4);
    expect(Math.max(...zooms)).toBeGreaterThanOrEqual(10);
  });

  it("a custom referencePx scales every em value inversely", () => {
    const doubled = infoBoxTextOffsetZoomExpression(INFO_BOX_TEXT_OFFSET_REFERENCE_PX * 2);
    const baseStops = expr.slice(3);
    const doubledStops = doubled.slice(3);
    for (let i = 1; i < baseStops.length; i += 2) {
      const [baseDx] = stopOutput(baseStops[i]);
      const [doubledDx] = stopOutput(doubledStops[i]);
      expect(doubledDx).toBeCloseTo(baseDx / 2, 6);
    }
  });

  // #1815: components/MapView.tsx passes this expression straight through
  // to `map.addLayer(...)` as INFO_BOX_LAYER_ID's `text-offset`. Every
  // other test in this file only checks the plain-array *shape* of the
  // return value -- none of them ever asked MapLibre's own parser whether
  // it's a legal expression, which is exactly how a bare (non-"literal")
  // array stop output shipped in #1814: `map.addLayer` failed MapLibre's
  // internal style validation ("Expression name must be a string, but
  // found number instead. If you wanted a literal array, use ["literal",
  // [...]].", logged via console.error, not thrown/caught by this app)
  // and silently skipped adding the layer -- no info box ever rendered,
  // for any aircraft, regardless of selection/hover/"Labels: All".
  // Reproduced live via a headless-Chrome + mock-backend harness against
  // the pre-fix code; this test exercises the same real parser
  // (@maplibre/maplibre-gl-style-spec, the package maplibre-gl itself
  // uses internally) without needing a DOM/WebGL context, so it catches
  // the same class of bug in plain `vitest run`.
  it("is a valid MapLibre text-offset expression (#1815)", () => {
    // The style-spec package's own generated .d.ts doesn't match its runtime v8.json shape closely
    // enough for createExpression's parameter type (missing `transition`, among others) -- the object
    // itself is correct (it's literally the same v8.json data MapLibre validates real layers against),
    // so this narrows past a type-only mismatch rather than a real one.
    const result = createExpression(expr, "layout_symbol.text-offset", styleSpecV8.layout_symbol["text-offset"] as never);
    expect(result.result).toBe("success");
  });

  it("documents the #1815 bug: the same expression with bare (non-literal) array stop outputs fails MapLibre's real validation with its own 'use [\"literal\", [...]]' message", () => {
    const bareArrayStops = expr.slice(3).map((value, i) => (i % 2 === 1 ? (value as ["literal", unknown])[1] : value));
    const preFixExpr = ["interpolate", ["linear"], ["zoom"], ...bareArrayStops];
    const result = createExpression(preFixExpr, "layout_symbol.text-offset", styleSpecV8.layout_symbol["text-offset"] as never);
    expect(result.result).toBe("error");
    if (result.result === "error") {
      expect(result.value.some((e) => e.message.includes('use ["literal"'))).toBe(true);
    }
  });
});
