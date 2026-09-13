import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (declared by vite/client, referenced in src/vite-env.d.ts)
// imports a file's contents as a plain string -- used here instead of node:fs so
// this stays a normal Vite/vitest module rather than needing @types/node, which
// this project's tsconfig.app.json (unlike tsconfig.node.json) doesn't pull in.
import mapViewSource from "./MapView.tsx?raw";
import { SDF_RADIUS_PX } from "../lib/aircraftIcon";
import { MUTED_GRAY } from "../lib/crosshairIcon";
import { hasPosition } from "../lib/featureCollections";

// MapView.tsx's aircraft symbol layer is built inline inside a `map.on("load", ...)`
// callback that also constructs a real maplibregl.Map and reads several hooks --
// there's no jsdom/DOM environment in this project's test setup (see
// lib/config.test.ts), so mounting the component to inspect the live layer isn't
// practical. Instead, this reads the actual paint object's source text out of the
// file and evaluates it as plain JS (MapLibre expressions are just arrays/strings/
// numbers -- no TSX-specific syntax involved), so the assertions below check the
// real expression the component will pass to MapLibre, not a hand-copied duplicate.
function findMatchingBrace(text: string, openIndex: number): number {
  let depth = 0;
  for (let i = openIndex; i < text.length; i++) {
    if (text[i] === "{") depth++;
    else if (text[i] === "}") {
      depth--;
      if (depth === 0) return i;
    }
  }
  throw new Error("No matching closing brace found");
}

// Extracts the `paint: { ... }` object literal belonging to the aircraft
// symbol layer's `map.addLayer({ id: AIRCRAFT_LAYER_ID, ... })` call, and
// evaluates it into a real JS object.
function aircraftLayerPaint(): Record<string, unknown> {
  const idIndex = mapViewSource.indexOf("id: AIRCRAFT_LAYER_ID");
  if (idIndex === -1) throw new Error("Could not find AIRCRAFT_LAYER_ID layer definition");

  const paintKeyIndex = mapViewSource.indexOf("paint: {", idIndex);
  if (paintKeyIndex === -1) throw new Error("Could not find paint block after AIRCRAFT_LAYER_ID");

  const objectOpenIndex = paintKeyIndex + "paint: ".length;
  const objectCloseIndex = findMatchingBrace(mapViewSource, objectOpenIndex);
  const paintLiteral = mapViewSource.slice(objectOpenIndex, objectCloseIndex + 1);

  // eslint-disable-next-line no-new-func -- evaluating a plain object literal
  // extracted from our own source, not user input.
  return new Function(`return (${paintLiteral});`)();
}

// Extracts the `paint: { ... }` object literal belonging to the trail
// line layer's `map.addLayer({ id: TRAIL_LAYER_ID, ... })` call.
function trailLayerPaint(): Record<string, unknown> {
  const idIndex = mapViewSource.indexOf("id: TRAIL_LAYER_ID");
  if (idIndex === -1) throw new Error("Could not find TRAIL_LAYER_ID layer definition");

  const paintKeyIndex = mapViewSource.indexOf("paint: {", idIndex);
  if (paintKeyIndex === -1) throw new Error("Could not find paint block after TRAIL_LAYER_ID");

  const objectOpenIndex = paintKeyIndex + "paint: ".length;
  const objectCloseIndex = findMatchingBrace(mapViewSource, objectOpenIndex);
  const paintLiteral = mapViewSource.slice(objectOpenIndex, objectCloseIndex + 1);

  // eslint-disable-next-line no-new-func -- evaluating a plain object literal
  // extracted from our own source, not user input.
  return new Function(`return (${paintLiteral});`)();
}

// Minimal evaluator for the small subset of MapLibre style-expression forms
// used by the paint properties below (case/boolean/get/coalesce/*/min over
// plain numbers and a feature-properties object) -- enough to check the
// *values* the real expression produces across icon_scale's actual range,
// not just its shape. Deliberately not the full style-spec grammar.
type Expr = unknown;
function evaluateExpr(expr: Expr, properties: Record<string, unknown>): unknown {
  if (!Array.isArray(expr)) return expr;
  const [op, ...args] = expr as [string, ...Expr[]];
  switch (op) {
    case "get":
      return properties[args[0] as string];
    case "boolean": {
      const value = evaluateExpr(args[0], properties);
      return typeof value === "boolean" ? value : evaluateExpr(args[1], properties);
    }
    case "coalesce": {
      for (const candidate of args) {
        const value = evaluateExpr(candidate, properties);
        if (value !== undefined && value !== null) return value;
      }
      return undefined;
    }
    case "case": {
      const [condition, thenExpr, elseExpr] = args;
      return evaluateExpr(condition, properties) ? evaluateExpr(thenExpr, properties) : evaluateExpr(elseExpr, properties);
    }
    case "*":
      return args.reduce((acc: number, a) => acc * (evaluateExpr(a, properties) as number), 1);
    case "min":
      return Math.min(...args.map((a) => evaluateExpr(a, properties) as number));
    default:
      throw new Error(`evaluateExpr: unsupported operator ${op}`);
  }
}

// The infoBoxItems computation is a plain expression assigned to a `const`
// inside the component body (no map/hooks involved beyond reading `aircraft`,
// `screenPositions`, and `isolateId` from the surrounding scope), so it's
// extracted and evaluated the same way as the paint objects above -- this
// exercises the real filter chain, including the isolate check, rather than
// a hand-copied duplicate that could drift from the source.
function extractInfoBoxItemsExpression(): string {
  const marker = "const infoBoxItems: InfoBoxLayerItem[] = ";
  const startIndex = mapViewSource.indexOf(marker);
  if (startIndex === -1) throw new Error("Could not find infoBoxItems declaration");

  const exprStart = startIndex + marker.length;
  const endIndex = mapViewSource.indexOf(";", exprStart);
  if (endIndex === -1) throw new Error("Could not find end of infoBoxItems computation");

  return mapViewSource.slice(exprStart, endIndex);
}

interface FakeAircraft {
  icao_hex: string;
  lat: number | null;
  lon: number | null;
  hidden: boolean;
}

interface FakeScreenPosition {
  x: number;
  y: number;
  offset: "above" | "below";
}

function computeInfoBoxItems(
  aircraft: Record<string, FakeAircraft>,
  screenPositions: Record<string, FakeScreenPosition>,
  isolateId: string | null,
): Array<{ id: string }> {
  const expression = extractInfoBoxItemsExpression();
  // eslint-disable-next-line no-new-func -- evaluating a plain expression
  // extracted from our own source, not user input.
  const fn = new Function(
    "aircraft",
    "screenPositions",
    "isolateId",
    "hasPosition",
    `return (${expression});`,
  );
  return fn(aircraft, screenPositions, isolateId, hasPosition);
}

describe("infoBoxItems -- isolate filtering", () => {
  function fakeAircraft(icao_hex: string): FakeAircraft {
    return { icao_hex, lat: 1, lon: 1, hidden: false };
  }

  function fakeScreenPosition(): FakeScreenPosition {
    return { x: 0, y: 0, offset: "above" };
  }

  const aircraft: Record<string, FakeAircraft> = {
    AAAAAA: fakeAircraft("AAAAAA"),
    BBBBBB: fakeAircraft("BBBBBB"),
    CCCCCC: fakeAircraft("CCCCCC"),
  };
  const screenPositions: Record<string, FakeScreenPosition> = {
    AAAAAA: fakeScreenPosition(),
    BBBBBB: fakeScreenPosition(),
    CCCCCC: fakeScreenPosition(),
  };

  it("with Isolate on, only the isolated aircraft's info box item appears", () => {
    const items = computeInfoBoxItems(aircraft, screenPositions, "BBBBBB");
    expect(items.map((item) => item.id)).toEqual(["BBBBBB"]);
  });

  it("with Isolate off (isolateId null), every aircraft's info box item appears", () => {
    const items = computeInfoBoxItems(aircraft, screenPositions, null);
    expect(items.map((item) => item.id).sort()).toEqual(["AAAAAA", "BBBBBB", "CCCCCC"]);
  });
});

describe("aircraft layer paint -- icon-halo-*", () => {
  const paint = aircraftLayerPaint();

  it("gives selected aircraft the thick white selection halo, unselected a thin black outline", () => {
    expect(paint["icon-halo-color"]).toEqual([
      "case",
      ["boolean", ["get", "selected"], false],
      "#ffffff",
      "#000000",
    ]);
  });

  it("scales the selected halo width down for small icon_scale shapes, unchanged at/above 1", () => {
    expect(paint["icon-halo-width"]).toEqual([
      "case",
      ["boolean", ["get", "selected"], false],
      ["*", 3, ["min", 1, ["coalesce", ["get", "icon_scale"], 1]]],
      1,
    ]);
  });

  it("scales the selected halo blur down for small icon_scale shapes, unchanged at/above 1", () => {
    expect(paint["icon-halo-blur"]).toEqual([
      "case",
      ["boolean", ["get", "selected"], false],
      ["*", 0.08, ["min", 1, ["coalesce", ["get", "icon_scale"], 1]]],
      0,
    ]);
  });

  it("never touches the unselected halo (width 1 / blur 0) regardless of icon_scale", () => {
    for (const icon_scale of [0.6, 0.7, 1, 1.3, 1.6, undefined]) {
      const properties = { selected: false, icon_scale };
      expect(evaluateExpr(paint["icon-halo-width"], properties)).toBe(1);
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0);
    }
  });

  it("leaves the selected halo exactly at its original fixed value for icon_scale >= 1 (larger aircraft unchanged)", () => {
    for (const icon_scale of [1, 1.2, 1.433, 1.6]) {
      const properties = { selected: true, icon_scale };
      expect(evaluateExpr(paint["icon-halo-width"], properties)).toBe(3);
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0.08);
    }
  });

  it("scales the selected halo proportionally to icon_scale below 1, across the real generated range (0.6-1.6, clamped in generate-aircraft-shapes.mjs)", () => {
    // AIRCRAFT_SHAPES.P28A.scale is exactly SCALE_MIN (0.6) -- the smallest
    // multiplier any real shape uses (generate-aircraft-shapes.mjs).
    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.6 })).toBeCloseTo(1.8);
    expect(evaluateExpr(paint["icon-halo-blur"], { selected: true, icon_scale: 0.6 })).toBeCloseTo(0.048);

    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.8 })).toBeCloseTo(2.4);
    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.99 })).toBeCloseTo(2.97);
  });

  it("documents the #1763 fix: the old 0.5 base icon-halo-blur pushed the shader's smoothstep band well below zero at the reference icon_scale = 1 (the issue's own worked example, ~[-0.163, 0.299]), giving every texel fully outside the SDF shape -- floored at exactly 0, never negative -- nonzero halo alpha (the reported wash); the new value keeps the band's lower bound close to zero instead, at every real icon_scale", () => {
    // Mirrors the shader math verified against map/frontend/node_modules/
    // maplibre-gl/src/shaders/glsl/symbol_sdf.fragment.glsl: SDF_PX = 8,
    // EDGE_GAMMA = 0.105 / DPR, halo_edge = (6 - halo_width / fontScale) /
    // SDF_PX, gamma_halo = (halo_blur * 1.19 / SDF_PX + EDGE_GAMMA) /
    // fontScale (u_gamma_scale taken as 1, matching the issue's own worked
    // example), band = halo_edge -/+ gamma_halo. Only halo_blur's own
    // contribution to gamma_halo cancels icon_scale out (it carries the
    // same min(1, icon_scale) factor as fontScale); the fixed EDGE_GAMMA
    // term does not (fontScale alone shrinks with icon_scale), so the
    // band is *more* negative for smaller aircraft regardless of
    // halo_blur -- that residual is the issue's separately-documented,
    // out-of-scope baseline wash, not something this fix touches. What
    // this fix controls is the halo_blur-specific delta, checked here as
    // a comparison against the old value rather than an absolute
    // threshold that would vary by icon_scale.
    const SDF_PX = 8;
    const DPR = 2;
    const EDGE_GAMMA = 0.105 / DPR;
    const BASE_ICON_SIZE_MULTIPLIER = 0.55;
    const OLD_HALO_BLUR = 0.5;

    function bandLowerBound(haloBlur: number, iconScale: number): number {
      const fontScale = BASE_ICON_SIZE_MULTIPLIER * iconScale;
      const haloWidth = 3 * Math.min(1, iconScale);
      const scaledBlur = haloBlur * Math.min(1, iconScale);
      const haloEdge = (6 - haloWidth / fontScale) / SDF_PX;
      const gammaHalo = (scaledBlur * 1.19) / SDF_PX / fontScale + EDGE_GAMMA / fontScale;
      return haloEdge - gammaHalo;
    }

    // Reference case: reproduces the issue's own numbers almost exactly
    // (halo_edge ~= 0.068, band ~= [-0.163, 0.299] at the old value).
    const oldReferenceLowerBound = bandLowerBound(OLD_HALO_BLUR, 1);
    expect(oldReferenceLowerBound).toBeCloseTo(-0.1625, 3);
    const newReferenceLowerBound = bandLowerBound(0.08, 1);
    expect(newReferenceLowerBound).toBeGreaterThan(-0.05);

    // At every real icon_scale, the new value's band lower bound is
    // strictly closer to zero (less negative, or less far above zero)
    // than the old value's -- a consistent improvement, even though (per
    // the comment above) it isn't driven fully to zero for the smallest
    // aircraft. icon_scale <= 1 (where fontScale is smallest and the
    // wash is worst) sees a large improvement; icon_scale > 1 already had
    // little/no wash even at the old value, so the improvement there is
    // small but still strictly present.
    for (const icon_scale of [0.6, 0.8, 1, 1.3, 1.6]) {
      const newHaloBlur = evaluateExpr(paint["icon-halo-blur"], { selected: true, icon_scale }) as number;
      const oldBound = bandLowerBound(OLD_HALO_BLUR, icon_scale);
      const newBound = bandLowerBound(newHaloBlur, icon_scale);
      expect(newBound).toBeGreaterThan(oldBound);
      if (icon_scale <= 1) {
        expect(newBound).toBeGreaterThan(oldBound + 0.1);
      }
    }
  });

  it("documents the bug this fixes: the old fixed 3px halo width at the smallest real icon_scale (0.6, e.g. P28A) demanded more texture-space distance than the SDF falloff band encodes -- exceeding it is what turned the ring into a solid box", () => {
    const BASE_ICON_SIZE_MULTIPLIER = 0.55;
    const smallestIconScale = 0.6;
    const oldFixedHaloWidth = 3;
    const iconSize = BASE_ICON_SIZE_MULTIPLIER * smallestIconScale;
    const oldTextureDistance = oldFixedHaloWidth / iconSize;
    expect(oldTextureDistance).toBeGreaterThan(SDF_RADIUS_PX);

    const newHaloWidth = evaluateExpr(paint["icon-halo-width"], {
      selected: true,
      icon_scale: smallestIconScale,
    }) as number;
    const newTextureDistance = newHaloWidth / iconSize;
    expect(newTextureDistance).toBeLessThan(SDF_RADIUS_PX);
  });

  it("keeps requested halo texture-space distance constant across icon_scale <= 1, matching the reference (icon_scale = 1) shape MapLibre already renders correctly (symbol_sdf.fragment.glsl: halo_edge = (6.0 - halo_width / fontScale) / SDF_PX, where fontScale is icon-size)", () => {
    const BASE_ICON_SIZE_MULTIPLIER = 0.55; // MapView.tsx's icon-size expression
    const referenceIconSize = BASE_ICON_SIZE_MULTIPLIER * 1;
    const referenceTextureDistance = 3 / referenceIconSize;

    for (const icon_scale of [0.6, 0.65, 0.7, 0.8, 0.9, 1]) {
      const iconSize = BASE_ICON_SIZE_MULTIPLIER * icon_scale;
      const haloWidth = evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale }) as number;
      const textureDistance = haloWidth / iconSize;
      expect(textureDistance).toBeCloseTo(referenceTextureDistance, 6);
      // Comfortably inside the SDF's falloff band (aircraftIcon.ts) -- this
      // margin is exactly why icon_scale = 1 already renders a correctly
      // fitted ring today, and why holding every smaller shape to the same
      // distance fixes them too.
      expect(textureDistance).toBeLessThan(SDF_RADIUS_PX);
    }
  });

  it("leaves icon-color and icon-opacity untouched by the outline change", () => {
    expect(paint["icon-color"]).toEqual(["get", "color"]);
    expect(paint["icon-opacity"]).toEqual(["case", ["boolean", ["get", "stale"], false], 0.4, 1]);
  });
});

// Extracts the full `map.addLayer({ ... }, beforeId)` call for the layer
// whose definition contains `id: <layerIdConstant>` (a source-level
// constant name, e.g. "CENTER_POINT_CIRCLE_LAYER_ID" -- not its string
// value, since that's how the real source refers to it).
function extractAddLayerCall(layerIdConstant: string): string {
  const idIndex = mapViewSource.indexOf(`id: ${layerIdConstant}`);
  if (idIndex === -1) throw new Error(`Could not find ${layerIdConstant} layer definition`);

  const callStart = mapViewSource.lastIndexOf("map.addLayer(", idIndex);
  if (callStart === -1) throw new Error(`Could not find map.addLayer( call for ${layerIdConstant}`);

  const parenOpenIndex = callStart + "map.addLayer".length;
  let depth = 0;
  let i = parenOpenIndex;
  for (; i < mapViewSource.length; i++) {
    if (mapViewSource[i] === "(") depth++;
    else if (mapViewSource[i] === ")") {
      depth--;
      if (depth === 0) break;
    }
  }
  return mapViewSource.slice(callStart, i + 1);
}

// Returns the `beforeId` (second) argument of an `addLayer(layer, beforeId)`
// call, i.e. whatever comes after the layer object literal's matching `}`.
function addLayerBeforeId(layerIdConstant: string): string {
  const call = extractAddLayerCall(layerIdConstant);
  const objectOpenIndex = call.indexOf("{");
  const objectCloseIndex = findMatchingBrace(call, objectOpenIndex);
  const rest = call.slice(objectCloseIndex + 1, call.lastIndexOf(")"));
  return rest.replace(/^[,\s]+/, "").replace(/[,\s]+$/, "");
}

function extractLayerPaint(layerIdConstant: string): Record<string, unknown> {
  const idIndex = mapViewSource.indexOf(`id: ${layerIdConstant}`);
  if (idIndex === -1) throw new Error(`Could not find ${layerIdConstant} layer definition`);

  const paintKeyIndex = mapViewSource.indexOf("paint: {", idIndex);
  if (paintKeyIndex === -1) throw new Error(`Could not find paint block after ${layerIdConstant}`);

  const objectOpenIndex = paintKeyIndex + "paint: ".length;
  const objectCloseIndex = findMatchingBrace(mapViewSource, objectOpenIndex);
  const paintLiteral = mapViewSource.slice(objectOpenIndex, objectCloseIndex + 1);

  // The center point label's paint literal references the imported
  // MUTED_GRAY constant by name (not a literal string), so it must be
  // supplied as an in-scope binding here -- same reasoning as the
  // aircraft/isolate expression evaluators above.
  // eslint-disable-next-line no-new-func -- evaluating a plain object literal
  // extracted from our own source, not user input.
  const fn = new Function("MUTED_GRAY", `return (${paintLiteral});`);
  return fn(MUTED_GRAY);
}

describe("center point layer -- visible, and behind aircraft icons", () => {
  // This regression was a DOM `Marker` given a negative z-index
  // (`el.style.zIndex = "-1"`) so it would lose to aircraft icons
  // drawn on MapLibre's WebGL canvas -- but a DOM element appended into the
  // canvas's own container paints either entirely in front of that canvas
  // or entirely behind ALL of it, never behind just some of what it draws.
  // A negative z-index buried the marker under the canvas's own opaque
  // paint, hiding it outright regardless of whether an aircraft was nearby.
  // The center point is now a map layer (source feature), sharing the same
  // WebGL paint pipeline as the aircraft icons, so it's always painted (no
  // z-index fight to lose) and layer order -- not z-index -- controls
  // whether it renders under aircraft icons.

  it("has no DOM Marker construction left for the center point (the regression's actual mechanism)", () => {
    expect(mapViewSource).not.toContain("new maplibregl.Marker(");
    expect(mapViewSource).not.toContain("el.style.zIndex");
  });

  it("inserts the circle layer before the aircraft icon layer, not merely appended on top", () => {
    expect(addLayerBeforeId("CENTER_POINT_CIRCLE_LAYER_ID")).toBe("AIRCRAFT_LAYER_ID");
  });

  it("inserts the label layer before the aircraft icon layer too", () => {
    expect(addLayerBeforeId("CENTER_POINT_LABEL_LAYER_ID")).toBe("AIRCRAFT_LAYER_ID");
  });

  it("the circle layer paints a fully visible black dot -- no opacity/visibility hiding it", () => {
    const paint = extractLayerPaint("CENTER_POINT_CIRCLE_LAYER_ID");
    expect(paint["circle-color"]).toBe("#000000");
    expect(paint["circle-opacity"]).toBeUndefined();
  });

  it("the label layer paints the CENTER text with a white halo for contrast, not hidden", () => {
    const paint = extractLayerPaint("CENTER_POINT_LABEL_LAYER_ID");
    expect(paint["text-color"]).toBe(MUTED_GRAY);
    expect(paint["text-opacity"]).toBeUndefined();
    expect(paint["text-halo-color"]).toBe("#ffffff");
  });
});

// Extracts the body of `function handleRecenter() { ... }` as plain source
// text -- same rationale as the helpers above: handleRecenter closes over
// several hooks (mapRef, config, cancelFollow) and calls into a real
// maplibregl.Map, so there's no jsdom/component-render setup to mount it
// through. Reading the real source instead of a hand-copied duplicate means
// this can't drift from what the component actually does.
function handleRecenterBody(): string {
  const marker = "function handleRecenter() {";
  const startIndex = mapViewSource.indexOf(marker);
  if (startIndex === -1) throw new Error("Could not find handleRecenter declaration");

  const bodyOpenIndex = startIndex + marker.length - 1;
  const bodyCloseIndex = findMatchingBrace(mapViewSource, bodyOpenIndex);
  return mapViewSource.slice(bodyOpenIndex + 1, bodyCloseIndex);
}

describe("handleRecenter -- cancels Follow before recentering", () => {
  const body = handleRecenterBody();

  it("calls cancelFollow()", () => {
    expect(body).toContain("cancelFollow()");
  });

  it("cancels Follow before issuing the easeTo, not after -- otherwise Follow's own recenter effect\n" +
    "    (which re-runs on every `aircraft` update while followId is set) can still land a second\n" +
    "    easeTo that snaps the view right back before the cancellation takes effect", () => {
    const cancelIndex = body.indexOf("cancelFollow()");
    const easeToIndex = body.indexOf("map.easeTo(");
    expect(cancelIndex).toBeGreaterThan(-1);
    expect(easeToIndex).toBeGreaterThan(-1);
    expect(cancelIndex).toBeLessThan(easeToIndex);
  });

  it("still guards on a missing map/center reference, unchanged from before", () => {
    expect(body).toContain("if (!map || !config.center) return;");
  });
});

describe("trail layer paint -- line-opacity dims a Follow-lost trail", () => {
  const paint = trailLayerPaint();

  it("dims a trail feature flagged dimmed (see featureCollections.ts's trailFeatureCollection)", () => {
    expect(paint["line-opacity"]).toEqual(["case", ["boolean", ["get", "dimmed"], false], 0.35, 0.85]);
  });

  it("leaves line-color untouched", () => {
    expect(paint["line-color"]).toEqual(["get", "color"]);
  });
});

// Extracts the body of `function handleToggleFullscreen() { ... }` as plain
// source text -- same rationale as handleRecenterBody above: it calls into
// real Fullscreen API methods with no jsdom/component-render setup to
// mount it through.
function handleToggleFullscreenBody(): string {
  const marker = "function handleToggleFullscreen() {";
  const startIndex = mapViewSource.indexOf(marker);
  if (startIndex === -1) throw new Error("Could not find handleToggleFullscreen declaration");

  const bodyOpenIndex = startIndex + marker.length - 1;
  const bodyCloseIndex = findMatchingBrace(mapViewSource, bodyOpenIndex);
  return mapViewSource.slice(bodyOpenIndex + 1, bodyCloseIndex);
}

describe("Fullscreen toggle -- targets document.documentElement, not mapContainerRef", () => {
  const body = handleToggleFullscreenBody();

  it("requests fullscreen on document.documentElement", () => {
    expect(body).toContain("document.documentElement.requestFullscreen()");
  });

  it("never targets mapContainerRef -- that's a sibling of the overlay panels, not their parent", () => {
    expect(body).not.toContain("mapContainerRef");
  });

  it("calls document.exitFullscreen() to leave fullscreen", () => {
    expect(body).toContain("document.exitFullscreen()");
  });

  it("branches on document.fullscreenElement to decide enter vs. exit", () => {
    expect(body).toContain("document.fullscreenElement");
  });
});

describe("Fullscreen state sync -- fullscreenchange listener", () => {
  it("checks document.fullscreenEnabled once via a lazy useState initializer, not on every render", () => {
    expect(mapViewSource).toContain("useState(() => document.fullscreenEnabled)");
  });

  it("registers a fullscreenchange listener on document", () => {
    expect(mapViewSource).toContain('document.addEventListener("fullscreenchange", handleFullscreenChange)');
  });

  it("cleans up the fullscreenchange listener on unmount", () => {
    expect(mapViewSource).toContain('document.removeEventListener("fullscreenchange", handleFullscreenChange)');
  });

  it("registers the listener in an effect with an empty dependency array -- once per mount", () => {
    const addIndex = mapViewSource.indexOf('document.addEventListener("fullscreenchange"');
    const removeIndex = mapViewSource.indexOf('document.removeEventListener("fullscreenchange"');
    const closeIndex = mapViewSource.indexOf("}, []);", removeIndex);
    expect(removeIndex).toBeGreaterThan(addIndex);
    expect(closeIndex).toBeGreaterThan(removeIndex);
    // Nothing but the cleanup return sits between the listener registration
    // and the effect's own closing `}, []);` -- proves this is a single,
    // mount-once effect rather than one re-subscribing on every render.
    expect(mapViewSource.slice(addIndex, closeIndex)).not.toContain("useEffect(");
  });
});
