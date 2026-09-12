import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (declared by vite/client, referenced in src/vite-env.d.ts)
// imports a file's contents as a plain string -- used here instead of node:fs so
// this stays a normal Vite/vitest module rather than needing @types/node, which
// this project's tsconfig.app.json (unlike tsconfig.node.json) doesn't pull in.
import mapViewSource from "./MapView.tsx?raw";
import { SDF_RADIUS_PX } from "../lib/aircraftIcon";
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
      ["*", 0.5, ["min", 1, ["coalesce", ["get", "icon_scale"], 1]]],
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
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0.5);
    }
  });

  it("scales the selected halo proportionally to icon_scale below 1, across the real generated range (0.6-1.6, clamped in generate-aircraft-shapes.mjs)", () => {
    // AIRCRAFT_SHAPES.P28A.scale is exactly SCALE_MIN (0.6) -- the smallest
    // multiplier any real shape uses (generate-aircraft-shapes.mjs).
    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.6 })).toBeCloseTo(1.8);
    expect(evaluateExpr(paint["icon-halo-blur"], { selected: true, icon_scale: 0.6 })).toBeCloseTo(0.3);

    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.8 })).toBeCloseTo(2.4);
    expect(evaluateExpr(paint["icon-halo-width"], { selected: true, icon_scale: 0.99 })).toBeCloseTo(2.97);
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

describe("home marker stacking order", () => {
  // No jsdom in this project's test setup (see the file-level comment above),
  // and DOM z-index stacking can't be asserted meaningfully without a real
  // browser render anyway -- so this checks the one thing that is testable:
  // the marker's element has its z-index set below the map canvas's default
  // stacking, and that it's set on the same `el` passed into `new
  // maplibregl.Marker(...)` (MapLibre appends that element directly, with no
  // wrapper, when a custom `element` option is given -- see marker.ts).
  const markerCallIndex = mapViewSource.indexOf("new maplibregl.Marker(");
  if (markerCallIndex === -1) throw new Error("Could not find maplibregl.Marker construction");

  const homeBlockStart = mapViewSource.lastIndexOf("const el = document.createElement", markerCallIndex);
  if (homeBlockStart === -1) throw new Error("Could not find home marker element creation");

  const homeBlock = mapViewSource.slice(homeBlockStart, markerCallIndex);

  it("sets a negative z-index on the marker's own element before constructing the Marker", () => {
    expect(homeBlock).toMatch(/el\.style\.zIndex\s*=\s*["']-1["']/);
  });

  it("passes that same element into the Marker constructor", () => {
    const markerCallEnd = mapViewSource.indexOf(")", markerCallIndex);
    const markerCall = mapViewSource.slice(markerCallIndex, markerCallEnd);
    expect(markerCall).toContain("element: el");
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

  it("still guards on a missing map/home reference, unchanged from before", () => {
    expect(body).toContain("if (!map || !config.home) return;");
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
