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
    case "all":
      return args.every((a) => evaluateExpr(a, properties));
    case ">=":
      return (evaluateExpr(args[0], properties) as number) >= (evaluateExpr(args[1], properties) as number);
    case "<":
      return (evaluateExpr(args[0], properties) as number) < (evaluateExpr(args[1], properties) as number);
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

  it("gives selected aircraft the thick white selection halo, unselected aircraft no halo (#1787)", () => {
    expect(paint["icon-halo-color"]).toEqual([
      "case",
      ["boolean", ["get", "selected"], false],
      "#ffffff",
      "#000000",
    ]);
  });

  it("#1806: gives the icon's own halo only to selected icon_scale >= 1 aircraft, at the original fixed values -- unchanged shape, only the gating condition changed", () => {
    const selectedAtLeastOne = [
      "all",
      ["boolean", ["get", "selected"], false],
      [">=", ["coalesce", ["get", "icon_scale"], 1], 1],
    ];
    expect(paint["icon-halo-width"]).toEqual(["case", selectedAtLeastOne, 3, 0]);
    expect(paint["icon-halo-blur"]).toEqual(["case", selectedAtLeastOne, 0.08, 0]);
  });

  it("never gives the unselected halo any width or blur (#1787), regardless of icon_scale", () => {
    for (const icon_scale of [0.6, 0.7, 1, 1.3, 1.6, undefined]) {
      const properties = { selected: false, icon_scale };
      expect(evaluateExpr(paint["icon-halo-width"], properties)).toBe(0);
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0);
    }
  });

  it("leaves the selected halo exactly at its original fixed value for icon_scale >= 1 (larger aircraft unchanged, e.g. B77L at 1.435 -- confirmed clean in #1806)", () => {
    for (const icon_scale of [1, 1.2, 1.433, 1.435, 1.6]) {
      const properties = { selected: true, icon_scale };
      expect(evaluateExpr(paint["icon-halo-width"], properties)).toBe(3);
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0.08);
    }
  });

  it("#1806: gives selected icon_scale < 1 aircraft NO icon halo at all (width and blur both 0) -- including the issue's own reported cases, E55P/C25B at 0.722 and GALX/GLF6 at 0.989 -- rather than a scaled-down one; selection is shown via AIRCRAFT_SELECTION_RING_LAYER_ID instead (see the describe block below)", () => {
    for (const icon_scale of [0.6, 0.722, 0.8, 0.989, 0.999]) {
      const properties = { selected: true, icon_scale };
      expect(evaluateExpr(paint["icon-halo-width"], properties)).toBe(0);
      expect(evaluateExpr(paint["icon-halo-blur"], properties)).toBe(0);
    }
  });

  it("documents why #1806 stops scaling icon-halo-width/-blur down for icon_scale < 1 instead of tightening the constants further (three prior rounds -- #1705, #1742/#1758, #1763/#1767 -- all tried that): every real icon_scale < 1 is mathematically guaranteed a negative smoothstep-band lower bound (i.e. some wash), and it cannot be pushed back to >= 0 by any icon-halo-width/-blur value, because both only ever add to the band width -- neither can subtract enough to cancel EDGE_GAMMA/fontScale, the one term in the shader's gamma_halo that has no icon_scale factor to cancel against fontScale's", () => {
    // Mirrors the shader math verified against map/frontend/node_modules/
    // maplibre-gl/src/shaders/glsl/symbol_sdf.fragment.glsl: SDF_PX = 8,
    // EDGE_GAMMA = 0.105 / DPR, halo_edge = (6 - halo_width / fontScale) /
    // SDF_PX, gamma_halo = (halo_blur * 1.19 / SDF_PX + EDGE_GAMMA) /
    // fontScale (u_gamma_scale taken as 1, matching prior worked examples
    // in this file). A background texel gets nonzero halo alpha the moment
    // `halo_edge - gamma_halo < 0` -- that's the box overflow.
    const SDF_PX = 8;
    const DPR = 2;
    const EDGE_GAMMA = 0.105 / DPR;
    const BASE_ICON_SIZE_MULTIPLIER = 0.55;

    // The *best possible* band lower bound achievable purely by tuning
    // icon-halo-width/-blur at a given icon_scale, using the #1763/#1767
    // scaling law (width/blur both carry the same min(1, icon_scale)
    // factor as fontScale, so their own contributions are already
    // scale-invariant/minimal -- see the two tests above). Even with
    // halo_blur pushed all the way to 0 (the smallest it can go), the
    // fixed EDGE_GAMMA term alone still determines the bound:
    function bestAchievableLowerBound(iconScale: number): number {
      const fontScale = BASE_ICON_SIZE_MULTIPLIER * iconScale;
      const haloWidth = 3 * Math.min(1, iconScale); // #1763/#1767's own-cancelling width law
      const haloEdge = (6 - haloWidth / fontScale) / SDF_PX;
      const gammaHalo = EDGE_GAMMA / fontScale; // halo_blur = 0: only the irreducible term remains
      return haloEdge - gammaHalo;
    }

    // At the icon_scale = 1 reference point -- the value every prior round
    // pinned everything else to, and which ships today without further
    // complaint -- the bound is already negative...
    expect(bestAchievableLowerBound(1)).toBeLessThan(0);

    // ...and it's strictly *increasing* in icon_scale (more headroom at
    // larger icon_scale), so every icon_scale below 1 -- the issue's own
    // 0.989 (GALX/GLF6) and 0.722 (E55P/C25B) included, down to the
    // smallest real shape at 0.6 (P28A) -- is strictly worse than that
    // already-negative reference, no matter how icon-halo-width/-blur are
    // retuned. This is the real mathematical floor #1806 asks about: it's
    // not a matter of finding better constants.
    const referenceBound = bestAchievableLowerBound(1);
    for (const icon_scale of [0.6, 0.722, 0.8, 0.989, 0.999]) {
      expect(bestAchievableLowerBound(icon_scale)).toBeLessThan(referenceBound);
    }
    // Above 1, no such floor applies -- the bound keeps rising and crosses
    // zero (a mathematically clean ring, not merely "less negative") well
    // before B77L's real 1.435, consistent with it being reported clean.
    expect(bestAchievableLowerBound(1.435)).toBeGreaterThan(0);
    for (const icon_scale of [1.065, 1.435, 1.6]) {
      expect(bestAchievableLowerBound(icon_scale)).toBeGreaterThan(referenceBound);
    }
  });

  it("documents the bug this fixes: the old fixed 3px halo width at the smallest real icon_scale (0.6, e.g. P28A) demanded more texture-space distance than the SDF falloff band encodes -- exceeding it is what turned the ring into a solid box; #1806 now avoids the icon's own halo there entirely rather than trying to fit a smaller one into it", () => {
    const BASE_ICON_SIZE_MULTIPLIER = 0.55;
    const smallestIconScale = 0.6;
    const oldFixedHaloWidth = 3;
    const iconSize = BASE_ICON_SIZE_MULTIPLIER * smallestIconScale;
    const oldTextureDistance = oldFixedHaloWidth / iconSize;
    expect(oldTextureDistance).toBeGreaterThan(SDF_RADIUS_PX);

    // #1806: icon-halo-width is now exactly 0 at this icon_scale (no
    // halo drawn at all -- see AIRCRAFT_SELECTION_RING_LAYER_ID for the
    // replacement selection indicator), not a smaller-but-still-nonzero
    // value, so there's no texture-space distance to overflow the falloff
    // band with in the first place.
    const newHaloWidth = evaluateExpr(paint["icon-halo-width"], {
      selected: true,
      icon_scale: smallestIconScale,
    }) as number;
    expect(newHaloWidth).toBe(0);
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

  // eslint-disable-next-line no-new-func -- evaluating a plain object literal
  // extracted from our own source, not user input.
  const fn = new Function(`return (${paintLiteral});`);
  return fn();
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

  it("the circle layer paints a fully visible black dot -- no opacity/visibility hiding it", () => {
    const paint = extractLayerPaint("CENTER_POINT_CIRCLE_LAYER_ID");
    expect(paint["circle-color"]).toBe("#000000");
    expect(paint["circle-opacity"]).toBeUndefined();
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

describe("screen-position sync on 'move' -- throttled (#1776)", () => {
  it("registers a throttled wrapper on 'move', not syncScreenPositions directly", () => {
    expect(mapViewSource).toContain('map.on("move", throttledSyncScreenPositions);');
    expect(mapViewSource).not.toContain('map.on("move", syncScreenPositions);');
  });

  it("unregisters the same throttled wrapper on cleanup", () => {
    expect(mapViewSource).toContain('map.off("move", throttledSyncScreenPositions);');
  });

  it("routes the wrapper through a dedicated throttle instance, not the data-source sync's own", () => {
    expect(mapViewSource).toContain("screenPositionThrottleRef.current.request(syncScreenPositions)");
    expect(mapViewSource).toContain("createTrailingThrottle(SCREEN_POSITION_THROTTLE_MS)");
  });

  it("cancels the screen-position throttle's pending trailing run on unmount", () => {
    const refIndex = mapViewSource.indexOf("screenPositionThrottleRef = useRef(");
    const cancelIndex = mapViewSource.indexOf("screenPositionThrottleRef.current.cancel()");
    expect(refIndex).toBeGreaterThan(-1);
    expect(cancelIndex).toBeGreaterThan(refIndex);
  });

  it("still calls syncScreenPositions directly (unthrottled) once right after registering the listener", () => {
    // The initial paint shouldn't wait on the throttle's own window.
    const registerIndex = mapViewSource.indexOf('map.on("move", throttledSyncScreenPositions);');
    const callSite = mapViewSource.slice(registerIndex, registerIndex + 200);
    expect(callSite).toContain("syncScreenPositions();");
  });
});

describe("aircraft/trail source sync -- incremental updateData() diff path (#1775)", () => {
  it("imports the diff-tracking and diff-building helpers", () => {
    expect(mapViewSource).toContain('import { diffAircraftMaps } from "../lib/aircraftMapDiff"');
    expect(mapViewSource).toContain("buildAircraftSourceDiff");
    expect(mapViewSource).toContain("buildTrailSourceDiff");
  });

  it("tracks the previous aircraft snapshot and synced trail-segment ids across ticks", () => {
    expect(mapViewSource).toContain("const prevAircraftRef = useRef<AircraftMap>({});");
    expect(mapViewSource).toContain("const syncedTrailSegmentIdsRef = useRef<Map<string, string[]>>(new Map());");
  });

  it("computes visibilityChanged from the previous run's visibility-affecting inputs, not just aircraft", () => {
    expect(mapViewSource).toContain("const prevVisibilityInputsRef = useRef<{");
    expect(mapViewSource).toContain("const visibilityChanged =");
    expect(mapViewSource).toContain("prevInputs.historyAll !== historyAll");
    expect(mapViewSource).toContain("prevInputs.selected !== selected");
    expect(mapViewSource).toContain("prevInputs.isolateId !== isolateId");
    expect(mapViewSource).toContain("prevInputs.followId !== followId");
    expect(mapViewSource).toContain("prevInputs.protectedId !== selectedIcaoHex");
    expect(mapViewSource).toContain("prevInputs.tracePointsEnabled !== tracePointsEnabled");
  });

  it("uses full setData() rebuilds only on the visibilityChanged branch", () => {
    const ifIndex = mapViewSource.indexOf("if (visibilityChanged) {");
    const elseIndex = mapViewSource.indexOf("} else {", ifIndex);
    expect(ifIndex).toBeGreaterThan(-1);
    expect(elseIndex).toBeGreaterThan(ifIndex);
    const ifBranch = mapViewSource.slice(ifIndex, elseIndex);
    expect(ifBranch).toContain("aircraftSource?.setData(fc)");
    expect(ifBranch).toContain("trailSource?.setData(");
    expect(ifBranch).not.toContain("updateData");
  });

  it("uses diffAircraftMaps + updateData() on the data-only (else) branch, gated on any actual change", () => {
    const ifIndex = mapViewSource.indexOf("if (visibilityChanged) {");
    const elseIndex = mapViewSource.indexOf("} else {", ifIndex);
    const effectEnd = mapViewSource.indexOf("prevAircraftRef.current = aircraft;", elseIndex);
    expect(elseIndex).toBeGreaterThan(-1);
    expect(effectEnd).toBeGreaterThan(elseIndex);
    const elseBranch = mapViewSource.slice(elseIndex, effectEnd);
    expect(elseBranch).toContain("diffAircraftMaps(prevAircraftRef.current, aircraft)");
    expect(elseBranch).toContain("if (changed.size > 0) {");
    expect(elseBranch).toContain("aircraftSource?.updateData(aircraftDiff)");
    expect(elseBranch).toContain("trailSource?.updateData(trailResult.diff)");
    expect(elseBranch).not.toContain("setData");
  });

  it("registers SDF shape images for newly-added features on both the full and incremental paths", () => {
    // The full path already registered from `fc.features`, unchanged; the
    // incremental path must do the same from the diff's own `add` list,
    // since a changed aircraft can introduce a shape never seen before.
    expect(mapViewSource).toContain("for (const f of aircraftDiff.add ?? []) {");
    const registerCallCount = (mapViewSource.match(/registerShapeImage\(map, shape\)/g) ?? []).length;
    expect(registerCallCount).toBe(2);
  });

  it("re-syncs the trail-segment-id bookkeeping after a full rebuild, so the next incremental tick starts correctly", () => {
    expect(mapViewSource).toContain("syncedTrailSegmentIdsRef.current = bySegmentHex;");
    expect(mapViewSource).toContain("syncedTrailSegmentIdsRef.current = trailResult.syncedSegmentIds;");
  });

  it("updates prevAircraftRef exactly once per run, after both branches", () => {
    const occurrences = (mapViewSource.match(/prevAircraftRef\.current = aircraft;/g) ?? []).length;
    expect(occurrences).toBe(1);
  });
});

describe("handleSelectFromList -- AircraftListPanel row click (#1791)", () => {
  it("enables Isolate in addition to selecting, matching the Isolate button's own effect", () => {
    const startIndex = mapViewSource.indexOf("function handleSelectFromList(icaoHex: string) {");
    const endIndex = mapViewSource.indexOf("\n  }", startIndex);
    expect(startIndex).toBeGreaterThan(-1);
    const body = mapViewSource.slice(startIndex, endIndex);
    expect(body).toContain("setSelected((prev) => nextSelection(prev, icaoHex));");
    expect(body).toContain("setIsolateEnabled(true);");
  });
});

describe('map "click" handler -- background click deselects (#1792)', () => {
  it("clears selection instead of a no-op when the click misses every selectable feature", () => {
    const startIndex = mapViewSource.indexOf('map.on("click", (e) => {');
    const endIndex = mapViewSource.indexOf('map.on("mousemove"', startIndex);
    expect(startIndex).toBeGreaterThan(-1);
    expect(endIndex).toBeGreaterThan(startIndex);
    const body = mapViewSource.slice(startIndex, endIndex);
    expect(body).not.toMatch(/if \(!icaoHex\) return;/);
    expect(body).toContain("if (!icaoHex) {");
    expect(body).toContain("setSelected(new Set());");
    // Still selects normally when a feature *is* hit.
    expect(body).toContain("setSelected((prev) => nextSelection(prev, icaoHex));");
  });
});

describe("Trace Points circle layer -- inserted below the trail line (#1794)", () => {
  it("passes TRAIL_LAYER_ID as addLayer's beforeId, so the trail always paints on top of the dots", () => {
    const idIndex = mapViewSource.indexOf("id: TRACE_POINTS_CIRCLE_LAYER_ID");
    expect(idIndex).toBeGreaterThan(-1);
    // The addLayer(...) call this id belongs to must close with a second
    // argument of TRAIL_LAYER_ID, not a bare `);` -- otherwise MapLibre's
    // default (stack on top of everything so far) applies, which is
    // exactly the bug: the 8px dots would paint over the 2.5px trail line.
    const callEnd = mapViewSource.indexOf("TRAIL_LAYER_ID,\n      );", idIndex);
    expect(callEnd).toBeGreaterThan(idIndex);
    expect(callEnd - idIndex).toBeLessThan(1000); // same addLayer call, not a later unrelated one
  });
});

// Finds the index of the "]" matching the "[" at openIndex, the array
// counterpart of findMatchingBrace above.
function findMatchingBracket(text: string, openIndex: number): number {
  let depth = 0;
  for (let i = openIndex; i < text.length; i++) {
    if (text[i] === "[") depth++;
    else if (text[i] === "]") {
      depth--;
      if (depth === 0) return i;
    }
  }
  throw new Error("No matching closing bracket found");
}

// Extracts the `filter: [ ... ]` array literal belonging to the layer whose
// definition contains `id: <layerIdConstant>`, evaluated into a real
// MapLibre expression array -- same extraction convention as
// aircraftLayerPaint/extractLayerPaint above.
function extractLayerFilter(layerIdConstant: string): unknown {
  const idIndex = mapViewSource.indexOf(`id: ${layerIdConstant}`);
  if (idIndex === -1) throw new Error(`Could not find ${layerIdConstant} layer definition`);

  const filterKeyIndex = mapViewSource.indexOf("filter: [", idIndex);
  if (filterKeyIndex === -1) throw new Error(`Could not find filter after ${layerIdConstant}`);

  const arrayOpenIndex = filterKeyIndex + "filter: ".length;
  const arrayCloseIndex = findMatchingBracket(mapViewSource, arrayOpenIndex);
  const filterLiteral = mapViewSource.slice(arrayOpenIndex, arrayCloseIndex + 1);

  // eslint-disable-next-line no-new-func -- evaluating a plain array literal
  // extracted from our own source, not user input.
  return new Function(`return (${filterLiteral});`)();
}

describe("AIRCRAFT_SELECTION_RING_LAYER_ID -- fixed-size selection ring for icon_scale < 1 (#1806)", () => {
  // #1806: the icon's own icon-halo-width/-blur cannot render a clean
  // fitted ring below icon_scale = 1 at any value (see the "documents why
  // #1806 stops scaling..." test above) -- this separate, non-SDF circle
  // layer is the replacement selection indicator for that range. It has no
  // texture-space math to overflow: circle-radius/circle-stroke-width are
  // screen pixels throughout, so it renders identically regardless of
  // icon_scale.

  it("filters to selected AND icon_scale < 1 -- exactly the range the icon's own halo now skips", () => {
    const filter = extractLayerFilter("AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(filter).toEqual([
      "all",
      ["boolean", ["get", "selected"], false],
      ["<", ["coalesce", ["get", "icon_scale"], 1], 1],
    ]);
  });

  it("shares AIRCRAFT_SOURCE_ID rather than a separate source -- no extra data-sync wiring needed", () => {
    const idIndex = mapViewSource.indexOf("id: AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(idIndex).toBeGreaterThan(-1);
    const sourceIndex = mapViewSource.indexOf("source: AIRCRAFT_SOURCE_ID", idIndex);
    expect(sourceIndex).toBeGreaterThan(idIndex);
    expect(sourceIndex - idIndex).toBeLessThan(200);
  });

  it("draws a transparent-fill, white-stroke ring, dimmed the same way a stale icon is (matching icon-opacity's convention)", () => {
    const paint = extractLayerPaint("AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(paint["circle-color"]).toBe("rgba(0, 0, 0, 0)");
    expect(paint["circle-stroke-color"]).toBe("#ffffff");
    expect(paint["circle-stroke-width"]).toBeGreaterThan(0);
    const staleDimming = ["case", ["boolean", ["get", "stale"], false], 0.4, 1];
    expect(paint["circle-opacity"]).toEqual(staleDimming);
    expect(paint["circle-stroke-opacity"]).toEqual(staleDimming);
  });

  it("worked examples from the issue: E55P/C25B (icon_scale 0.722) and GALX/GLF6 (icon_scale 0.989) both match this layer's filter when selected, while B77L (1.435) and the icon_scale = 1 reference case do not (they stay on the icon's own halo instead)", () => {
    const filter = extractLayerFilter("AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(evaluateExpr(filter, { selected: true, icon_scale: 0.722 })).toBe(true);
    expect(evaluateExpr(filter, { selected: true, icon_scale: 0.989 })).toBe(true);
    expect(evaluateExpr(filter, { selected: true, icon_scale: 1.435 })).toBe(false);
    expect(evaluateExpr(filter, { selected: true, icon_scale: 1 })).toBe(false);
    // Never shown for an unselected aircraft, regardless of icon_scale.
    expect(evaluateExpr(filter, { selected: false, icon_scale: 0.722 })).toBe(false);
  });
});
