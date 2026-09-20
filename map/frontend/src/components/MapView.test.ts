import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (declared by vite/client, referenced in src/vite-env.d.ts)
// imports a file's contents as a plain string -- used here instead of node:fs so
// this stays a normal Vite/vitest module rather than needing @types/node, which
// this project's tsconfig.app.json (unlike tsconfig.node.json) doesn't pull in.
import mapViewSource from "./MapView.tsx?raw";
import { SDF_RADIUS_PX } from "../lib/aircraftIcon";
import { AIRCRAFT_LAYER_ID, SELECTABLE_LAYER_IDS, TRAIL_HIT_AREA_LAYER_ID } from "../lib/mapLayerIds";

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

// Extracts the body of `function updateIsCentered() { ... }` -- same
// rationale as handleRecenterBody above: it closes over the real
// maplibregl.Map/config, so there's no jsdom/component-render setup to
// mount it through.
function updateIsCenteredBody(): string {
  const marker = "function updateIsCentered() {";
  const startIndex = mapViewSource.indexOf(marker);
  if (startIndex === -1) throw new Error("Could not find updateIsCentered declaration");

  const bodyOpenIndex = startIndex + marker.length - 1;
  const bodyCloseIndex = findMatchingBrace(mapViewSource, bodyOpenIndex);
  return mapViewSource.slice(bodyOpenIndex + 1, bodyCloseIndex);
}

describe("updateIsCentered / Center button active state (#1847)", () => {
  const body = updateIsCenteredBody();

  it("bails out without setting state when no center is configured", () => {
    expect(body).toContain("if (!config.center) return;");
  });

  it("projects both the current camera center and the configured center through map.project()", () => {
    expect(body).toContain("map.project(map.getCenter())");
    expect(body).toContain("map.project([config.center.longitude, config.center.latitude])");
  });

  it("compares the two projected points via the pixel-distance-tolerance helper, not a lat/lon epsilon", () => {
    expect(body).toContain("isWithinCenterTolerance(current, target)");
    expect(mapViewSource).toContain('import { isWithinCenterTolerance } from "../lib/mapCentered"');
  });

  it("never reads the map's zoom -- centered is a pure position match, independent of zoom", () => {
    expect(body).not.toMatch(/\bmap\.getZoom\(/);
    expect(body).not.toContain("zoom");
  });

  it("is registered on 'moveend' only -- updateIsCentered doesn't add a new per-frame 'move' listener (this project's real perf history, #1830/#1831/#1838)", () => {
    // #1851 restored the DOM InfoBoxLayer's own "move"-driven screen-position
    // sync (throttledSyncScreenPositions) -- a real, pre-existing "move"
    // listener unrelated to centering. This test only asserts that
    // updateIsCentered itself isn't also wired to "move", not that the file
    // has no "move" listener at all.
    expect(mapViewSource).toContain('map.on("moveend", updateIsCentered)');
    expect(mapViewSource).not.toMatch(/map\.on\(\s*"move"\s*,\s*updateIsCentered/);
  });

  it("only attaches the moveend listener when a center is actually configured", () => {
    const registrationIndex = mapViewSource.indexOf('map.on("moveend", updateIsCentered)');
    const guardIndex = mapViewSource.lastIndexOf("if (config.center) {", registrationIndex);
    expect(guardIndex).toBeGreaterThan(-1);
    // No unrelated code between the guard and the registration.
    expect(mapViewSource.slice(guardIndex, registrationIndex)).not.toContain("}");
  });

  it("is also called once inside the 'load' handler, so the button reads active immediately on first render", () => {
    const loadIndex = mapViewSource.indexOf('map.on("load", () => {');
    const setMapLoadedIndex = mapViewSource.indexOf("setMapLoaded(true);");
    const updateCallIndex = mapViewSource.indexOf("updateIsCentered();", loadIndex);
    expect(loadIndex).toBeGreaterThan(-1);
    expect(updateCallIndex).toBeGreaterThan(loadIndex);
    expect(updateCallIndex).toBeLessThan(setMapLoadedIndex);
  });

  it("passes the resulting isCentered state through to ControlsPanel as recenterActive", () => {
    expect(mapViewSource).toContain("recenterActive={isCentered}");
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

describe("aircraft/trail source sync -- incremental updateData() diff path (#1775)", () => {
  it("imports the diff-tracking and diff-building helpers", () => {
    expect(mapViewSource).toContain('import { diffAircraftMaps } from "../lib/aircraftMapDiff"');
    expect(mapViewSource).toContain("buildAircraftSourceDiff");
    expect(mapViewSource).toContain("buildTrailSourceDiff");
  });

  it("tracks the previous aircraft snapshot and synced trail-block sync state across ticks (#1838)", () => {
    expect(mapViewSource).toContain("const prevAircraftRef = useRef<AircraftMap>({});");
    expect(mapViewSource).toContain("const trailSyncRef = useRef<Map<string, TrailSyncState>>(new Map());");
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
    const elseIndex = mapViewSource.indexOf("} else if (changed.size > 0) {", ifIndex);
    expect(ifIndex).toBeGreaterThan(-1);
    expect(elseIndex).toBeGreaterThan(ifIndex);
    const ifBranch = mapViewSource.slice(ifIndex, elseIndex);
    expect(ifBranch).toContain("aircraftSource?.setData(fc)");
    expect(ifBranch).toContain("trailSource?.setData(");
    expect(ifBranch).not.toContain("updateData");
  });

  it("uses diffAircraftMaps + updateData() on the data-only (else-if) branch, gated on any actual change", () => {
    // `changed` is computed once, up front, rather than inside this branch
    // -- so this branch is `else if (changed.size > 0)`, not a nested `if`
    // inside a bare `else`. Bounded at the InfoBoxLayer screen-position
    // recompute (that block's own separate, unconditional-every-tick logic
    // starts there) rather than at prevAircraftRef's assignment further
    // down, which sits *after* it.
    const ifIndex = mapViewSource.indexOf("if (visibilityChanged) {");
    const elseIndex = mapViewSource.indexOf("} else if (changed.size > 0) {", ifIndex);
    const branchEnd = mapViewSource.indexOf("// InfoBoxLayer.tsx screen positions:", elseIndex);
    expect(elseIndex).toBeGreaterThan(-1);
    expect(branchEnd).toBeGreaterThan(elseIndex);
    const elseBranch = mapViewSource.slice(elseIndex, branchEnd);
    expect(elseBranch).toContain("aircraftSource?.updateData(aircraftDiff)");
    expect(elseBranch).toContain("trailSource?.updateData(trailResult.diff)");
    expect(elseBranch).not.toContain("setData");
    expect(mapViewSource).toContain("const changed = diffAircraftMaps(prevAircraftRef.current, aircraft);");
  });

  it("registers SDF shape images for newly-added features on both the full and incremental paths", () => {
    // The full path already registered from `fc.features`, unchanged; the
    // incremental path must do the same from the diff's own `add` list,
    // since a changed aircraft can introduce a shape never seen before.
    expect(mapViewSource).toContain("for (const f of aircraftDiff.add ?? []) {");
    const registerCallCount = (mapViewSource.match(/registerShapeImage\(map, shape\)/g) ?? []).length;
    expect(registerCallCount).toBe(2);
  });

  it("re-syncs the trail-block sync-state bookkeeping after a full rebuild, so the next incremental tick starts correctly", () => {
    expect(mapViewSource).toContain("trailSyncRef.current = nextTrailSync;");
    expect(mapViewSource).toContain("trailSyncRef.current = trailResult.syncState;");
  });

  it("updates prevAircraftRef exactly once per run, after both branches", () => {
    const occurrences = (mapViewSource.match(/prevAircraftRef\.current = aircraft;/g) ?? []).length;
    expect(occurrences).toBe(1);
  });
});

// #1851: reverts #1808's GPU/MapLibre symbol-layer info box back to the
// original DOM-based InfoBoxLayer.tsx overlay -- a live CPU trace
// comparison (see #1851's issue body) found the two roughly a wash now
// that #1838/#1840 fixed the real dominant cost (trail rendering) #1808's
// >100% CPU measurement had conflated with the info box's own cost. The
// GPU symbol layer (INFO_BOX_LAYER_ID/INFO_BOX_SOURCE_ID, lib/infoBoxIcon.ts,
// lib/infoBoxSource.ts) and the temporary #1837 DOM-vs-GPU toggle
// (lib/infoBoxImpl.ts, isDomInfoBox) are both removed entirely -- DOM is
// the only implementation, not a mode. The filter/rendering logic itself
// (selected/hovered/showAll, altitude sort key, empty-content omission) is
// covered directly in InfoBoxLayer.tsx's own dependencies (lib/infoBox.ts,
// lib/labelStackOrder.ts); these tests only check MapView.tsx's own wiring
// (which can't be exercised without a live map -- see this file's module
// docstring): screen-position sync and how InfoBoxLayer.tsx's props are
// built.
describe("InfoBoxLayer screen-position sync (#1851)", () => {
  it("imports InfoBoxLayer unconditionally and renders it, gated only on mapLoaded", () => {
    expect(mapViewSource).toContain('import { InfoBoxLayer, type InfoBoxLayerItem } from "./InfoBoxLayer"');
    const usageIndex = mapViewSource.indexOf("<InfoBoxLayer items=");
    const returnIndex = mapViewSource.lastIndexOf("  return (", usageIndex);
    expect(usageIndex).toBeGreaterThan(-1);
    expect(returnIndex).toBeGreaterThan(-1);
    const precedingLines = mapViewSource.slice(returnIndex, usageIndex);
    expect(precedingLines).toContain("{mapLoaded && (");
    // No leftover toggle -- the only gate is mapLoaded.
    expect(mapViewSource).not.toContain("isDomInfoBox");
  });

  it("registers and cleans up the 'move' screen-position listener unconditionally", () => {
    expect(mapViewSource).toContain('map.on("move", throttledSyncScreenPositions)');
    expect(mapViewSource).toContain('map.off("move", throttledSyncScreenPositions)');
  });

  it("recomputes every tracked, positioned aircraft's screen position via map.project() on both the 'move' listener and the throttled sync effect", () => {
    const occurrences = (mapViewSource.match(/const p = map\.project\(\[a\.lon, a\.lat\]\);/g) ?? []).length;
    expect(occurrences).toBe(2);
    expect(mapViewSource).toContain("setScreenPositions(positions);");
  });

  it("builds infoBoxItems from every hasPosition aircraft, keeping a Followed/selected-but-hidden aircraft (isFollowLost bypass) and respecting isolateId, same as the aircraft/trail feature builders", () => {
    const startIndex = mapViewSource.indexOf("const infoBoxItems: InfoBoxLayerItem[] = Object.values(aircraft)");
    const endIndex = mapViewSource.indexOf(".map((a) => ({", startIndex);
    expect(startIndex).toBeGreaterThan(-1);
    expect(endIndex).toBeGreaterThan(startIndex);
    const body = mapViewSource.slice(startIndex, endIndex);
    expect(body).toContain(".filter(hasPosition)");
    expect(body).toContain(".filter((a) => !a.hidden || isFollowLost(a, followId, selectedIcaoHex))");
    expect(body).toContain(".filter((a) => !isolateId || a.icao_hex === isolateId)");
    expect(body).toContain(".filter((a) => screenPositions[a.icao_hex] !== undefined)");
  });

  it("passes selected/showAll/hoveredId straight through to InfoBoxLayer, unchanged from the pre-#1808 contract", () => {
    expect(mapViewSource).toContain(
      "<InfoBoxLayer items={infoBoxItems} selected={selected} showAll={labelsAll} hoveredId={hoveredId} />",
    );
  });
});

describe("SELECTABLE_LAYER_IDS", () => {
  it("only includes the aircraft icon and trail hit-area layers -- range rings, the center point, and info-box labels (a DOM overlay, not a MapLibre layer at all) are never selection/hover targets", () => {
    expect(SELECTABLE_LAYER_IDS).toEqual([AIRCRAFT_LAYER_ID, TRAIL_HIT_AREA_LAYER_ID]);
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

// Extracts the `layout: { ... }` object literal belonging to the layer whose
// definition contains `id: <layerIdConstant>`, evaluated into a real object
// -- same extraction convention as extractLayerPaint/extractLayerFilter
// above. `scope` binds identifiers the layout literal itself references but
// that aren't defined within the extracted snippet (module-level imports or
// a called function) -- pass the *real* imported values/functions so the
// evaluated layout is the actual one MapView.tsx builds, not a stand-in.
// Layers with no such references (the common case) need no scope at all.
function extractLayerLayout(layerIdConstant: string, scope: Record<string, unknown> = {}): Record<string, unknown> {
  const idIndex = mapViewSource.indexOf(`id: ${layerIdConstant}`);
  if (idIndex === -1) throw new Error(`Could not find ${layerIdConstant} layer definition`);

  const layoutKeyIndex = mapViewSource.indexOf("layout: {", idIndex);
  if (layoutKeyIndex === -1) throw new Error(`Could not find layout block after ${layerIdConstant}`);

  const objectOpenIndex = layoutKeyIndex + "layout: ".length;
  const objectCloseIndex = findMatchingBrace(mapViewSource, objectOpenIndex);
  // Strips a TS `as ...`/`as unknown as ...` type-assertion trailing a
  // property value -- valid TS, not valid plain JS, and `new Function`
  // below only understands the latter.
  const layoutLiteral = mapViewSource
    .slice(objectOpenIndex, objectCloseIndex + 1)
    .replace(/\s+as\s+unknown\s+as\s+[A-Za-z_][\w.]*(\["[^"]+"\])?/g, "")
    .replace(/\s+as\s+[A-Za-z_][\w.]*(\["[^"]+"\])?/g, "");

  // eslint-disable-next-line no-new-func -- evaluating a plain object literal
  // (plus caller-supplied real bindings) extracted from our own source, not
  // user input.
  const fn = new Function(...Object.keys(scope), `return (${layoutLiteral});`);
  return fn(...Object.values(scope));
}

describe("AIRCRAFT_SELECTION_RING_LAYER_ID -- dilated-silhouette selection outline for icon_scale < 1 (#1806/#1816)", () => {
  // #1806: the icon's own icon-halo-width/-blur cannot render a clean
  // fitted ring below icon_scale = 1 at any value (see the "documents why
  // #1806 stops scaling..." test above). #1813's first replacement -- a
  // fixed circle-radius ring -- fixed the box/wash bug but didn't fit a
  // non-circular airframe (#1816). This layer is a second, enlarged copy
  // of the same per-shape SDF icon (AIRCRAFT_LAYER_ID's own icon-image
  // expression), painted white and underneath the real icon, so the
  // enlarged silhouette's edge reads as a fitted outline.

  it("filters to selected AND icon_scale < 1 -- exactly the range the icon's own halo skips", () => {
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

  it("is added before AIRCRAFT_LAYER_ID, not merely appended on top -- otherwise the enlarged white copy would paint over the real icon instead of peeking out from underneath it", () => {
    const ringIdIndex = mapViewSource.indexOf("id: AIRCRAFT_SELECTION_RING_LAYER_ID");
    const iconIdIndex = mapViewSource.indexOf("id: AIRCRAFT_LAYER_ID");
    expect(ringIdIndex).toBeGreaterThan(-1);
    expect(iconIdIndex).toBeGreaterThan(-1);
    expect(ringIdIndex).toBeLessThan(iconIdIndex);
  });

  it("uses the same per-shape icon-image and rotation as AIRCRAFT_LAYER_ID -- the outline must be the same silhouette, at the same heading, or it won't line up with the real icon", () => {
    const layout = extractLayerLayout("AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(layout["icon-image"]).toEqual(["concat", "sf-ac-", ["get", "shape"]]);
    expect(layout["icon-rotate"]).toEqual(["get", "heading"]);
    expect(layout["icon-rotation-alignment"]).toBe("map");
  });

  it("icon-size is enlarged relative to AIRCRAFT_LAYER_ID's own icon-size, by the same icon_scale factor, at every real icon_scale in this range", () => {
    const layout = extractLayerLayout("AIRCRAFT_SELECTION_RING_LAYER_ID");
    for (const icon_scale of [0.6, 0.722, 0.8, 0.989, 0.999]) {
      const ringSize = evaluateExpr(layout["icon-size"], { icon_scale }) as number;
      const realSize = 0.55 * icon_scale;
      expect(ringSize).toBeGreaterThan(realSize);
      // Same relative enlargement (a fixed multiplier on the real layer's
      // own icon-size formula) at every icon_scale -- not a fixed pixel
      // add-on, so it never disappears at the 0.6 floor.
      expect(ringSize / realSize).toBeCloseTo(1.15, 5);
    }
  });

  it("paints white, dimmed the same way a stale icon is (matching AIRCRAFT_LAYER_ID's icon-opacity convention)", () => {
    const paint = extractLayerPaint("AIRCRAFT_SELECTION_RING_LAYER_ID");
    expect(paint["icon-color"]).toBe("#ffffff");
    expect(paint["icon-opacity"]).toEqual(["case", ["boolean", ["get", "stale"], false], 0.4, 1]);
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

describe("map render loop (idle redraws)", () => {
  // Default 300ms symbol fade kept MapLibre's render loop permanently
  // re-armed under live traffic (~57 redraws/sec with the camera still) --
  // see the Map constructor's own comment.
  it("constructs the map with symbol fading disabled", () => {
    expect(mapViewSource).toContain("fadeDuration: 0,");
  });

  it("never sends an empty diff to the aircraft or trail source", () => {
    expect(mapViewSource).toContain("if (!isEmptySourceDiff(aircraftDiff)) aircraftSource?.updateData(aircraftDiff);");
    expect(mapViewSource).toContain("if (!isEmptySourceDiff(trailResult.diff)) trailSource?.updateData(trailResult.diff);");
  });

  it("only re-sends Trace Points when the buffer reference changed", () => {
    expect(mapViewSource).toContain("if (tracePoints !== syncedTracePointsRef.current) {");
  });

  // #1844: AIRCRAFT_SOURCE_ID's untuned default maxzoom (18) meant every
  // visible tile touched by a moved aircraft got a full worker rebuild +
  // GPU re-upload each ~500ms sync tick, one render per completed tile --
  // the source of the traced burst-then-idle frame pattern. Capping it
  // forces MapLibre to over-zoom a single cached low-zoom tile instead.
  it("caps AIRCRAFT_SOURCE_ID's maxzoom well below the app's typical display zoom, to collapse per-tick tile invalidation", () => {
    const addSourceIndex = mapViewSource.indexOf("map.addSource(AIRCRAFT_SOURCE_ID,");
    expect(addSourceIndex).toBeGreaterThan(-1);
    const callEnd = mapViewSource.indexOf(");", addSourceIndex);
    const call = mapViewSource.slice(addSourceIndex, callEnd);
    expect(call).toContain("maxzoom: 8");
  });

  it("does not cap TRAIL_SOURCE_ID's maxzoom -- LineStrings would visibly simplify at a low maxzoom (already fixed differently by #1840)", () => {
    const addSourceIndex = mapViewSource.indexOf("map.addSource(TRAIL_SOURCE_ID,");
    expect(addSourceIndex).toBeGreaterThan(-1);
    const callEnd = mapViewSource.indexOf(");", addSourceIndex);
    const call = mapViewSource.slice(addSourceIndex, callEnd);
    expect(call).not.toContain("maxzoom");
  });
});
