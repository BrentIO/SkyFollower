import { describe, expect, it } from "vitest";
// Vite's `?raw` suffix (declared by vite/client, referenced in src/vite-env.d.ts)
// imports a file's contents as a plain string -- used here instead of node:fs so
// this stays a normal Vite/vitest module rather than needing @types/node, which
// this project's tsconfig.app.json (unlike tsconfig.node.json) doesn't pull in.
import mapViewSource from "./MapView.tsx?raw";

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

describe("aircraft layer paint -- icon-halo-*", () => {
  const paint = aircraftLayerPaint();

  it("gives selected aircraft the thick white selection halo, unselected a thin black outline", () => {
    expect(paint["icon-halo-color"]).toEqual([
      "case",
      ["boolean", ["get", "selected"], false],
      "#ffffff",
      "#000000",
    ]);
    expect(paint["icon-halo-width"]).toEqual(["case", ["boolean", ["get", "selected"], false], 3, 1]);
  });

  it("only blurs the selected halo, keeping the unselected outline crisp", () => {
    expect(paint["icon-halo-blur"]).toEqual(["case", ["boolean", ["get", "selected"], false], 0.5, 0]);
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

describe("trail layer paint -- line-opacity dims a Follow-lost trail", () => {
  const paint = trailLayerPaint();

  it("dims a trail feature flagged dimmed (see featureCollections.ts's trailFeatureCollection)", () => {
    expect(paint["line-opacity"]).toEqual(["case", ["boolean", ["get", "dimmed"], false], 0.35, 0.85]);
  });

  it("leaves line-color untouched", () => {
    expect(paint["line-color"]).toEqual(["get", "color"]);
  });
});
