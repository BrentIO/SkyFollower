import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// maplibre-gl ships its worker as a separate file (maplibre-gl-worker.mjs)
// with a hardcoded relative import of a second file
// (maplibre-gl-shared.mjs) -- neither is something Vite/Rollup can
// discover and bundle on its own (the worker is only ever loaded at
// runtime via a URL, and its own internal import is resolved by the
// browser, not by our build). vite.config.ts's maplibreWorkerAssets
// plugin copies both files, verbatim and under these exact names, to
// /assets/ in both dev and build, which is what this path points at.
// Without this, a map silently never fires its `load` event and
// everything gated on that hangs forever.
//
// Imported (for its side effect) by every view that creates a
// maplibregl.Map -- AreasView.tsx and LookupView.tsx as of this writing --
// so the workaround lives in exactly one place rather than being
// re-declared per view.
maplibregl.setWorkerUrl("/assets/maplibre-gl-worker.mjs");

// "positron" (CARTO's well-known light/grayscale basemap design, served
// here by the same OpenFreeMap provider) instead of "liberty"'s full-color
// style, per request. Verify this URL is still live if a map ever shows a
// blank/broken basemap.
export const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";
