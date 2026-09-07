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
// Ported verbatim from
// management-ui/frontend/src/lib/maplibreSetup.ts -- this is a separate,
// standalone frontend project, so it carries its own copy of the constant
// rather than importing across the two.
maplibregl.setWorkerUrl("/assets/maplibre-gl-worker.mjs");

// "positron" (CARTO's well-known light/grayscale basemap design, served
// here by the same OpenFreeMap provider) -- same basemap as
// management-ui/frontend, for visual consistency across the two
// SkyFollower frontends. Verify this URL is still live if a map ever
// shows a blank/broken basemap.
export const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";
