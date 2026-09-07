import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// maplibre-gl ships its worker as a separate file (maplibre-gl-worker.mjs)
// with a hardcoded relative import of a second file
// (maplibre-gl-shared.mjs) -- neither is something Vite/Rollup can
// discover and bundle on its own (the worker is only ever loaded at
// runtime via a URL, and its own internal import is resolved by the
// browser, not by our build). vite.config.ts's maplibreWorkerAssets
// plugin copies both files, verbatim and under these exact names, into
// the build's own assets/ directory (in both dev and build).
//
// Deliberately NOT the root-relative "/assets/..." path
// management-ui/frontend/src/lib/maplibreSetup.ts (this file's origin --
// see that file's own comment) uses: that frontend is served from root by
// nginx, but this one is served under /map (map/main.py's StaticFiles
// mount, Vite's base: '/map/' in vite.config.ts), so a root-relative path
// would 404 -- the worker file actually lives under /map/assets/. Vite
// sets import.meta.env.BASE_URL from that same `base` config at build
// time (and in dev, once vite.config.ts's `server` section is also
// base-aware -- see that file), so building the URL from it keeps this
// correct under whatever sub-path the app is actually mounted at, without
// hardcoding "/map/" a second time here.
//
// Without this, a map silently never fires its `load` event and
// everything gated on that hangs forever.
maplibregl.setWorkerUrl(`${import.meta.env.BASE_URL}assets/maplibre-gl-worker.mjs`);

// "positron" (CARTO's well-known light/grayscale basemap design, served
// here by the same OpenFreeMap provider) -- same basemap as
// management-ui/frontend, for visual consistency across the two
// SkyFollower frontends. Verify this URL is still live if a map ever
// shows a blank/broken basemap.
export const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";
