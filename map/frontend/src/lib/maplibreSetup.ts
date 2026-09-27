import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// maplibre-gl ships its worker as a separate file (maplibre-gl-worker.mjs)
// that Vite/Rollup can't discover and bundle on its own -- it's only ever
// loaded at runtime via a URL. vite.config.ts's maplibreWorkerAssets
// plugin copies it into the build's own assets/ directory.
//
// This app is served under /map (map/main.py's StaticFiles mount, Vite's
// base: '/map/'), not from root, so a root-relative "/assets/..." path
// would 404 -- import.meta.env.BASE_URL keeps this correct under whatever
// sub-path the app is mounted at. Without this, a map silently never fires
// its `load` event and everything gated on that hangs forever.
maplibregl.setWorkerUrl(`${import.meta.env.BASE_URL}assets/maplibre-gl-worker.mjs`);

// "positron" (CARTO's light/grayscale basemap design, served here by
// OpenFreeMap). Verify this URL is still live if a map ever shows a
// blank/broken basemap.
export const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";
