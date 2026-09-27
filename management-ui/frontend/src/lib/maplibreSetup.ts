import * as maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// maplibre-gl's worker file can't be discovered/bundled by Vite/Rollup; vite.config.ts's
// maplibreWorkerAssets plugin copies it to /assets/ verbatim, which this path must match.
// Without it, the map never fires `load` and hangs silently.
maplibregl.setWorkerUrl("/assets/maplibre-gl-worker.mjs");

// Verify this URL is live if a map ever shows a blank/broken basemap.
export const MAP_STYLE = "https://tiles.openfreemap.org/styles/positron";
