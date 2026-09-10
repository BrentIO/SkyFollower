import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { copyFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// maplibre-gl ships its worker as a separate file (maplibre-gl-worker.mjs)
// that itself has a hardcoded relative import of a second file
// (maplibre-gl-shared.mjs) -- both need to be copied as a pair, under
// their exact original (unhashed) names, into the same directory, since
// the worker's own `import ... from "./maplibre-gl-shared.mjs"` is
// resolved by the browser at Worker-script-execution time, not by
// Vite/Rollup at our build time (unlike our main-thread `import * as
// maplibregl from "maplibre-gl"`, which Rollup inlines normally). A
// content-hashed filename (Vite's usual `?url` import mechanism) would
// break that hardcoded relative import, so this copies both files
// verbatim instead, in both dev and build. Ported verbatim from
// management-ui/frontend/vite.config.ts -- see that file's comment for
// the full explanation; this is a separate, standalone frontend project
// so it carries its own copy rather than importing across the two.
const MAPLIBRE_WORKER_FILES = ["maplibre-gl-worker.mjs", "maplibre-gl-shared.mjs"];
const _here = path.dirname(fileURLToPath(import.meta.url));
const _maplibreDist = path.join(_here, "node_modules/maplibre-gl/dist");

function maplibreWorkerAssets(): Plugin {
  return {
    name: "maplibre-worker-assets",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        // Matches ".../assets/<file>" regardless of what (if anything)
        // precedes "assets/" -- with base: "/map/" set below, the browser
        // requests "/map/assets/maplibre-gl-worker.mjs" (per
        // maplibreSetup.ts's import.meta.env.BASE_URL-prefixed URL), not
        // the bare "/assets/..." this middleware originally only matched.
        const name = req.url?.match(/\/assets\/([^/?]+)(?:\?.*)?$/)?.[1];
        if (!name || !MAPLIBRE_WORKER_FILES.includes(name)) {
          next();
          return;
        }
        res.setHeader("Content-Type", "application/javascript");
        res.end(readFileSync(path.join(_maplibreDist, name)));
      });
    },
    async closeBundle() {
      const outDir = path.join(_here, "dist/assets");
      mkdirSync(outDir, { recursive: true });
      for (const name of MAPLIBRE_WORKER_FILES) {
        const src = path.join(_maplibreDist, name);
        if (existsSync(src)) {
          await copyFile(src, path.join(outDir, name));
        }
      }
    },
  };
}

// https://vite.dev/config/
export default defineConfig({
  // map/main.py mounts the built dist/ under /map (see its _SPAStaticFiles
  // mount), so built asset URLs must be emitted rooted at /map/ rather
  // than / -- both dist/index.html's own <script>/<link> tags (handled by
  // Vite automatically) and the maplibre-gl worker URL src/lib/
  // maplibreSetup.ts sets explicitly at runtime via
  // import.meta.env.BASE_URL, which Vite derives from this same setting.
  base: "/map/",
  plugins: [react(), tailwindcss(), maplibreWorkerAssets()],
  server: {
    proxy: {
      // Proxies both the REST snapshot and the WebSocket (ws: true) during
      // `vite dev` so VITE_MAP_API_BASE_URL can be left unset locally. Targets
      // an unprivileged plain-HTTP port a local `python -m map.main` can bind
      // without root and without a TLS cert -- not the production MAP_HTTP_PORT
      // default (443), which is privileged and HTTPS. Run the dev backend with
      // MAP_HTTP_PORT=8080.
      "/api": "http://localhost:8080",
      "/ws": { target: "ws://localhost:8080", ws: true },
    },
  },
});
