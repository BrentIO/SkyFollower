import { existsSync, mkdirSync, readFileSync } from "node:fs";
import { copyFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

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
// verbatim instead, in both dev and build.
const MAPLIBRE_WORKER_FILES = ["maplibre-gl-worker.mjs", "maplibre-gl-shared.mjs"];
const _here = path.dirname(fileURLToPath(import.meta.url));
const _maplibreDist = path.join(_here, "node_modules/maplibre-gl/dist");

function maplibreWorkerAssets(): Plugin {
  return {
    name: "maplibre-worker-assets",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const name = req.url?.replace(/^\/assets\//, "");
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
  plugins: [react(), maplibreWorkerAssets()],
  server: {
    proxy: {
      "/api": "http://localhost:8000",
    },
  },
});
