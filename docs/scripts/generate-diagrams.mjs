#!/usr/bin/env node
// Renders every PlantUML (.puml) diagram source under docs/ to SVG (see
// #435), matching the FireFly-Docs precedent. Requires the `plantuml`
// command on PATH — .github/workflows/deploy-docs.yaml installs it via a
// JAR wrapper before running `npm run docs:build`. Locally, a contributor
// without PlantUML installed just won't see rendered diagrams (a missing
// SVG is a broken <img>, not a broken build) since CI is the authoritative
// build for the deployed site. Output SVGs are gitignored — regenerated
// from the .puml source every build.
//
// Each rendered SVG is also copied into public/ at its literal relative
// path (e.g. architecture/images/pipeline.svg -> public/architecture/
// images/pipeline.svg). VitePress's asset pipeline rewrites a markdown
// image's <img src> to a hashed /assets/ URL, but does *not* do the same
// for a plain link wrapped around that image (the
// [![alt](path)](path) "click to enlarge" pattern used on the
// architecture page) — that link's href stays the literal relative path,
// which 404s unless a real file exists there too. Mirroring into public/
// is the same fix FireFly-Docs uses for the same reason.

import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");
const PUBLIC_DIR = join(DOCS_ROOT, "public");

function findPumlFiles(dir) {
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (entry.name === "node_modules" || entry.name === ".vitepress") continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findPumlFiles(full));
    } else if (entry.name.endsWith(".puml")) {
      results.push(full);
    }
  }
  return results;
}

const pumlFiles = findPumlFiles(DOCS_ROOT);
if (pumlFiles.length === 0) {
  console.log("docs: no PlantUML diagrams found, nothing to generate");
  process.exit(0);
}

const check = spawnSync("plantuml", ["-version"], { stdio: "ignore" });
if (check.error) {
  console.warn(
    `docs: 'plantuml' not found on PATH — skipping diagram generation for ` +
      `${pumlFiles.length} file(s). Diagrams will render as broken images ` +
      `locally; CI installs PlantUML and generates them for the deployed site.`,
  );
  process.exit(0);
}

for (const file of pumlFiles) {
  const result = spawnSync("plantuml", ["-tsvg", file], { stdio: "inherit" });
  if (result.status !== 0) {
    throw new Error(`docs: plantuml failed to render ${file}`);
  }
}

for (const file of pumlFiles) {
  const svgPath = file.replace(/\.puml$/, ".svg");
  const dest = join(PUBLIC_DIR, relative(DOCS_ROOT, svgPath));
  mkdirSync(dirname(dest), { recursive: true });
  copyFileSync(svgPath, dest);
}

console.log(`docs: generated ${pumlFiles.length} PlantUML diagram(s)`);
