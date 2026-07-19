#!/usr/bin/env node
// Renders every PlantUML (.puml) diagram source in the repo to SVG (see
// #435, #463), matching the FireFly-Docs precedent. Requires the `plantuml`
// command on PATH — .github/workflows/deploy-docs.yaml installs it via a
// JAR wrapper before running `npm run docs:build`. Locally, a contributor
// without PlantUML installed just won't see rendered diagrams (a missing
// SVG is a broken <img>, not a broken build) since CI is the authoritative
// build for the deployed site. Output SVGs are gitignored — regenerated
// from the .puml source every build.
//
// Diagram sources aren't confined to docs/ (see #463): a component's own
// architecture diagram (e.g. receiver/receiver.puml) lives next to its
// source and README, and gets `!include`d into docs/architecture/images/
// pipeline.puml as a reusable fragment (via PlantUML's !startsub/!endsub +
// `!include file.puml!NAME`) as well as rendered standalone for that
// component's own docs page. A component isn't limited to one diagram —
// e.g. archive-processor/ has both its architecture diagram and a sequence
// diagram for a specific behavior — any .puml directly in a component's
// directory gets picked up. Two different "make the rendered SVG land
// somewhere a generated docs page can reference" steps follow from that:
//
// 1. For .puml files already under docs/ (pipeline.puml itself, and its
//    non-component fragments like central-server.puml/aws.puml), the
//    rendered SVG is mirrored into public/ at its literal relative path.
//    VitePress's asset pipeline rewrites a markdown image's <img src> to a
//    hashed /assets/ URL, but does *not* do the same for a plain link
//    wrapped around that image (the [![alt](path)](path) "click to
//    enlarge" pattern used on the architecture page) — that link's href
//    stays the literal relative path, which 404s unless a real file exists
//    there too. Mirroring into public/ is the same fix FireFly-Docs uses
//    for the same reason.
// 2. For a component's own .puml file(s) (outside docs/ entirely), each
//    rendered SVG is copied into docs/components/<basename>.svg — a
//    sibling of the generated docs/components/<name>.md page (see
//    generate-pages.mjs). Since the source README embeds a diagram with a
//    same-basename relative reference (e.g. `./receiver.svg`, sitting next
//    to receiver/README.md), and the copied SVG lands at the identical
//    relative position next to the generated page, no link-rewriting is
//    needed — the reference resolves unchanged. This intentionally does
//    *not* render on GitHub (the source .svg is gitignored, never
//    committed) — docs-site only, see #463.

import { spawnSync } from "node:child_process";
import { copyFileSync, mkdirSync, readdirSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";
import { REPO_ROOT, discoverComponents } from "./discover.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");
const PUBLIC_DIR = join(DOCS_ROOT, "public");

const SKIP_DIRS = new Set(["node_modules", ".git", ".vitepress", "__pycache__"]);

function findPumlFiles(dir) {
  const results = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (SKIP_DIRS.has(entry.name) || entry.name.startsWith(".")) continue;
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...findPumlFiles(full));
    } else if (entry.name.endsWith(".puml")) {
      results.push(full);
    }
  }
  return results;
}

const pumlFiles = findPumlFiles(REPO_ROOT);
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

// Step 1: mirror .puml files already under docs/ into public/ at their
// literal relative path (unchanged from #435/#458).
for (const file of pumlFiles) {
  if (!file.startsWith(DOCS_ROOT)) continue;
  const svgPath = file.replace(/\.puml$/, ".svg");
  const dest = join(PUBLIC_DIR, relative(DOCS_ROOT, svgPath));
  mkdirSync(dirname(dest), { recursive: true });
  copyFileSync(svgPath, dest);
}

// Step 2: copy each component's own diagram SVG(s) next to its generated
// docs page (see #463) — every .puml directly in the component's
// directory, not just one matching the component's own name.
const componentsDir = join(DOCS_ROOT, "components");
for (const component of discoverComponents()) {
  for (const entry of readdirSync(component.dir, { withFileTypes: true })) {
    if (!entry.isFile() || !entry.name.endsWith(".puml")) continue;
    const svgName = entry.name.replace(/\.puml$/, ".svg");
    const svgPath = join(component.dir, svgName);
    const dest = join(componentsDir, svgName);
    mkdirSync(dirname(dest), { recursive: true });
    copyFileSync(svgPath, dest);
  }
}

console.log(`docs: generated ${pumlFiles.length} PlantUML diagram(s)`);
