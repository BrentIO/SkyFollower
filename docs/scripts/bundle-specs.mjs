#!/usr/bin/env node
// Copies the AsyncAPI web component bundle + the repo's specs/asyncapi.yaml
// into docs/public/asyncapi/ so the interactive spec viewer (see #433) has
// something to fetch at runtime. Regenerated on every docs:dev/docs:build —
// output is gitignored, matching generate-pages.mjs's approach: nothing
// binary or spec-derived is committed, it's all copied from node_modules /
// specs/ at build time.

import { copyFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { REPO_ROOT } from "./discover.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");
const OUT_DIR = join(DOCS_ROOT, "public", "asyncapi");

mkdirSync(OUT_DIR, { recursive: true });

copyFileSync(
  join(DOCS_ROOT, "node_modules", "@asyncapi", "web-component", "lib", "asyncapi-web-component.js"),
  join(OUT_DIR, "web-component.js"),
);
copyFileSync(
  join(DOCS_ROOT, "node_modules", "@asyncapi", "react-component", "styles", "default.min.css"),
  join(OUT_DIR, "default.min.css"),
);
copyFileSync(join(REPO_ROOT, "specs", "asyncapi.yaml"), join(OUT_DIR, "asyncapi.yaml"));

console.log("docs: bundled AsyncAPI web component + specs/asyncapi.yaml into public/asyncapi/");
