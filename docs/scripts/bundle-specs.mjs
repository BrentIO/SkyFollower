#!/usr/bin/env node
// Copies each interactive spec viewer's web component bundle + the repo's
// own spec file into docs/public/{asyncapi,openapi}/ so the viewers have
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

const asyncapiOutDir = join(DOCS_ROOT, "public", "asyncapi");
mkdirSync(asyncapiOutDir, { recursive: true });
copyFileSync(
  join(DOCS_ROOT, "node_modules", "@asyncapi", "web-component", "lib", "asyncapi-web-component.js"),
  join(asyncapiOutDir, "web-component.js"),
);
copyFileSync(
  join(DOCS_ROOT, "node_modules", "@asyncapi", "react-component", "styles", "default.min.css"),
  join(asyncapiOutDir, "default.min.css"),
);
copyFileSync(join(REPO_ROOT, "specs", "asyncapi.yaml"), join(asyncapiOutDir, "asyncapi.yaml"));
console.log("docs: bundled AsyncAPI web component + specs/asyncapi.yaml into public/asyncapi/");

const openapiOutDir = join(DOCS_ROOT, "public", "openapi");
mkdirSync(openapiOutDir, { recursive: true });
copyFileSync(
  join(DOCS_ROOT, "node_modules", "@stoplight", "elements", "web-components.min.js"),
  join(openapiOutDir, "web-components.min.js"),
);
copyFileSync(
  join(DOCS_ROOT, "node_modules", "@stoplight", "elements", "styles.min.css"),
  join(openapiOutDir, "styles.min.css"),
);
copyFileSync(join(REPO_ROOT, "specs", "openapi.yaml"), join(openapiOutDir, "openapi.yaml"));
console.log("docs: bundled Stoplight Elements web component + specs/openapi.yaml into public/openapi/");
