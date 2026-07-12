#!/usr/bin/env node
// Generates docs/components/*.md and docs/data-runners/*.md from the source
// READMEs discovered by discover.mjs (see #434). Regenerated on every
// `docs:dev`/`docs:build` — output is gitignored so the source READMEs stay
// the single source of truth and the docs site can't drift from them.

import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { discoverComponents, discoverRunners } from "./discover.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");

function writeIncludePage(relPath, includePath) {
  const target = join(DOCS_ROOT, relPath);
  mkdirSync(dirname(target), { recursive: true });
  writeFileSync(target, `<!--@include: ${includePath}-->\n`);
}

const components = discoverComponents();
for (const component of components) {
  if (!component.hasReadme) {
    // Flag the gap loudly rather than silently omitting the page.
    throw new Error(
      `docs generation: ${component.name}/README.md is missing — add it ` +
        `(or an explicit stub page) before the docs site can build. See #434.`,
    );
  }
  writeIncludePage(`components/${component.name}.md`, `../../${component.name}/README.md`);
}

rmSync(join(DOCS_ROOT, "data-runners"), { recursive: true, force: true });
const runners = discoverRunners();
for (const runner of runners) {
  writeIncludePage(`data-runners/${runner.name}.md`, `../../data-runners/${runner.name}/README.md`);
}
writeIncludePage("data-runners/index.md", "../../data-runners/README.md");

console.log(
  `docs: generated ${components.length} component page(s) and ${runners.length} data runner page(s)`,
);
