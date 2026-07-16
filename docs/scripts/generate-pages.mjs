#!/usr/bin/env node
// Generates docs/components/*.md and docs/data-runners/*.md from the source
// READMEs discovered by discover.mjs (see #434). Regenerated on every
// `docs:dev`/`docs:build` — output is gitignored so the source READMEs stay
// the single source of truth and the docs site can't drift from them.
//
// Content is copied rather than referenced via VitePress's `@include`
// because these READMEs also render as plain files on GitHub, where they
// need plain relative links to each other (e.g. `../processor/README.md`).
// Splicing that raw text into a docs page verbatim leaves a relative link
// that resolves against the *including* page's location instead, which
// VitePress's dead-link checker correctly rejects. Rewriting known
// `<dir>/README.md` links to their docs-site route here means the source
// stays GitHub-correct, the generated page stays docs-site-correct, and the
// link is still validated at build time — see #448.

import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { REPO_ROOT, discoverComponents, discoverRunners } from "./discover.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");

const components = discoverComponents();
for (const component of components) {
  if (!component.hasReadme) {
    // Flag the gap loudly rather than silently omitting the page.
    throw new Error(
      `docs generation: ${component.name}/README.md is missing — add it ` +
        `(or an explicit stub page) before the docs site can build. See #434.`,
    );
  }
}
const runners = discoverRunners();

// Maps a README's containing directory name (e.g. "processor") to the
// docs-site route it's generated to (e.g. "/components/processor"), so a
// source link like "../processor/README.md" can be rewritten to the page
// that actually renders it.
const ROUTE_BY_DIR = new Map([
  ...components.map((c) => [c.name, `/components/${c.name}`]),
  ...runners.map((r) => [r.name, `/data-runners/${r.name}`]),
]);

// Matches a markdown link target that is a relative path ending in
// `<dirname>/README.md`, e.g. "../processor/README.md" or
// "data-runners/ourairports/README.md".
const README_LINK_RE = /\]\((?:\.\.\/|[\w.-]+\/)*([\w.-]+)\/README\.md\)/g;

function rewriteReadmeLinks(content) {
  return content.replace(README_LINK_RE, (match, dirName) => {
    const route = ROUTE_BY_DIR.get(dirName);
    return route ? `](${route})` : match;
  });
}

function writePage(relPath, sourceReadmePath) {
  const target = join(DOCS_ROOT, relPath);
  mkdirSync(dirname(target), { recursive: true });
  const raw = readFileSync(sourceReadmePath, "utf-8");
  writeFileSync(target, rewriteReadmeLinks(raw));
}

for (const component of components) {
  writePage(`components/${component.name}.md`, component.readmePath);
}

rmSync(join(DOCS_ROOT, "data-runners"), { recursive: true, force: true });
for (const runner of runners) {
  writePage(`data-runners/${runner.name}.md`, runner.readmePath);
}
writePage("data-runners/index.md", join(REPO_ROOT, "data-runners", "README.md"));

console.log(
  `docs: generated ${components.length} component page(s) and ${runners.length} data runner page(s)`,
);
