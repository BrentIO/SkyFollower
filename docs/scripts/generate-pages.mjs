#!/usr/bin/env node
// Generates docs/components/*.md, docs/runners/*.md, and docs/tools/*.md
// from the source READMEs discovered by discover.mjs. Regenerated on every
// `docs:dev`/`docs:build` — output is gitignored so the source READMEs stay
// the single source of truth and the docs site can't drift from them.
//
// Content is copied rather than referenced via VitePress's `@include`
// because these READMEs also render as plain files on GitHub, where they
// need plain relative links to each other (e.g. `../message-processor/README.md`).
// Splicing that raw text into a docs page verbatim leaves a relative link
// that resolves against the *including* page's location instead, which
// VitePress's dead-link checker correctly rejects. Rewriting known
// `<dir>/README.md` links to their docs-site route here means the source
// stays GitHub-correct, the generated page stays docs-site-correct, and the
// link is still validated at build time.

import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { REPO_ROOT, discoverComponents, discoverRunners, discoverTools } from "./discover.mjs";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = join(__dirname, "..");

const components = discoverComponents();
for (const component of components) {
  if (!component.hasReadme) {
    // Flag the gap loudly rather than silently omitting the page.
    throw new Error(
      `docs generation: ${component.name}/README.md is missing — add it ` +
        `(or an explicit stub page) before the docs site can build.`,
    );
  }
}
const runners = discoverRunners();
const tools = discoverTools();

// Maps a README's containing directory name (e.g. "message-processor") to the
// docs-site route it's generated to (e.g. "/components/message-processor"), so a
// source link like "../message-processor/README.md" can be rewritten to the page
// that actually renders it.
const ROUTE_BY_DIR = new Map([
  ...components.map((c) => [c.name, `/components/${c.name}`]),
  ...runners.map((r) => [r.name, `/runners/${r.name}`]),
  ...tools.map((t) => [t.name, `/tools/${t.name}`]),
]);

// Matches a markdown link target that is a relative path ending in
// `<dirname>/README.md`, e.g. "../message-processor/README.md" or
// "runners/ourairports/README.md", with an optional trailing `#heading`
// anchor into a specific section, e.g. "../archive-processor/README.md#local-index-cache".
const README_LINK_RE = /\]\((?:\.\.\/|[\w.-]+\/)*([\w.-]+)\/README\.md(#[\w-]*)?\)/g;

// Matches a markdown link target that is a relative path into docs/ itself
// (a flat page, not a component README), e.g. "../docs/aws-setup.md" from
// archive-processor/README.md. VitePress serves docs/<page>.md at the
// extensionless route /<page>.
const DOCS_PAGE_LINK_RE = /\]\((?:\.\.\/)*docs\/([\w.-]+)\.md\)/g;

function rewriteReadmeLinks(content) {
  return content
    .replace(README_LINK_RE, (match, dirName, anchor = "") => {
      const route = ROUTE_BY_DIR.get(dirName);
      return route ? `](${route}${anchor || ""})` : match;
    })
    .replace(DOCS_PAGE_LINK_RE, (match, page) => `](/${page})`);
}

function writePage(relPath, sourceReadmePath, frontmatterTitle) {
  const target = join(DOCS_ROOT, relPath);
  mkdirSync(dirname(target), { recursive: true });
  const raw = readFileSync(sourceReadmePath, "utf-8");
  const body = rewriteReadmeLinks(raw);
  // The sidebar entry and on-page heading (both derived from the README's
  // H1) stay just the friendly name; only the browser-tab title also gets
  // the runner's directory name appended, so it needs its own frontmatter
  // override rather than changing the H1 itself.
  const content = frontmatterTitle
    ? `---\ntitle: "${frontmatterTitle.replace(/"/g, '\\"')}"\n---\n\n${body}`
    : body;
  writeFileSync(target, content);
}

for (const component of components) {
  writePage(`components/${component.name}.md`, component.readmePath);
}

rmSync(join(DOCS_ROOT, "runners"), { recursive: true, force: true });
for (const runner of runners) {
  // The README H1 (sidebar/heading text) omits the trailing "Runner" word;
  // the browser-tab title adds it back plus the directory name, since the
  // longer form reads better as a page title than in a sidebar list.
  writePage(`runners/${runner.name}.md`, runner.readmePath, `${runner.title} Runner (${runner.name})`);
}
writePage("runners/index.md", join(REPO_ROOT, "runners", "README.md"));

// Unlike runners/index.md (generated from runners/README.md), tools/index.md
// is hand-authored and checked in — there's no tools/README.md at the repo
// root to source it from — so only the per-tool pages are generated here.
for (const tool of tools) {
  writePage(`tools/${tool.name}.md`, tool.readmePath, `${tool.title} (${tool.name})`);
}

console.log(
  `docs: generated ${components.length} component page(s), ${runners.length} runner page(s), ` +
    `and ${tools.length} tool page(s)`,
);
