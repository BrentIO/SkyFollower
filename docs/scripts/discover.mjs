// Shared filesystem discovery for docs generation (see #434).
//
// Components are a fixed, explicit set — mirroring the explicit
// receiver/processor/archive-processor/ui cases in
// .github/workflows/build-images.yaml's discover-images job (shared has no
// Dockerfile/image but still gets a docs page). Data runners are discovered
// dynamically from the filesystem, mirroring that same job's `data-runners/*`
// wildcard case, so a new runner directory picks up a docs page with no
// changes here.

import { existsSync, readFileSync, readdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
export const REPO_ROOT = join(__dirname, "..", "..");

const COMPONENT_DIRS = ["receiver", "processor", "shared", "archive-processor"];

// Sidebar label per component, decoupled from the page <title>/H1 (which
// stays whatever the README says — e.g. "SkyFollower Archive Processor").
// An explicit map here means a future README wording change can't silently
// change what the sidebar shows, the way stripping a "SkyFollower " prefix
// with a regex would.
const SIDEBAR_LABELS = {
  receiver: "Receiver",
  processor: "Message Processor",
  "archive-processor": "Archive Processor",
  shared: "Shared Data Models",
};

function h1Title(readmePath, fallback) {
  const firstLine = readFileSync(readmePath, "utf-8").split("\n", 1)[0];
  const match = firstLine.match(/^#\s+(.*\S)\s*$/);
  return match ? match[1] : fallback;
}

export function discoverComponents() {
  return COMPONENT_DIRS.map((name) => {
    const readmePath = join(REPO_ROOT, name, "README.md");
    const hasReadme = existsSync(readmePath);
    return {
      name,
      readmePath,
      hasReadme,
      title: hasReadme ? h1Title(readmePath, name) : name,
      sidebarLabel: SIDEBAR_LABELS[name] ?? name,
    };
  });
}

export function discoverRunners() {
  const runnersDir = join(REPO_ROOT, "data-runners");
  return readdirSync(runnersDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .filter((name) => existsSync(join(runnersDir, name, "README.md")))
    .sort()
    .map((name) => {
      const readmePath = join(runnersDir, name, "README.md");
      return { name, readmePath, title: h1Title(readmePath, name) };
    });
}
