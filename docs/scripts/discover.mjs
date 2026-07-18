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

function h1Title(readmePath, fallback) {
  const firstLine = readFileSync(readmePath, "utf-8").split("\n", 1)[0];
  const match = firstLine.match(/^#\s+(.*\S)\s*$/);
  return match ? match[1] : fallback;
}

export function discoverComponents() {
  return COMPONENT_DIRS.map((name) => {
    const readmePath = join(REPO_ROOT, name, "README.md");
    const hasReadme = existsSync(readmePath);
    // A component's architecture diagram, if it has one (see #463) — e.g.
    // receiver/receiver.puml. Not every component has one (shared doesn't).
    const pumlPath = join(REPO_ROOT, name, `${name}.puml`);
    const hasPuml = existsSync(pumlPath);
    return {
      name,
      readmePath,
      hasReadme,
      title: hasReadme ? h1Title(readmePath, name) : name,
      pumlPath: hasPuml ? pumlPath : null,
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
