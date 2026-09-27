# Local Development

Setting up and maintaining a local checkout for developing or testing
SkyFollower — as opposed to [installing a release onto a
host](/getting-started/), which is what most of this repo's docs cover.
This page is the unified starting point; each service's own README stays
the source of truth for its specifics (endpoints, configuration,
internals) — this page links out to those rather than duplicating them.

## Prerequisites

- **Python 3.14** — every backend service (`receiver`, `message-processor`,
  `map`, `management-ui`'s backend, `archive-processor`, `archive-compaction`,
  `core-health`, `aws-setup`, the `runners/*`) targets this version; it's
  what each `Dockerfile` and the test workflow both build against.
- **Node 26** — the `map/frontend` and `management-ui/frontend` React
  apps, and the `docs/` site itself.
- **Docker** — for Redis (RedisJSON + Lua scripting, i.e.
  `redis/redis-stack-server`, not plain `redis`) and RabbitMQ, which most
  services need reachable even for local iteration. The simplest way to
  get both is to bring up the relevant role's compose file (see
  [Deployment](/deployment/)) and just not start the one service you're
  actively developing — point that service at `localhost` instead of the
  compose network's service names.
- **git**

## Setting up a Python service

Each Python component keeps its own `requirements.txt` (`receiver/`,
`message-processor/`, `map/`, `management-ui/`, `archive-processor/`,
`archive-compaction/`, `core-health/`, `aws-setup/`, `shared/`, each
`runners/*/`). Create a venv per component you're actively working on:

```bash
cd message-processor
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Running the service standalone (outside Docker) generally follows the same
module invocation its `Dockerfile`'s `CMD` uses — e.g.
`python -m receiver.main`, `python -m message_processor.main`, `python -m
map.main` — from the repo root, with the venv active and the service's
configuration supplied as environment variables (see that service's
README's Configuration section for the full variable list). Each service
expects its usual dependencies (Redis, RabbitMQ, etc.) to already be
reachable — see Prerequisites above.

### Running tests

Every component's tests live under its own `<component>/tests/` and are
discovered from the repo root via the root `pytest.ini`
(`--import-mode=importlib`, so components don't collide on a shared
top-level `tests` module name). `pytest` itself is a test-only dependency
— it's intentionally not in any `requirements.txt` — so install it (and
the other test-only extras CI uses) into whichever venv you're testing
with:

```bash
pip install pytest pytest-xdist pyyaml httpx2
```

Then, with that component's own `requirements.txt` also installed:

```bash
python -m pytest message-processor/tests
```

Some suites need Redis with RedisJSON + Lua scripting reachable at
`localhost:6379` (e.g. `shared/tests/test_merge_aircraft_lua.py`) —
`docker run --rm -p 6379:6379 redis/redis-stack-server:7.4.0-v8` covers
that. See [`CONTRIBUTING.md`](https://github.com/BrentIO/SkyFollower/blob/main/CONTRIBUTING.md)
and `.github/workflows/run-tests.yaml` for the exact CI invocation this
mirrors — CI runs each component in its own job with its own dependency
set, rather than one shared environment.

## Setting up a frontend

Both browser frontends are standalone Vite projects with their own
`package.json`:

```bash
cd map/frontend
npm install
npm run dev
```

```bash
cd management-ui/frontend
npm install
npm run dev
```

Each proxies its API calls to a backend running on `localhost` — see
[map/README.md](https://github.com/BrentIO/SkyFollower/blob/main/map/README.md#frontend-frontend)
and
[management-ui/README.md](https://github.com/BrentIO/SkyFollower/blob/main/management-ui/README.md#frontend-frontend)
for the exact dev-server port, proxy target, and any `VITE_*` env vars —
run that backend alongside (see [Setting up a Python
service](#setting-up-a-python-service) above).

`map/frontend` generates its aircraft-shape assets (`generate:shapes`)
from vector source SVGs before it can build or run — this is already
wired as an npm pre-hook on `dev`, `build`, `typecheck`, and `test` in
`map/frontend/package.json`, so `npm run dev` handles it automatically;
no separate step needed. `management-ui/frontend` has no such
generation step.

## Upgrading a local dev environment

This is about keeping an existing local checkout current — distinct from
[Upgrading](/getting-started/#upgrading) a *deployed* host, which is a
different operation (`install.sh --upgrade` against release tags).

1. **Pull latest:**

   ```bash
   git pull
   ```

2. **Reinstall changed dependencies**, per component you're working in:

   ```bash
   pip install -r <component>/requirements.txt   # Python services
   npm install                                    # inside map/frontend or management-ui/frontend
   ```

   There's no single command that does this across every component —
   `requirements.txt`/`package.json` only change for the components a
   given pull actually touched, so reinstall in whichever directories you
   use.

3. **Regenerate derived assets:** `map/frontend`'s aircraft-shape
   generation (see above) is already wired into its `npm` scripts'
   pre-hooks, so `npm run dev`/`npm run build`/`npm test` re-run it
   automatically whenever needed — no manual step. No other component has
   a comparable generation step today.

4. **Re-run tests** for anything you changed (see [Running
   tests](#running-tests) above) before opening a PR.

## Testing a Dev Build

Cutting a real release for every change under test is slow. `main` or any
branch can be built and published as a real, pullable image without a
release, using `build-container-images.yaml`'s `dev_mode` (requires push
access to the repo):

```bash
gh workflow run build-container-images.yaml --ref my-branch -f dev_mode=true
```

(`--ref main` builds the latest merged code.) This publishes every image
tagged `:dev-{branch}` and the floating `:dev` (the most recent dev build
on any branch) — never `:latest`, so a dev build can never be pulled by a
fresh production install by accident. The installer only ever selects
`:dev-{branch}`; the floating `:dev` stays available for a manual
`docker pull`.

Point a host at one with a **single** variable, `branch`. Its presence is
what makes the run a dev install; its value selects **both** the branch
whose compose/config files are fetched and the matching `:dev-{branch}`
images — they cannot desync. Put it on the `bash` side of the pipe, on a
single line — `curl`'s environment is a separate process from `bash`'s, so
anything set before `curl` never reaches the script that actually runs:

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | branch=my-branch bash
```

`branch=main` is the common "just give me the latest dev build" case.
Branch names containing `/` are sanitized to `-` to match the image tag —
pass the real branch name and the installer handles it.

Before it does anything, the installer checks that the `dev-{branch}`
images are actually published in GHCR; if they are not it prints the exact
`gh workflow run` command above and stops — no silent fallback to
`:latest` or a stale local image. Every run (fresh install **or** re-run)
pulls before bringing the stack up, so re-running with the same `branch`
always lands every component on the current dev build. A loud
`⚠️ DEVELOPMENT BUILD ⚠️` banner prints at the start and end of the run.

A dev build's images report their `VERSION` — and therefore Home Assistant
`sw_version` — as `9999.99.99` (the same "not a release" sentinel
`specs/*.yaml` carry on `main`), with the actual short commit hash appended in
parentheses (e.g. `9999.99.99 (abcdef01)`) so two dev builds are still
distinguishable from each other; the branch is recorded in `.env` as
`SKYFOLLOWER_VERSION=dev-{branch}`.

Already installed? `branch` works with `--upgrade` on an existing host too
(placed the same way, after the pipe):

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | branch=my-branch bash -s -- --upgrade
```

**Going back to a real release:** run `--upgrade` with no `branch` set — it
re-resolves the latest release tag exactly as normal.
