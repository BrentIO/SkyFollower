# Getting Started

SkyFollower runs across several hosts — one more if you add an optional
additional receiver instance — each bringing up exactly one Docker Compose
file (the core host brings up two). See [Deployment](/deployment/) for the
full compose-file mapping before you start.

Every compose file references a pre-built `ghcr.io/brentio/skyfollower-*`
image, so a host only needs its own compose file(s) and a `.env` — no
source checkout. `scripts/install.sh` handles that for you; that's the
[Quick Start](#quick-start) below, and the right path for every install.

## Quick Start

Run on each host — no `git clone` required:

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash
```

It prompts for which role(s) this host runs, prompts for anything else it
can't infer, writes `.env`, and offers to bring the stack up right then.
`<role>` is one of `receiver`, `core`, `management-ui`,
`message-processor`, or `archive` — see the [Compose Files
table](/deployment/#compose-files).

To skip the role prompt, pass one or more `--role` (the core host runs
both `core` and `management-ui`):

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash -s -- --role core --role management-ui
```

Installing `archive` or `management-ui`? The installer provisions the
archive infrastructure inline during those roles' prompts and asks for one
elevated AWS credential to do it — either an existing AWS access-portal /
SSO session, or a one-time IAM user it prints a least-privilege policy for
and walks you through creating (and offers to delete again afterwards).
See [AWS Configuration](/aws-configuration). The `core` role on its own
needs nothing from AWS.

**Other flags**

| Flag | Effect |
|---|---|
| `--root <path>` | Where role folders are created. Default `~/SkyFollower`. |
| `--non-interactive` | Reads every value from already-exported environment variables instead of prompting (same names written to `.env` — e.g. `RECEIVER_NAME`, `RABBITMQ_HOST`). Requires `--role` at least once. |

Files come from the **latest GitHub release**, matching the `:latest`
container images. Installing or testing a **development build** instead
of a release is a contributor workflow — see [Testing a Dev
Build](/architecture/local-development#testing-a-dev-build) in the
Architecture docs.

Once `core` is up, the script offers a first-time bulk data load — see
[Loading All Data](#loading-all-data) below.

## Upgrading

Run on every host that has SkyFollower installed:

```bash
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash -s -- --upgrade
```

No prompts: this re-resolves the latest release tag, rewrites
`SKYFOLLOWER_VERSION` in every role directory under the install root, and
runs `docker compose pull && up -d` for each.

**Rolling back:** edit `SKYFOLLOWER_VERSION` in that host's `.env` to an
older release tag, then `docker compose up -d`.

## Loading All Data

`install.sh` offers a first-time bulk load right after bringing `core`
up: seeds Redis by running every runner once instead of waiting on each
one's own `ofelia` schedule (up to a week away for weekly runners).
`mictronics` runs first since most country runners resolve `icao_hex`
against its RediSearch index; the rest follow alphabetically, with
`cz-caa-registry` and then `uk-caa-registry` pushed to the end since both
take far longer than the others — each does a per-record detail fetch
(`cz-caa-registry` with a 0.25s delay between requests, `uk-caa-registry`
with 676+ prefix searches on top). `cz-caa-registry` runs immediately
before `uk-caa-registry`. The whole sequence can take hours, so accepting
the offer runs it detached from the installer (it keeps going after the
installer moves on to the next role, or exits entirely) and prints the
path to a log file to `tail -f` for progress; `docker compose ps` in the
core host's directory also shows whichever runner is currently mid-run.

To do this manually instead — or to bulk-load again later — from the core
host's directory:

```bash
docker compose run --rm runner-mictronics
for svc in $(docker compose --profile runners config --services \
    | grep '^runner-' | grep -v -E '^runner-(mictronics|cz-caa-registry|uk-caa-registry)$' | sort); do
  docker compose run --rm "$svc"
done
docker compose run --rm runner-cz-caa-registry
docker compose run --rm runner-uk-caa-registry
```

The runner list comes from `docker compose config` itself, not a separate
list, so it's always accurate for whatever's actually declared in
`docker-compose.core.yaml`.

To run a single data runner manually (e.g. re-importing one source
without touching the rest):

```bash
docker compose run --rm runner-ourairports
```

## Next steps

- [Deployment](/deployment/) — compose-file mapping and full environment-variable reference
- Component READMEs for anything role-specific: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [message processor](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md), [runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md)
- Building or testing SkyFollower from source instead of a release?
  See [Local Development](/architecture/local-development) in the
  Architecture docs.
