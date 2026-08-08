# Getting Started

SkyFollower runs across several hosts (one more if you add the optional
dedicated MLAT receiver), each bringing up exactly one Docker Compose file
(except the core host, which brings up two — see below). See
[Deployment](/deployment/) for the full compose-file mapping before you
start — this page only covers the commands to actually bring each host
up.

Every `docker-compose.*.yaml` file already references a pre-built
`ghcr.io/brentio/skyfollower-*` image — none of them build from source. A
host deploying the finished product only needs its own compose file(s) and
matching `config/*/*.example` files, not a full source checkout — see
Quick Start below. Cloning the whole monorepo (see Advanced) is right for
contributors, or anyone who wants the full source.

## Quick Start

`scripts/download-host-files.sh` fetches just the compose file(s) and
`config/*/*.example` files a given host role needs, without a `git clone`,
and creates a real config file from each `.example` template (skipping any
that already exist, so a re-run into the same directory never overwrites
values you've already filled in):

```bash
./scripts/download-host-files.sh <role> [dest-dir]

# or, without cloning anything first:
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/download-host-files.sh \
  | bash -s -- <role>
```

`<role>` is one of `receiver`, `receiver-mlat`, `core`, `management-ui`,
`message-processor`, or `archive` — matching the Compose Files table in
[Deployment](/deployment/#compose-files). The core host runs the script
twice (`core` and `management-ui`), since it brings up both compose
files. `[dest-dir]` defaults to the current directory.

Files are fetched from the **latest GitHub release**, not tip of `main` —
this matches whatever the `:latest` container images actually are, since
every image is only ever built and published on a release tag, never on
every push to `main` (set `REF=main`, or any other ref, to fetch
something else instead). A file that doesn't exist yet at the latest
release (e.g. a component added since the last one shipped) falls back to
`main` for that one file, with a printed warning.

The script also writes a `.env` (mode `0600`) into the destination
directory, holding this host's Compose values — the image version
(`SKYFOLLOWER_VERSION`, pinned to the release the files were fetched
from), the Compose project name, the compose file to act on, and the
destination's absolute path. Compose reads it automatically, which is why
no `-f` flag appears below. Editing `SKYFOLLOWER_VERSION` to an older
release tag and re-running `docker compose up -d` is the rollback path.

Once the files are downloaded, fill in real values in each generated
`config/*/*` file, then bring the host up from that directory:
```bash
docker compose up -d
```

That's the complete command for every role, including `core` — the runner
scheduler (`ofelia`) comes up with everything else, while the
runner-\* services stay behind the `runners` profile so `up -d` never
launches them all at once. See [Loading All Data](#loading-all-data)
below to do a first-time data import instead of waiting on each runner's
own scheduled run.

## Advanced

Clone the repo on every host that will run a SkyFollower component
instead, then copy the example settings for whichever components run on
that host (see [Deployment](/deployment/#configuration) for the full
list) and fill in real values before starting containers.

```bash
# 1. Copy the example settings for each component on this host and fill in values
#    e.g. for the core host:
cp config/runners/settings.json.example config/runners/settings.json
cp config/management-ui/settings.json.example config/management-ui/settings.json
cp config/rabbitmq/rabbitmq.conf.example config/rabbitmq/rabbitmq.conf
cp config/rabbitmq/enabled_plugins.example config/rabbitmq/enabled_plugins

# 2. Write a .env alongside the compose files. Quick Start generates this
#    for you; on a full clone, create it by hand. SKYFOLLOWER_ROOT must be
#    an absolute path -- ofelia's scheduled jobs bind-mount through the
#    Docker Engine API, which has no project directory to resolve a
#    relative path against:
cat > .env <<EOF
SKYFOLLOWER_VERSION=latest
COMPOSE_PROJECT_NAME=skyfollower-core
COMPOSE_FILE=docker-compose.core.yaml
SKYFOLLOWER_ROOT=$(pwd)
EOF

# ADS-B reception — receiver. Its compose file sets no project name, so
# COMPOSE_PROJECT_NAME in .env is what keeps two receivers on one host
# apart (see receiver/README.md's "Running Multiple Receiver Instances"):
docker compose -f docker-compose.receiver.yaml up -d

# Dedicated MLAT receiver (optional; same image and compose file, either on
# its own host or alongside the first on this host -- give it its own
# folder, its own .env with a different COMPOSE_PROJECT_NAME, and its own
# settings.json):
cp config/receiver/mlat-settings.json.example config/receiver/mlat-settings.json

# Core — message bus + enrichment data. ofelia comes up with it; the
# runner-* services stay behind their profile:
docker compose -f docker-compose.core.yaml up -d
# The core host also runs the management UI, as its own compose file (its
# only dependency is Redis, already on this host):
docker compose -f docker-compose.management-ui.yaml up -d

# Message processor (scale by adding more hosts running this same file)
docker compose -f docker-compose.message-processor.yaml up -d

# Archive
docker compose -f docker-compose.archive.yaml up -d
```

## Loading All Data

To run a single data runner manually (e.g. for a first-time import), from
the core host's directory:
```bash
docker compose run --rm runner-ourairports
```

To bulk-load *every* runner once (e.g. right after install, so Redis isn't
empty until each runner's first scheduled `ofelia` run — up to a week away
for weekly runners). `mictronics` goes first since most country runners
resolve `icao_hex` against its RediSearch index; the rest follow
alphabetically, with `uk-caa-registry` pushed to the end since it takes far
longer than the others. The runner list comes from `docker compose config`
itself, not a separate list, so it's always accurate for whatever's actually
declared in `docker-compose.core.yaml`:
```bash
docker compose run --rm runner-mictronics
for svc in $(docker compose --profile runners config --services \
    | grep '^runner-' | grep -v -E '^runner-(mictronics|uk-caa-registry)$' | sort); do
  docker compose run --rm "$svc"
done
docker compose run --rm runner-uk-caa-registry
```

## Next steps

- [Deployment](/deployment/) — compose-file mapping and full component list
- Component READMEs for settings fields: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [message processor](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md), [runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md)
