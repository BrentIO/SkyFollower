# Getting Started

SkyFollower runs across up to four hosts (five if you add the optional
dedicated MLAT receiver), each bringing up exactly one Docker Compose file.
See [Deployment](/deployment/) for the full host topology and compose-file
mapping before you start — this page only covers the commands to actually
bring each host up.

Clone the repo on every host that will run a SkyFollower component, then
copy the example settings for whichever components run on that host (see
[Deployment](/deployment/#configuration) for the full list) and fill in
real values before starting containers.

Cloning the whole monorepo is right for contributors, but every
`docker-compose.*.yaml` file already references a pre-built
`ghcr.io/brentio/skyfollower-*` image — none of them build from source. If
you're deploying the finished product rather than developing it, a host
only needs its own compose file(s) and matching `config/*/*.example`
files, not the full source tree. `scripts/download-host-files.sh` fetches
just those, without a `git clone`:

```bash
./scripts/download-host-files.sh <role> [dest-dir]

# or, without cloning anything first:
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/download-host-files.sh \
  | bash -s -- <role>
```

`<role>` is one of `receiver`, `receiver-mlat`, `server`, `management-ui`,
`message-processor`, or `archive` — matching the Compose Files table in
[Deployment](/deployment/#compose-files). Host B runs the script twice
(`server` and `management-ui`), since it brings up both compose files.
`[dest-dir]` defaults to the current directory.

## Quick Start

```bash
# 1. Copy the example settings for each component on this host and fill in values
#    e.g. for Host B:
cp config/runners/settings.json.example config/runners/settings.json
cp config/ofelia/config.ini.example config/ofelia/config.ini
cp config/management-ui/settings.json.example config/management-ui/settings.json

# Host A — receiver
docker compose -f docker-compose.receiver.yaml up -d

# Host A2 — dedicated MLAT receiver (optional)
docker compose -f docker-compose.receiver-mlat.yaml up -d

# Host B — central server
docker compose -f docker-compose.server.yaml up -d
# Create runner containers in stopped state so ofelia can schedule them:
docker compose -f docker-compose.server.yaml --profile runners up --no-start
# Host B also runs the management UI, as its own compose file (its only
# dependency is Redis, already on this host):
docker compose -f docker-compose.management-ui.yaml up -d

# Host C — message processor
docker compose -f docker-compose.message-processor.yaml up -d

# Host D — archive
docker compose -f docker-compose.archive.yaml up -d
```

To run a single data runner manually (e.g. for a first-time import):
```bash
docker compose -f docker-compose.server.yaml run --rm runner-ourairports
```

To bulk-load *every* runner once (e.g. right after install, so Redis isn't
empty until each runner's first scheduled `ofelia` run — up to a week away
for weekly runners). `mictronics` goes first since most country runners
resolve `icao_hex` against its RediSearch index; the rest follow
alphabetically. The runner list comes from `docker compose config` itself,
not a separate list, so it's always accurate for whatever's actually
declared in `docker-compose.server.yaml`:
```bash
docker compose -f docker-compose.server.yaml run --rm runner-mictronics
for svc in $(docker compose -f docker-compose.server.yaml config --services \
    | grep '^runner-' | grep -v '^runner-mictronics$' | sort); do
  docker compose -f docker-compose.server.yaml run --rm "$svc"
done
```

## Next steps

- [Deployment](/deployment/) — host topology, compose-file mapping, and full component list
- Component READMEs for settings fields: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [message processor](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md), [runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md)
