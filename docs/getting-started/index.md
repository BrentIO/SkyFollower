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
a `.env`, not a full source checkout — see Quick Start below. Cloning the
whole monorepo (see Advanced) is right for contributors, or anyone who
wants the full source.

## Quick Start

`scripts/install.sh` fetches just the compose file(s) a given host role
needs, without a `git clone`, prompts for the values it can't infer (with
sensible defaults where one exists), writes `.env`, and offers to bring the
stack up:

```bash
./scripts/install.sh

# or, without cloning anything first:
curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash
```

Run with no arguments, it prompts for which role(s) this host runs. To skip
that prompt, pass `--role` (repeatable — e.g. `--role core --role
management-ui` for the core host, which runs both):

```bash
./scripts/install.sh --role core --role management-ui
```

`<role>` is one of `receiver`, `core`, `management-ui`, `message-processor`,
or `archive` — matching the Compose Files table in
[Deployment](/deployment/#compose-files).

Other flags:

| Flag | Effect |
|---|---|
| `--root <path>` | Where role folders are created. Defaults to `~/SkyFollower`. |
| `--non-interactive` | Reads every value from already-exported environment variables (the same names written into `.env` — `RECEIVER_NAME`, `RABBITMQ_HOST`, etc.) instead of prompting. Requires `--role` at least once. Every missing required value is reported together as one error, not one per restart. |
| `--upgrade` | Re-resolves the latest release tag and runs `docker compose pull && up -d` in every role directory found under the install root, rewriting `SKYFOLLOWER_VERSION` in each. No prompting. |

Files are fetched from the **latest GitHub release**, not tip of `main` —
this matches whatever the `:latest` container images actually are, since
every image is only ever built and published on a release tag, never on
every push to `main` (set `REF=main`, or any other ref, to fetch
something else instead). A file that doesn't exist yet at the latest
release (e.g. a component added since the last one shipped) falls back to
`main` for that one file, with a printed warning.

The script writes a `.env` (mode `0600`) into the destination directory,
holding this host's Compose values — the image version
(`SKYFOLLOWER_VERSION`, pinned to the release the files were fetched
from), the Compose project name, the compose file to act on, the
destination's absolute path, and every credential/setting the role's
components need. Compose reads it automatically, which is why no `-f`
flag appears anywhere on this page. Editing `SKYFOLLOWER_VERSION` to an
older release tag and re-running `docker compose up -d` is the rollback
path (or re-run `install.sh --upgrade` after checking out an older tag
locally).

Once a role's `.env` is written, the script offers to bring it up
(`docker compose up -d`) right then — that's the complete command for
every role, including `core`, where the runner scheduler (`ofelia`)
comes up with everything else, while the `runner-*` services stay behind
the `runners` Compose profile so `up -d` never launches them all at once.
For `core`, it also offers to start `ofelia` and run a first-time bulk
data load — see [Loading All Data](#loading-all-data) below for what that
does and the manual fallback.

## Advanced

Clone the repo on every host that will run a SkyFollower component
instead, then write a `.env` by hand for whichever components run on that
host (see [Deployment](/deployment/#configuration) for the full
environment-variable reference) before starting containers. This is the
same information `install.sh` collects interactively — writing it by hand
just skips the prompts.

```bash
# Write .env alongside the compose file(s) this host runs. SKYFOLLOWER_ROOT
# must be an absolute path -- ofelia's scheduled jobs bind-mount through the
# Docker Engine API, which has no project directory to resolve a relative
# path against. Example for the core host:
cat > .env <<EOF
SKYFOLLOWER_VERSION=latest
COMPOSE_PROJECT_NAME=skyfollower-core
COMPOSE_FILE=docker-compose.core.yaml
SKYFOLLOWER_ROOT=$(pwd)
RABBITMQ_USERNAME=skyfollower
RABBITMQ_PASSWORD=changeme
RABBITMQ_ADMIN_USERNAME=skyfollower-admin
RABBITMQ_ADMIN_PASSWORD=changeme-too
REDIS_PASSWORD=changeme-as-well
MQTT_HOST=mqtt.example.com
MQTT_USERNAME=mqttuser
MQTT_PASSWORD=mqttpass
EOF

# ADS-B reception — receiver. Its compose file sets no project name of its
# own, so COMPOSE_PROJECT_NAME in .env is what keeps two receivers on one
# host apart (see receiver/README.md's "Running Multiple Receiver
# Instances"). Give each instance its own folder and its own .env:
docker compose up -d

# Dedicated MLAT receiver (optional; same image and compose file as
# above, either on its own host or alongside the first on this host --
# give it its own folder and its own .env with a different
# COMPOSE_PROJECT_NAME and RECEIVER_SOURCES):
docker compose up -d

# Core — message bus + enrichment data. ofelia comes up with it; the
# runner-* services stay behind their profile:
docker compose up -d
# The core host also runs the management UI, as its own compose file (its
# only dependency is Redis, already on this host) -- own directory, own
# .env, own COMPOSE_PROJECT_NAME:
docker compose up -d

# Message processor (scale by adding more hosts running this same file,
# or more processors on one host via COMPOSE_PROFILES=mp-2,mp-3,... in
# .env -- see message-processor/README.md)
docker compose up -d

# Archive
docker compose up -d
```

Once RabbitMQ reports healthy, provision its users by hand the same way
`install.sh` does automatically (see `scripts/install.sh`'s
`provision_rabbitmq_users` for the exact commands) — the application user
needs demoting from the full-admin state `RABBITMQ_DEFAULT_USER` always
creates it in, and the admin user needs creating.

## Loading All Data

`install.sh` offers a first-time bulk load right after bringing `core` up:
seeds Redis by running every runner once instead of waiting on each one's
own `ofelia` schedule (up to a week away for weekly runners). `mictronics`
runs first since most country runners resolve `icao_hex` against its
RediSearch index; the rest follow alphabetically, with `uk-caa-registry`
pushed to the end since it takes far longer than the others.

To do this manually instead — or to bulk-load again later — from the core
host's directory:

```bash
docker compose run --rm runner-mictronics
for svc in $(docker compose --profile runners config --services \
    | grep '^runner-' | grep -v -E '^runner-(mictronics|uk-caa-registry)$' | sort); do
  docker compose run --rm "$svc"
done
docker compose run --rm runner-uk-caa-registry
```

The runner list comes from `docker compose config` itself, not a separate
list, so it's always accurate for whatever's actually declared in
`docker-compose.core.yaml`.

To run a single data runner manually (e.g. re-importing one source without
touching the rest):
```bash
docker compose run --rm runner-ourairports
```

## Next steps

- [Deployment](/deployment/) — compose-file mapping and full environment-variable reference
- Component READMEs for anything role-specific: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [message processor](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md), [runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md)
