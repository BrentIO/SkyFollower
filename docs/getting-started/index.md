# Getting Started

SkyFollower runs across several hosts — one more if you add an optional
additional receiver instance — each bringing up exactly one Docker Compose
file (the core host brings up two). See [Deployment](/deployment/) for the
full compose-file mapping before you start.

Every compose file references a pre-built `ghcr.io/brentio/skyfollower-*`
image, so a host only needs its own compose file(s) and a `.env` — no
source checkout. `scripts/install.sh` handles that for you; that's the
[Quick Start](#quick-start) below, and the right path for virtually every
install. Cloning the repo (see [Advanced](#advanced)) is only for
contributors, or anyone who wants the full source.

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
archive infrastructure inline during those roles' prompts. It needs one
elevated AWS credential for that step — either an existing AWS
access-portal / SSO session, or a one-time IAM user it prints a
least-privilege policy for and walks you through creating (and deletes
again afterwards). See [AWS Configuration](/aws-configuration). The `core`
role on its own needs nothing from AWS.

**Other flags**

| Flag | Effect |
|---|---|
| `--root <path>` | Where role folders are created. Default `~/SkyFollower`. |
| `--non-interactive` | Reads every value from already-exported environment variables instead of prompting (same names written to `.env` — e.g. `RECEIVER_NAME`, `RABBITMQ_HOST`). Requires `--role` at least once. |

Files come from the **latest GitHub release**, matching the `:latest`
container images. Set `REF=main` (or any other ref) as an environment
variable before the command to fetch something else instead.

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
`uk-caa-registry` pushed to the end since it takes far longer than the
others.

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

To run a single data runner manually (e.g. re-importing one source
without touching the rest):

```bash
docker compose run --rm runner-ourairports
```

## Advanced

### Installing from a clone

Already have the repo cloned (e.g. as a contributor)? `scripts/install.sh`
works the same way locally — same flags, no `curl` needed:

```bash
./scripts/install.sh
```

```bash
./scripts/install.sh --upgrade
```

### Writing `.env` by hand

Skip `install.sh` entirely: clone the repo on every host that runs a
component, then write each host's `.env` yourself — same variables the
script would have written; see [Deployment](/deployment/#configuration)
for the full reference. `SKYFOLLOWER_ROOT` must be an absolute path —
ofelia's scheduled jobs bind-mount through the Docker Engine API, which
has no project directory to resolve a relative path against.

Example `.env` for the core host:

```bash
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
```

Then, from each role's own directory (its own `.env`, its own
`COMPOSE_PROJECT_NAME`):

```bash
docker compose up -d
```

Notes per role:

- **Receiver** — one fixed `receiver/` folder and one shared `.env` per
  host; each instance is a generated service block in
  `docker-compose.receiver.yaml` (with its own `RECEIVER_NAME` /
  `RECEIVER_SOURCES` and `./data/skyfollower-receiver-{slug}`). Re-run
  `scripts/install.sh` for the `receiver` role to add another. Same
  shape as the message processor — see receiver/README.md's "Running
  Multiple Receiver Instances".
- **Core** — brings up `ofelia` with everything else; `runner-*` services
  stay behind the `runners` Compose profile so `up -d` never launches
  them all at once. The core host also runs `management-ui` as its own
  compose file, own directory, own `.env`.
- **Message processor** — scale by adding more hosts running this file,
  or more processors on one host; re-run `scripts/install.sh` for this
  role to add fleet-wide IDs to this node's generated compose file (see
  message-processor/README.md).
- **Archive** — no special notes.

Once RabbitMQ reports healthy, provision its users by hand the same way
`install.sh` does automatically (see `scripts/install.sh`'s
`provision_rabbitmq_users` for the exact commands) — the application user
needs demoting from the full-admin state `RABBITMQ_DEFAULT_USER` always
creates it in, and the admin user needs creating.

## Next steps

- [Deployment](/deployment/) — compose-file mapping and full environment-variable reference
- Component READMEs for anything role-specific: [receiver](https://github.com/BrentIO/SkyFollower/blob/main/receiver/README.md), [message processor](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md), [runners](https://github.com/BrentIO/SkyFollower/blob/main/runners/README.md)
