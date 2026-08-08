#!/usr/bin/env bash
#
# Downloads only the docker-compose.*.yaml file and any config/*/*.example
# file(s) a given host role needs, without a full `git clone` of the
# monorepo, and writes that role's .env with every environment variable its
# components read. Mirrors the host/file mapping documented in
# docs/deployment/index.md's Compose Files and Configuration tables --
# update both places together if that mapping ever changes.
#
# Usage:
#   ./download-host-files.sh <role> [dest-dir]
#
# Or, without cloning anything first:
#   curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/download-host-files.sh | bash -s -- <role>
#
# <role> is one of: receiver, receiver-mlat, core, management-ui,
# message-processor, archive
#
# Files are fetched from the latest GitHub *release tag* by default (REF
# env var overrides, e.g. REF=main for bleeding-edge). Every image is
# only ever built and published on a release tag (see
# .github/workflows/build-container-images.yaml's `on: push: tags:`
# trigger -- there is no build-on-every-push-to-main path), so a
# docker-compose.*.yaml's image: :latest always corresponds to the most
# recent release commit, not tip of main. main moves continuously between
# releases; fetching config from main by default would risk downloading
# a newer config/compose shape than the :latest image it's meant to run
# alongside actually understands.
#
# Any individual file missing at that release tag (a release cut before
# a given role/component existed at all -- expect this until the next
# release ships) falls back to main for that one file, with a printed
# warning -- not a hard failure, and not a blanket switch to main for
# every file in the role.

set -euo pipefail

# jq isn't used for the tag_name parsing below (see the "No jq assumption"
# comment further down -- that stays grep/cut on purpose), but it's a
# reasonable baseline dependency to require up front and fail fast on,
# rather than midway through a partially-completed download.
if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required but not installed." >&2
  echo "Install it, then re-run this script:" >&2
  echo "  Debian/Ubuntu/Raspberry Pi OS: sudo apt-get install -y jq" >&2
  echo "  Fedora/RHEL:                   sudo dnf install -y jq" >&2
  echo "  Alpine:                        apk add jq" >&2
  echo "  macOS (Homebrew):              brew install jq" >&2
  exit 1
fi

ROLE="${1:-}"
DEST="${2:-.}"

# curl is preferred (matches the release workflow's own tooling
# assumptions); wget is a fallback for a minimal image that happens to
# have it but not curl.
if command -v curl >/dev/null 2>&1; then
  http_get() { curl -fsSL "${1}"; }
elif command -v wget >/dev/null 2>&1; then
  http_get() { wget -qO- "${1}"; }
else
  echo "Neither curl nor wget is available -- install one and re-run." >&2
  exit 1
fi

# The tag written into .env as SKYFOLLOWER_VERSION. Only a release tag is
# also an image tag (every image is published as both :{tag} and :latest on
# release), so an explicitly-overridden REF -- REF=main, a branch, a commit
# -- can't be assumed to name an image and falls back to :latest.
IMAGE_VERSION="latest"

if [ -z "${REF:-}" ]; then
  # No jq assumption (Raspberry Pi OS Lite doesn't ship it) -- the tag_name
  # field is a simple top-level string, so a plain grep/cut pull is enough
  # without pulling in a JSON parser for one field.
  # Deliberately no `grep -m1`: it closes the pipe as soon as it finds a
  # match, before curl finishes writing the rest of the response body --
  # curl then dies of SIGPIPE/CURLE_WRITE_ERROR, and pipefail+set -e take
  # the whole script down with it even though REF would have been
  # captured correctly regardless. Letting grep read to EOF avoids that.
  REF="$(http_get "https://api.github.com/repos/BrentIO/SkyFollower/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d '"' -f 4)"
  if [ -z "$REF" ]; then
    echo "Could not determine the latest release tag -- pass REF=main or REF=<tag> explicitly." >&2
    exit 1
  fi
  IMAGE_VERSION="$REF"
  echo "Using latest release: ${REF}"
fi

RAW_BASE="https://raw.githubusercontent.com/BrentIO/SkyFollower/${REF}"
MAIN_BASE="https://raw.githubusercontent.com/BrentIO/SkyFollower/main"

usage() {
  echo "Usage: $0 <role> [dest-dir]" >&2
  echo "  role: receiver | receiver-mlat | core | management-ui | message-processor | archive" >&2
  exit 1
}

[ -n "$ROLE" ] || usage

# DATA_DIRS lists this role's bind-mount sources. Docker creates a missing
# bind-mount source itself, but as root -- an operator who is in the docker
# group without being root then can't inspect, copy, or remove the
# directory afterwards. Creating them here, unprivileged, keeps them owned
# by whoever runs the stack.
#
# PROJECT_NAME becomes COMPOSE_PROJECT_NAME in the generated .env. Left
# empty for the receiver roles only, where it's derived from the
# destination folder instead: that's what lets several receivers share a
# host, each in its own folder with its own project namespace.
case "$ROLE" in
  receiver)
    COMPOSE_FILES=(docker-compose.receiver.yaml)
    CONFIG_FILES=()
    DATA_DIRS=(data/receiver)
    PROJECT_NAME=""
    ;;
  receiver-mlat)
    # Same compose file as "receiver" -- the MLAT instance is the identical
    # image deployed a second time (on its own host, or alongside the
    # SDR-hosting instance on the same host), distinguished by the
    # RECEIVER_SOURCES it is given and by its own destination folder.
    COMPOSE_FILES=(docker-compose.receiver.yaml)
    CONFIG_FILES=()
    DATA_DIRS=(data/receiver)
    PROJECT_NAME=""
    ;;
  core)
    COMPOSE_FILES=(docker-compose.core.yaml)
    CONFIG_FILES=(config/runners/phonic_overrides.json.example config/rabbitmq/rabbitmq.conf.example config/rabbitmq/enabled_plugins.example)
    DATA_DIRS=(data/rabbitmq data/redis)
    PROJECT_NAME="skyfollower-core"
    ;;
  management-ui)
    COMPOSE_FILES=(docker-compose.management-ui.yaml)
    CONFIG_FILES=()
    DATA_DIRS=(data/management-ui)
    PROJECT_NAME="skyfollower-management-ui"
    ;;
  message-processor)
    COMPOSE_FILES=(docker-compose.message-processor.yaml)
    CONFIG_FILES=()
    # All eight, not just the one that always runs: enabling another
    # processor is meant to be a COMPOSE_PROFILES line in .env and nothing
    # else, which it stops being if its data directory has to be created
    # by hand first (or by Docker, as root).
    DATA_DIRS=(data/message-processor-1 data/message-processor-2 data/message-processor-3 data/message-processor-4 data/message-processor-5 data/message-processor-6 data/message-processor-7 data/message-processor-8)
    PROJECT_NAME="skyfollower-message-processor"
    ;;
  archive)
    COMPOSE_FILES=(docker-compose.archive.yaml)
    CONFIG_FILES=()
    DATA_DIRS=(data/archive-processor data/archive-compaction)
    PROJECT_NAME="skyfollower-archive"
    ;;
  *)
    echo "Unknown role: ${ROLE}" >&2
    usage
    ;;
esac

ALL_FILES=("${COMPOSE_FILES[@]}" ${CONFIG_FILES[@]+"${CONFIG_FILES[@]}"})

# A file missing at the resolved release tag doesn't necessarily mean the
# whole release is unusable for this role -- it means this one file is
# newer than the release (e.g. the release predates this component
# entirely). Falling back to main per-file, rather than switching the
# whole run to main on the first miss, still prefers the release-matched
# copy for every other file that does exist there.
for rel_path in "${ALL_FILES[@]}"; do
  dest_path="${DEST}/${rel_path}"
  mkdir -p "$(dirname "$dest_path")"
  echo "Fetching ${rel_path}..."
  if http_get "${RAW_BASE}/${rel_path}" > "$dest_path" 2>/dev/null; then
    continue
  fi
  if [ "$RAW_BASE" = "$MAIN_BASE" ]; then
    echo "  Not found." >&2
    exit 1
  fi
  echo "  Not in release ${REF} yet -- falling back to main for this file." >&2
  http_get "${MAIN_BASE}/${rel_path}" > "$dest_path"
done

echo
echo "Creating config files from *.example templates..."
# No-clobber: skips any real file that already exists, so a re-run into
# the same dest-dir (e.g. running this script once for "core" and again
# for "management-ui" into the same directory, or re-fetching after an
# update) can never silently overwrite values already filled in.
if [ -d "${DEST}/config" ]; then
  find "${DEST}/config" -name "*.example" -exec bash -c '
    for example; do
      target="${example%.example}"
      if [ -e "$target" ]; then
        echo "  Skipping ${target} (already exists)."
      else
        cp "$example" "$target"
        echo "  Created ${target}."
      fi
    done
  ' _ {} +
fi

echo
echo "Creating data directories..."
for data_dir in "${DATA_DIRS[@]}"; do
  mkdir -p "${DEST}/${data_dir}"
  echo "  ${DEST}/${data_dir}"
done

echo
echo "Writing .env..."
# Compose reads .env from the project directory automatically, so every
# host-specific value lives in one place a human can read and edit --
# no placeholder substitution in any tracked file.
#
# Resolved against DEST itself, not a separate flag: this assumes the
# operator brings the stack up from the same directory they downloaded
# into, which is already the workflow this script's own instructions
# describe.
ABS_ROOT="$(cd "${DEST}" && pwd)"

if [ -z "$PROJECT_NAME" ]; then
  # Derived from the destination folder, the same way Compose derives its
  # own default -- this is what lets several receivers share a host, each
  # in its own folder. Compose project names accept only lowercase
  # letters, digits, "-" and "_", and silently strip anything else rather
  # than erroring, so sanitize here and write the result into .env where
  # it can be seen and changed.
  PROJECT_NAME="$(printf '%s' "$(basename "$ABS_ROOT")" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_-' '-')"
  while [ "${PROJECT_NAME#-}" != "$PROJECT_NAME" ]; do PROJECT_NAME="${PROJECT_NAME#-}"; done
  while [ "${PROJECT_NAME%-}" != "$PROJECT_NAME" ]; do PROJECT_NAME="${PROJECT_NAME%-}"; done
  if [ -z "$PROJECT_NAME" ]; then
    echo "  Could not derive a valid project name from the destination folder (\"$(basename "$ABS_ROOT")\") -- pass a differently-named [dest-dir]." >&2
    exit 1
  fi
  # Compose always appends the service name ("receiver") when it builds a
  # container name, so a folder called "receiver" -- the common default --
  # would otherwise double up into skyfollower-receiver-receiver-1.
  case "$PROJECT_NAME" in
    receiver) PROJECT_NAME="" ;;
    *-receiver) PROJECT_NAME="${PROJECT_NAME%-receiver}" ;;
  esac
  if [ -n "$PROJECT_NAME" ]; then
    PROJECT_NAME="skyfollower-${PROJECT_NAME}"
  else
    PROJECT_NAME="skyfollower"
  fi
fi

# Every component now reads its whole configuration from environment
# variables, so each role's block below is appended to .env with example
# values and a comment naming what to replace. Values marked CHANGE_ME must
# be edited before the stack will start -- a component reports every one it
# is still missing in a single startup error rather than one per restart.
write_role_env() {
  case "$ROLE" in
    receiver|receiver-mlat)
      cat <<'ENV_EOF'

# Free-text label for this receiver, shown in Home Assistant. The receiver's
# stable identity is a UUID it generates into data/receiver/receiver_id --
# renaming here never renames its MQTT topics.
RECEIVER_NAME=CHANGE_ME

# Comma-separated host:port:source triples, one per readsb connection.
# source is one of 1090, 978, MLAT.
#   SDR host:  192.168.1.x:30002:1090,192.168.1.x:30978:978
#   MLAT-only: out.adsb.lol:1366:MLAT
RECEIVER_SOURCES=CHANGE_ME:30002:1090

RABBITMQ_HOST=CHANGE_ME
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=skyfollower
RABBITMQ_PASSWORD=CHANGE_ME

MQTT_HOST=CHANGE_ME
MQTT_PORT=1883
MQTT_USERNAME=CHANGE_ME
MQTT_PASSWORD=CHANGE_ME

# Seconds between telemetry publishes.
TELEMETRY_INTERVAL_SECONDS=30

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
      ;;
    core)
      cat <<'ENV_EOF'

# The broker container is started with these and every client authenticates
# with them -- one pair, one file, so the two can no longer drift apart.
RABBITMQ_USERNAME=skyfollower
RABBITMQ_PASSWORD=CHANGE_ME

# Redis as the runners on this host reach it: the compose service name,
# since they share this project's network.
REDIS_HOST=redis
REDIS_PORT=6379

# TTL applied to the enrichment keys the runners write.
REDIS_TTL_DAYS=14

MQTT_HOST=CHANGE_ME
MQTT_PORT=1883
MQTT_USERNAME=CHANGE_ME
MQTT_PASSWORD=CHANGE_ME

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
      ;;
    management-ui)
      cat <<'ENV_EOF'

REDIS_HOST=CHANGE_ME
REDIS_PORT=6379

# The archive bucket, read for flight objects and queried through Athena.
S3_BUCKET=CHANGE_ME
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=CHANGE_ME
AWS_SECRET_ACCESS_KEY=CHANGE_ME

# Athena workgroup plus the Glue database/table holding the Parquet index.
ATHENA_WORKGROUP=skyfollower
ATHENA_DATABASE=skyfollower
ATHENA_TABLE=archive_flights

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
      ;;
    message-processor)
      cat <<ENV_EOF

# Prefix for every processor ID on this node -- each instance appends its
# own number. Defaulted to this node's hostname so two nodes never collide
# on an ID without anyone having to coordinate the numbering.
MESSAGE_PROCESSOR_PREFIX=${MESSAGE_PROCESSOR_PREFIX}

# message-processor-1 always runs. To run more on this node, list the extra
# instances' profiles here -- nothing else changes:
#   COMPOSE_PROFILES=mp-2,mp-3      # three processors on this node
COMPOSE_PROFILES=

# Receiver's reference position, used to decode locally-referenced CPR
# positions. Decimal degrees.
LATITUDE=CHANGE_ME
LONGITUDE=CHANGE_ME

RABBITMQ_HOST=CHANGE_ME
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=skyfollower
RABBITMQ_PASSWORD=CHANGE_ME

REDIS_HOST=CHANGE_ME
REDIS_PORT=6379

MQTT_HOST=CHANGE_ME
MQTT_PORT=1883
MQTT_USERNAME=CHANGE_ME
MQTT_PASSWORD=CHANGE_ME

# How far behind live a message may be and still raise a rule notification.
RULE_NOTIFICATION_MAX_LAG_SECONDS=30

# Seconds between telemetry publishes.
TELEMETRY_INTERVAL_SECONDS=30

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
      ;;
    archive)
      cat <<'ENV_EOF'

# The archive bucket. AWS_* are boto3's own variable names: no credentials
# are passed in code, so an instance role can replace the key pair later
# by leaving these unset.
S3_BUCKET=CHANGE_ME
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=CHANGE_ME
AWS_SECRET_ACCESS_KEY=CHANGE_ME

RABBITMQ_HOST=CHANGE_ME
RABBITMQ_PORT=5672
RABBITMQ_USERNAME=skyfollower
RABBITMQ_PASSWORD=CHANGE_ME

REDIS_HOST=CHANGE_ME
REDIS_PORT=6379

MQTT_HOST=CHANGE_ME
MQTT_PORT=1883
MQTT_USERNAME=CHANGE_ME
MQTT_PASSWORD=CHANGE_ME

# Seconds between telemetry publishes.
TELEMETRY_INTERVAL_SECONDS=30

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
      ;;
  esac
}

# Derived the same way COMPOSE_PROJECT_NAME is derived from the destination
# folder above: sanitized to what a container-visible identifier can hold,
# and written into .env where it can be seen and changed.
if [ "$ROLE" = "message-processor" ]; then
  RAW_HOSTNAME="$(hostname -s 2>/dev/null || hostname 2>/dev/null || uname -n)"
  MESSAGE_PROCESSOR_PREFIX="$(printf '%s' "$RAW_HOSTNAME" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_-' '-')"
  while [ "${MESSAGE_PROCESSOR_PREFIX#-}" != "$MESSAGE_PROCESSOR_PREFIX" ]; do MESSAGE_PROCESSOR_PREFIX="${MESSAGE_PROCESSOR_PREFIX#-}"; done
  while [ "${MESSAGE_PROCESSOR_PREFIX%-}" != "$MESSAGE_PROCESSOR_PREFIX" ]; do MESSAGE_PROCESSOR_PREFIX="${MESSAGE_PROCESSOR_PREFIX%-}"; done
  if [ -z "$MESSAGE_PROCESSOR_PREFIX" ]; then
    MESSAGE_PROCESSOR_PREFIX="mp"
  fi
fi

ENV_FILE="${DEST}/.env"
if [ -e "$ENV_FILE" ]; then
  echo "  Skipping ${ENV_FILE} (already exists)."
else
  # umask, not a chmod afterwards: the file must never exist world-readable,
  # not even for the moment between creating and tightening it.
  (
    umask 077
    cat > "$ENV_FILE" <<ENV_EOF
# Host-specific values for this SkyFollower deployment, read automatically
# by docker compose from this directory. Written once by
# scripts/download-host-files.sh; edit and re-run \`docker compose up -d\`
# to change any of them.

# Tag every ghcr.io/brentio/skyfollower-* image resolves to. Set this to an
# older release tag and re-run \`docker compose up -d\` to roll back.
SKYFOLLOWER_VERSION=${IMAGE_VERSION}

# Compose project name -- the namespace every container and network on this
# host is named under.
COMPOSE_PROJECT_NAME=${PROJECT_NAME}

# Which compose file \`docker compose\` acts on, so no -f flag is needed.
COMPOSE_FILE=${COMPOSE_FILES[0]}

# Absolute path to this directory. Ofelia's scheduled jobs create their
# containers through the Docker Engine API, which accepts only an absolute
# host path as a bind-mount source -- it has no project directory to
# resolve a relative path against.
SKYFOLLOWER_ROOT=${ABS_ROOT}
ENV_EOF
    write_role_env >> "$ENV_FILE"
  )
  echo "  Created ${ENV_FILE}."
fi

echo
echo "Done. Downloaded to ${DEST}:"
for rel_path in "${ALL_FILES[@]}"; do
  echo "  ${DEST}/${rel_path}"
done
echo
echo "Next: replace every CHANGE_ME in ${DEST}/.env, then bring the host up"
echo "from ${DEST}:"
echo "  docker compose up -d"
