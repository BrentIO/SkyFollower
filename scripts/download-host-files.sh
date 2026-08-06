#!/usr/bin/env bash
#
# Downloads only the docker-compose.*.yaml file and config/*/*.example
# file(s) a given host role needs, without a full `git clone` of the
# monorepo. Mirrors the host/file mapping documented in
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
# comment further down -- that stays grep/sed on purpose), but it's a
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

if [ -z "${REF:-}" ]; then
  # No jq assumption (Raspberry Pi OS Lite doesn't ship it) -- the tag_name
  # field is a simple top-level string, so a plain grep/sed pull is enough
  # without pulling in a JSON parser for one field.
  # Deliberately no `grep -m1`: it closes the pipe as soon as it finds a
  # match, before curl finishes writing the rest of the response body --
  # curl then dies of SIGPIPE/CURLE_WRITE_ERROR, and pipefail+set -e take
  # the whole script down with it even though REF would have been
  # captured correctly regardless. Letting grep read to EOF avoids that.
  REF="$(http_get "https://api.github.com/repos/BrentIO/SkyFollower/releases/latest" \
    | grep '"tag_name"' | head -1 | sed -E 's/.*"tag_name": *"([^"]+)".*/\1/')"
  if [ -z "$REF" ]; then
    echo "Could not determine the latest release tag -- pass REF=main or REF=<tag> explicitly." >&2
    exit 1
  fi
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

case "$ROLE" in
  receiver)
    COMPOSE_FILES=(docker-compose.receiver.yaml)
    CONFIG_FILES=(config/receiver/settings.json.example)
    ;;
  receiver-mlat)
    # Same compose file as "receiver" -- the MLAT instance is the identical
    # image deployed a second time (on its own host, or alongside the
    # SDR-hosting instance on the same host -- see the __INSTANCE_SUFFIX__
    # resolution below), distinguished by which settings.json.example it
    # starts from and by its own destination folder.
    COMPOSE_FILES=(docker-compose.receiver.yaml)
    CONFIG_FILES=(config/receiver/mlat-settings.json.example)
    ;;
  core)
    COMPOSE_FILES=(docker-compose.core.yaml)
    CONFIG_FILES=(config/runners/settings.json.example config/runners/phonic_overrides.json.example config/ofelia/config.ini.example config/rabbitmq/rabbitmq.conf.example config/rabbitmq/enabled_plugins.example)
    ;;
  management-ui)
    COMPOSE_FILES=(docker-compose.management-ui.yaml)
    CONFIG_FILES=(config/management-ui/settings.json.example)
    ;;
  message-processor)
    COMPOSE_FILES=(docker-compose.message-processor.yaml)
    CONFIG_FILES=(config/message-processor/settings.json.example)
    ;;
  archive)
    COMPOSE_FILES=(docker-compose.archive.yaml)
    CONFIG_FILES=(config/archive/settings.json.example config/archive/compaction-settings.json.example config/archive/ofelia-config.ini.example)
    ;;
  *)
    echo "Unknown role: ${ROLE}" >&2
    usage
    ;;
esac

ALL_FILES=("${COMPOSE_FILES[@]}" "${CONFIG_FILES[@]}")

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
echo "Resolving __ROOT_DIRECTORY__ placeholders..."
# Ofelia's job-run calls the Docker Engine API directly to create each
# job's container and, unlike docker compose, never resolves a relative
# volume path against a working directory -- only an absolute host path
# (or a named volume) is a valid bind-mount source. Only the core and
# archive roles' ofelia config ships this placeholder; the loop below is
# a no-op for every other role since neither file exists yet.
# Resolved against DEST itself, not a separate flag -- this assumes the
# operator brings the stack up from the same directory they downloaded
# into, which is already the workflow this script's own instructions
# describe.
ABS_ROOT="$(cd "${DEST}" && pwd)"
for ofelia_ini in "${DEST}/config/ofelia/config.ini" "${DEST}/config/archive/ofelia-config.ini"; do
  [ -f "$ofelia_ini" ] || continue
  sed -i.bak "s#__ROOT_DIRECTORY__#${ABS_ROOT}#g" "$ofelia_ini"
  rm -f "${ofelia_ini}.bak"
  echo "  Resolved in ${ofelia_ini}."
done

echo
echo "Resolving __INSTANCE_SUFFIX__ placeholders..."
# docker-compose.receiver.yaml's project name (the "name:" line) is
# derived from the destination folder's own name, not hardcoded -- this
# is what lets more than one receiver run on the same host: deploy each
# into its own folder (e.g. "receiver", "mlat-adsb.lol") and each gets an
# independent Compose project namespace (independent container name,
# independent volume) instead of colliding on a fixed shared name.
# Docker Compose project names only accept lowercase letters, digits, -,
# and _ -- silently stripping anything else rather than erroring -- so
# the folder name is sanitized here instead of leaving that to chance.
# Only the receiver and receiver-mlat roles ship this placeholder; the
# check below is a no-op for every other role since the file doesn't
# exist there.
receiver_compose="${DEST}/docker-compose.receiver.yaml"
if [ -f "$receiver_compose" ]; then
  RAW_INSTANCE_NAME="$(basename "$ABS_ROOT")"
  INSTANCE_NAME="$(printf '%s' "$RAW_INSTANCE_NAME" | tr 'A-Z' 'a-z' | sed -E 's/[^a-z0-9_-]+/-/g; s/^-+//; s/-+$//')"
  if [ -z "$INSTANCE_NAME" ]; then
    echo "  Could not derive a valid instance name from the destination folder (\"${RAW_INSTANCE_NAME}\") -- pass a differently-named [dest-dir]." >&2
    exit 1
  fi
  # Compose always appends the service name ("receiver") itself when it
  # builds the final container/volume name (project-service-index) -- if
  # the instance name is exactly "receiver" or ends with "-receiver",
  # also putting it in the project name would double it up (folder
  # "receiver" would otherwise produce skyfollower-receiver-receiver-1).
  # Stripping that redundant trailing segment here, before it ever
  # reaches sed, means every instance name -- including the common
  # default of just calling the folder "receiver" -- produces a clean
  # container name.
  case "$INSTANCE_NAME" in
    receiver) INSTANCE_NAME="" ;;
    *-receiver) INSTANCE_NAME="${INSTANCE_NAME%-receiver}" ;;
  esac
  if [ -z "$INSTANCE_NAME" ]; then
    INSTANCE_SUFFIX=""
  else
    INSTANCE_SUFFIX="-${INSTANCE_NAME}"
  fi
  sed -i.bak "s#__INSTANCE_SUFFIX__#${INSTANCE_SUFFIX}#g" "$receiver_compose"
  rm -f "${receiver_compose}.bak"
  echo "  Resolved in ${receiver_compose} (project name: skyfollower${INSTANCE_SUFFIX})."
fi

echo
echo "Done. Downloaded to ${DEST}:"
for rel_path in "${ALL_FILES[@]}"; do
  echo "  ${DEST}/${rel_path}"
done
echo
echo "Next: fill in real values in each config file above, then bring the"
echo "host up:"
for rel_path in "${COMPOSE_FILES[@]}"; do
  echo "  docker compose -f ${rel_path} up -d"
done
