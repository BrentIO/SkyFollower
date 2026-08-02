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
    COMPOSE_FILES=(docker-compose.receiver-mlat.yaml)
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
echo "Done. Downloaded to ${DEST}:"
for rel_path in "${ALL_FILES[@]}"; do
  echo "  ${DEST}/${rel_path}"
done
echo
echo "Next: copy each config/*/*.example file to the same path without the"
echo ".example suffix, fill in real values, then bring the host up:"
for rel_path in "${COMPOSE_FILES[@]}"; do
  echo "  docker compose -f ${rel_path} up -d"
done
