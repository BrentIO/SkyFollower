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
# <role> is one of: receiver, receiver-mlat, server, management-ui,
# message-processor, archive
#
# Files are fetched from the `main` branch by default (REF env var
# overrides). Deliberately not a release tag: every docker-compose.*.yaml
# already pins its image: to :latest, not a release, so fetching config
# from main keeps this script's output on the same "latest" cadence as
# the images it's meant to run alongside -- pinning this to an older tag
# while the images keep moving would risk a config schema mismatch.
#
# Only fetches the two file kinds documented above -- this is not a
# preflight check for Docker/Compose being installed, and it never runs
# `docker compose` itself; that's still a manual step, same as the
# git-clone path.

set -euo pipefail

ROLE="${1:-}"
DEST="${2:-.}"
REF="${REF:-main}"
RAW_BASE="https://raw.githubusercontent.com/BrentIO/SkyFollower/${REF}"

usage() {
  echo "Usage: $0 <role> [dest-dir]" >&2
  echo "  role: receiver | receiver-mlat | server | management-ui | message-processor | archive" >&2
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
  server)
    COMPOSE_FILES=(docker-compose.server.yaml)
    CONFIG_FILES=(config/runners/settings.json.example config/ofelia/config.ini.example)
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
    CONFIG_FILES=(config/archive/settings.json.example config/archive/compaction-settings.json.example)
    ;;
  *)
    echo "Unknown role: ${ROLE}" >&2
    usage
    ;;
esac

# curl is preferred (matches the release workflow's own tooling
# assumptions); wget is a fallback for a minimal image that happens to
# have it but not curl.
if command -v curl >/dev/null 2>&1; then
  fetch() { curl -fsSL "${RAW_BASE}/${1}" -o "${2}"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q "${RAW_BASE}/${1}" -O "${2}"; }
else
  echo "Neither curl nor wget is available -- install one and re-run." >&2
  exit 1
fi

ALL_FILES=("${COMPOSE_FILES[@]}" "${CONFIG_FILES[@]}")

for rel_path in "${ALL_FILES[@]}"; do
  dest_path="${DEST}/${rel_path}"
  mkdir -p "$(dirname "$dest_path")"
  echo "Fetching ${rel_path}..."
  fetch "$rel_path" "$dest_path"
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
