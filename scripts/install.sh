#!/usr/bin/env bash
#
# Interactive, non-root installer for SkyFollower. Replaces
# download-host-files.sh: that script fetched files and stopped, leaving
# every remaining step -- filling in credentials, starting the stack,
# seeding Redis -- to an operator following documentation. This script
# asks for what it needs and finishes the job.
#
# Usage:
#   ./install.sh [--root <path>] [--role <role> ...] [--non-interactive] [--upgrade]
#
# Or, without cloning anything first:
#   curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash
#
# <role> is one of: receiver, core, management-ui, message-processor,
# archive -- may be repeated to select several in one
# run (e.g. --role core --role management-ui, since both live on the same
# host). Omit entirely for an interactive multi-select prompt.
#
# --non-interactive reads every value this run needs from already-exported
# environment variables (the same names written into .env -- RECEIVER_NAME,
# RABBITMQ_HOST, etc.) instead of prompting, and requires --role at least
# once. Every missing required value is reported together as one error, not
# one per restart.
#
# --upgrade re-resolves the latest release tag and runs `docker compose
# pull && up -d` in every role directory found under the install root,
# rewriting SKYFOLLOWER_VERSION in each. No prompting. Replaces
# `docker images | xargs -L1 docker pull`, which pulled every image on the
# host (unrelated ones included) and could not be rolled back.
#
# Files are fetched from the latest GitHub *release tag* by default (REF
# env var overrides, e.g. REF=main for bleeding-edge). Every image is only
# ever built and published on a release tag, so a docker-compose.*.yaml's
# image: :latest always corresponds to the most recent release commit, not
# tip of main -- fetching config from main by default would risk
# downloading a newer config/compose shape than the :latest image
# understands. Any individual file missing at the resolved tag (a release
# cut before a given role existed) falls back to main for that one file,
# with a printed warning.

set -euo pipefail

# ---------------------------------------------------------------------------
# Never escalate. No step in this script may invoke sudo -- a root
# precondition is detected, reported with the exact command to run, and the
# script exits so it can be re-run once that's done.
# ---------------------------------------------------------------------------

SCRIPT_NAME="$(basename "$0")"
NON_INTERACTIVE=0
UPGRADE=0
INSTALL_ROOT="$PWD"
ROOT_EXPLICIT=0
SELECTED_ROLES=()

ALL_ROLES="core management-ui archive message-processor receiver"

# usage()'s exit code depends on why it's being shown: 0 for an explicit
# --help request (informational, not an error), 1 for anything else
# (an unknown/malformed argument) -- callers pass the code they want.
usage() {
  local code="${1:-1}"
  cat >&2 <<USAGE
Usage: $SCRIPT_NAME [--root <path>] [--role <role> ...] [--non-interactive] [--upgrade]
  role: receiver | core | management-ui | message-processor | archive
USAGE
  exit "$code"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --root)
      INSTALL_ROOT="${2:?--root requires a path}"
      ROOT_EXPLICIT=1
      shift 2
      ;;
    --role)
      SELECTED_ROLES+=("${2:?--role requires a role name}")
      shift 2
      ;;
    --non-interactive)
      NON_INTERACTIVE=1
      shift
      ;;
    --upgrade)
      UPGRADE=1
      shift
      ;;
    -h|--help)
      usage 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage 1
      ;;
  esac
done

# curl is preferred (matches the release workflow's own tooling
# assumptions); wget is a fallback for a minimal image that happens to have
# it but not curl.
if command -v curl >/dev/null 2>&1; then
  http_get() { curl -fsSL "${1}"; }
elif command -v wget >/dev/null 2>&1; then
  http_get() { wget -qO- "${1}"; }
else
  echo "Neither curl nor wget is available -- install one and re-run." >&2
  exit 1
fi

resolve_ref() {
  # The tag written into .env as SKYFOLLOWER_VERSION. Only a release tag is
  # also an image tag (every image publishes as both :{tag} and :latest on
  # release), so an explicitly-overridden REF -- REF=main, a branch, a
  # commit -- can't be assumed to name an image and falls back to :latest.
  if [ -n "${REF:-}" ]; then
    IMAGE_VERSION="latest"
    return
  fi
  # No jq assumption (Raspberry Pi OS Lite doesn't ship it) -- the
  # tag_name field is a simple top-level string, so a plain grep/cut pull
  # is enough without pulling in a JSON parser for one field.
  # Deliberately no `grep -m1`: it closes the pipe as soon as it finds a
  # match, before curl finishes writing the rest of the response body --
  # curl then dies of SIGPIPE/CURLE_WRITE_ERROR, and pipefail+set -e take
  # the whole script down with it even though REF would have been
  # captured correctly regardless. Letting grep read to EOF avoids that.
  # `|| true` guards the other pipefail trap: if the API response ever
  # doesn't contain a tag_name field at all, grep's own no-match failure
  # would otherwise take the whole script down right here under set -e,
  # never reaching the friendly "could not determine" message below.
  REF="$(http_get "https://api.github.com/repos/BrentIO/SkyFollower/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d '"' -f 4 || true)"
  if [ -z "$REF" ]; then
    echo "Could not determine the latest release tag -- pass REF=main or REF=<tag> explicitly." >&2
    exit 1
  fi
  IMAGE_VERSION="$REF"
  echo "Using latest release: ${REF}"
}

# ---------------------------------------------------------------------------
# Preflight -- before any prompting, so a host that cannot possibly work
# fails in the first two seconds instead of after twenty minutes of
# questions.
# ---------------------------------------------------------------------------

preflight() {
  echo "Checking prerequisites..."
  local failed=0

  if ! command -v docker >/dev/null 2>&1; then
    echo "  ✗ docker is not on PATH." >&2
    echo "    Install Docker first: https://docs.docker.com/engine/install/" >&2
    failed=1
  else
    echo "  ✓ docker found"
  fi

  if [ "$failed" -eq 0 ]; then
    if ! docker info >/dev/null 2>&1; then
      echo "  ✗ 'docker info' failed for the current user ($(whoami))." >&2
      echo "    Run this yourself, then LOG OUT AND BACK IN (the group change" >&2
      echo "    does not apply to the current session) and re-run this script:" >&2
      echo "      sudo usermod -aG docker \$USER" >&2
      failed=1
    else
      echo "  ✓ docker reachable as $(whoami), no sudo needed"
    fi
  fi

  if command -v docker >/dev/null 2>&1; then
    # A successful `docker compose ...` invocation at all -- as a `docker`
    # subcommand, not a separate `docker-compose` binary -- is itself the
    # signal that the modern Compose plugin is installed; the legacy v1
    # standalone tool never registers as a docker subcommand in the first
    # place. Not pattern-matching the version's leading digit: Compose's
    # own release numbering advances independently of that v1/v2
    # distinction, so a hardcoded "must start with 2" check goes stale as
    # soon as Compose ships a 3.x/4.x/5.x release, even though every one
    # of those is still the plugin this check exists to require.
    local compose_version
    if compose_version="$(docker compose version --short 2>/dev/null)" && [ -n "$compose_version" ]; then
      echo "  ✓ docker compose v${compose_version} (Compose plugin)"
    else
      echo "  ✗ docker compose (the plugin, not the legacy standalone docker-compose) not found." >&2
      echo "    Install it: https://docs.docker.com/compose/install/" >&2
      failed=1
    fi
  fi

  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    echo "  ✗ Neither curl nor wget is available." >&2
    echo "    Install one: sudo apt-get install -y curl" >&2
    failed=1
  else
    echo "  ✓ $(command -v curl >/dev/null 2>&1 && echo curl || echo wget) found"
  fi

  # The install root itself, or its nearest existing ancestor, must be
  # writable by the current user -- Docker will happily create a missing
  # bind-mount source itself, but as root, which then can't be inspected,
  # copied, or removed by an operator who is merely in the docker group.
  local check_dir="$INSTALL_ROOT"
  while [ ! -e "$check_dir" ]; do
    check_dir="$(dirname "$check_dir")"
  done
  if [ ! -w "$check_dir" ]; then
    echo "  ✗ ${INSTALL_ROOT} is not writable (nearest existing ancestor: ${check_dir})." >&2
    echo "    Run this, then re-run this script:" >&2
    echo "      sudo mkdir -p ${INSTALL_ROOT}" >&2
    echo "      sudo chown \$USER:\$(id -gn) ${INSTALL_ROOT}" >&2
    failed=1
  else
    echo "  ✓ ${INSTALL_ROOT} is writable (or can be created)"
  fi

  if [ "$failed" -ne 0 ]; then
    echo >&2
    echo "Fix the above, then re-run this script. Nothing has been changed." >&2
    exit 1
  fi
  echo
}

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

sanitize_identifier() {
  # Lowercased, anything outside [a-z0-9_-] replaced with '-', leading and
  # trailing '-' trimmed. Matches Docker Compose's own project-name
  # character rules instead of relying on Compose silently stripping
  # anything else.
  local out
  out="$(printf '%s' "$1" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_-' '-')"
  while [ "${out#-}" != "$out" ]; do out="${out#-}"; done
  while [ "${out%-}" != "$out" ]; do out="${out%-}"; done
  printf '%s' "$out"
}

existing_env_value() {
  # Prints the last KEY=... line's value from an existing .env, or nothing
  # if the file or key doesn't exist -- used to offer current values as
  # defaults on a re-run, per component convention (last-write-wins for a
  # repeated key, matching how a human editing the file would expect it
  # to behave).
  local file="$1" key="$2"
  [ -f "$file" ] || return 0
  # || true: the key legitimately not being present yet (e.g. re-running
  # against a .env from before this key existed) makes grep itself exit
  # non-zero on zero matches -- since this is the function's last command,
  # that becomes this function's own return status, which would otherwise
  # take the whole script down under set -e the moment a caller does
  # x="$(existing_env_value ...)", not just fail to find a default.
  grep -E "^${key}=" "$file" 2>/dev/null | tail -1 | cut -d= -f2- || true
}

existing_env_value_or() {
  # existing_env_value(...) always exits 0 now (see its own comment above),
  # so the "existing_env_value ... || echo DEFAULT" idiom this replaces
  # never actually falls back to DEFAULT -- that only ever triggers on a
  # non-zero exit status, not on empty output. This checks the printed
  # value itself instead.
  local file="$1" key="$2" default="$3" val
  val="$(existing_env_value "$file" "$key")"
  if [ -n "$val" ]; then
    printf '%s' "$val"
  else
    printf '%s' "$default"
  fi
}

default_receiver_name() {
  # Suggested RECEIVER_NAME default on a fresh install: the machine's short
  # hostname, uppercased. `hostname -s` works on both this script's
  # realistic host platforms (Linux, macOS); the fallback strips everything
  # after the first '.' from plain `hostname` output in case `-s` isn't
  # available. `tr` rather than bash 4+ `${h^^}` -- this script has already
  # hit a real macOS-default-bash-3.2 compatibility bug once (#1031).
  local h
  h="$(hostname -s 2>/dev/null)"
  if [ -z "$h" ]; then
    h="$(hostname 2>/dev/null)"
    h="${h%%.*}"
  fi
  printf '%s' "$h" | tr 'a-z' 'A-Z'
}

generate_password() {
  if command -v openssl >/dev/null 2>&1; then
    openssl rand -base64 32 | tr -dc 'A-Za-z0-9' | head -c 32
  else
    tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 32
  fi
}

# All prompt_* helpers are no-ops in --non-interactive mode: they read the
# named environment variable instead of calling `read`, and record a
# problem (rather than exiting immediately) if it's required and unset --
# so a missing .env in that mode is reported as one list, matching the
# shared config loader's own "every problem at once" behaviour rather than
# failing on the first one.
#
# A file, not a bash array: every prompt_* function is invoked as
# X="$(prompt_string ...)" so its printed value can be captured, and
# command substitution always forks a subshell -- an array append made
# inside that subshell (NONINTERACTIVE_PROBLEMS+=(...)) would vanish the
# instant the subshell exits, silently discarding every recorded problem
# before this shell ever saw them. A file is real filesystem I/O, so it
# survives the subshell boundary that in-memory shell state cannot cross.
PROBLEMS_FILE="$(mktemp)"
trap 'rm -f "$PROBLEMS_FILE"' EXIT
record_problem() {
  echo "$1" >> "$PROBLEMS_FILE"
}

prompt_string() {
  local varname="$1" label="$2" default="$3" required="${4:-1}"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${!varname:-$default}"
    if [ "$required" -eq 1 ] && [ -z "$val" ]; then
      record_problem "$varname is required but is not set"
    fi
    printf '%s' "$val"
    return
  fi
  local input
  if [ -n "$default" ]; then
    read -r -p "  ${label} [${default}]: " input </dev/tty
    printf '%s' "${input:-$default}"
  else
    while true; do
      read -r -p "  ${label}: " input </dev/tty
      if [ -n "$input" ] || [ "$required" -eq 0 ]; then
        printf '%s' "$input"
        return
      fi
      echo "    Required." >&2
    done
  fi
}

prompt_password_value() {
  # required defaults to 1, but MQTT_PASSWORD passes 0: MQTT genuinely
  # supports an anonymous connection (both username and password blank),
  # so it must be possible to accept an empty value here on a first run,
  # not just on a re-run where an existing (also-blank) default exists.
  local varname="$1" label="$2" default="$3" required="${4:-1}"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${!varname:-$default}"
    if [ "$required" -eq 1 ] && [ -z "$val" ]; then
      record_problem "$varname is required but is not set"
    fi
    printf '%s' "$val"
    return
  fi
  local input
  if [ -n "$default" ]; then
    read -r -s -p "  ${label} [leave blank to keep existing]: " input </dev/tty
    echo >&2
    printf '%s' "${input:-$default}"
  elif [ "$required" -eq 0 ]; then
    read -r -s -p "  ${label} [blank for anonymous]: " input </dev/tty
    echo >&2
    printf '%s' "$input"
  else
    while true; do
      read -r -s -p "  ${label}: " input </dev/tty
      echo >&2
      if [ -n "$input" ]; then
        printf '%s' "$input"
        return
      fi
      echo "    Required." >&2
    done
  fi
}

prompt_int_range() {
  local varname="$1" label="$2" default="$3" min="$4" max="$5"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${!varname:-$default}"
    if [ -z "$val" ]; then
      record_problem "$varname is required but is not set"
    elif ! [[ "$val" =~ ^-?[0-9]+$ ]] || [ "$val" -lt "$min" ] || [ "$val" -gt "$max" ]; then
      record_problem "$varname must be a whole number between $min and $max (got '$val')"
    fi
    printf '%s' "$val"
    return
  fi
  local input
  while true; do
    read -r -p "  ${label} [${default}]: " input </dev/tty
    input="${input:-$default}"
    if [[ "$input" =~ ^-?[0-9]+$ ]] && [ "$input" -ge "$min" ] && [ "$input" -le "$max" ]; then
      printf '%s' "$input"
      return
    fi
    echo "    Must be a whole number between $min and $max." >&2
  done
}

prompt_number_range() {
  # Latitude/longitude -- decimal, not integer, so validated with a regex
  # rather than shell integer comparison.
  local varname="$1" label="$2" default="$3" min="$4" max="$5"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${!varname:-$default}"
    if [ -z "$val" ]; then
      record_problem "$varname is required but is not set"
    elif ! python3 -c "import sys; v=float('$val'); sys.exit(0 if $min<=v<=$max else 1)" 2>/dev/null; then
      record_problem "$varname must be a number between $min and $max (got '$val')"
    fi
    printf '%s' "$val"
    return
  fi
  local input
  while true; do
    read -r -p "  ${label} [${default:-required}]: " input </dev/tty
    input="${input:-$default}"
    if [ -n "$input" ] && python3 -c "import sys; v=float('$input'); sys.exit(0 if $min<=v<=$max else 1)" 2>/dev/null; then
      printf '%s' "$input"
      return
    fi
    echo "    Must be a number between $min and $max." >&2
  done
}

validate_receiver_sources() {
  # Mirrors shared/config.py's parse_receiver_sources -- kept in sync by
  # hand since this is bash validating input before it ever reaches that
  # Python parser, not a substitute for it.
  local raw="$1" triple host port tag
  IFS=',' read -ra triples <<< "$raw"
  [ "${#triples[@]}" -eq 0 ] && return 1
  for triple in "${triples[@]}"; do
    IFS=':' read -r host port tag <<< "$triple"
    [ -z "$host" ] && return 1
    [[ "$port" =~ ^[0-9]+$ ]] || return 1
    [ "$port" -ge 1 ] && [ "$port" -le 65535 ] || return 1
    # tr, not ${tag^^} -- the latter is a bash 4+ feature and this script
    # otherwise only relies on bash 3.2+ syntax.
    case "$(printf '%s' "$tag" | tr 'a-z' 'A-Z')" in
      1090|978|EXTERNAL) ;;
      *) return 1 ;;
    esac
  done
  return 0
}

prompt_receiver_sources() {
  local varname="RECEIVER_SOURCES" default="$1"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${RECEIVER_SOURCES:-$default}"
    if [ -z "$val" ]; then
      record_problem "RECEIVER_SOURCES is required but is not set"
    elif ! validate_receiver_sources "$val"; then
      record_problem "RECEIVER_SOURCES entries must be host:port:source triples (source is 1090, 978, or EXTERNAL)"
    fi
    printf '%s' "$val"
    return
  fi
  echo "  Comma-separated host:port:source triples, one per readsb connection." >&2
  echo "  source is one of 1090, 978, EXTERNAL. Example:" >&2
  echo "    192.168.1.x:30002:1090,192.168.1.x:30978:978" >&2
  local input
  while true; do
    read -r -p "  RECEIVER_SOURCES${default:+ [$default]}: " input </dev/tty
    input="${input:-$default}"
    if validate_receiver_sources "$input"; then
      printf '%s' "$input"
      return
    fi
    echo "    Each entry must be host:port:source (source: 1090, 978, or EXTERNAL)." >&2
  done
}

probe_tcp() {
  # No dependency on GNU coreutils' `timeout` (absent by default on
  # macOS, and not guaranteed on every minimal Linux image either): a
  # background watchdog kills the connection attempt after 3s if it's
  # still running, and the blocking `wait` below picks up its real exit
  # status either way -- 0 for an actual successful connect, non-zero for
  # both a genuine refusal and a watchdog-forced kill. Polling with
  # `kill -0` in a sleep loop was tried first and rejected: a background
  # job that has already exited but not yet been reaped can still answer
  # `kill -0` as "alive" on some systems, which would kill (and then
  # misreport as a timeout) a connection that had actually already
  # succeeded.
  local host="$1" port="$2" label="$3"
  [ -z "$host" ] && return 0
  ( exec 3<>"/dev/tcp/${host}/${port}" ) 2>/dev/null &
  local pid=$!
  ( sleep 3; kill -9 "$pid" 2>/dev/null ) &
  local watchdog_pid=$!
  local status
  if wait "$pid" 2>/dev/null; then
    status=0
  else
    status=1
  fi
  # Both of these routinely "fail" (the watchdog already fired and exited
  # on its own) and that's fine -- explicitly not checked, since under
  # set -e an unchecked bare failure here would silently kill the whole
  # script instead of just this cleanup step.
  kill "$watchdog_pid" 2>/dev/null || true
  wait "$watchdog_pid" 2>/dev/null || true
  if [ "$status" -eq 0 ]; then
    echo "  ✓ ${label} (${host}:${port}) reachable"
  else
    echo "  ⚠ ${label} (${host}:${port}) not reachable right now -- continuing anyway" >&2
    echo "    (expected if that host isn't deployed yet)" >&2
  fi
}

# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

role_files() {
  # Echoes "compose_file config_file..." (space-separated) for a role,
  # mirroring the mapping documented in docs/deployment/index.md's Compose
  # Files and Configuration tables -- update both places together if it
  # ever changes.
  case "$1" in
    receiver)
      echo "docker-compose.receiver.yaml"
      ;;
    core)
      echo "docker-compose.core.yaml config/runners/phonic_overrides.json.example config/rabbitmq/rabbitmq.conf.example config/rabbitmq/enabled_plugins.example"
      ;;
    management-ui)
      echo "docker-compose.management-ui.yaml"
      ;;
    message-processor)
      echo "docker-compose.message-processor.yaml"
      ;;
    archive)
      echo "docker-compose.archive.yaml"
      ;;
  esac
}

role_data_dirs() {
  case "$1" in
    receiver)
      echo "data/receiver"
      ;;
    core)
      echo "data/rabbitmq data/redis"
      ;;
    management-ui)
      echo "data/management-ui"
      ;;
    message-processor)
      # Nothing fixed to create here: which IDs this node hosts (and so
      # which data/skyfollower-message-processor-{id} directories exist)
      # isn't known until collect_message_processor_env() has run its
      # prompts, which creates each one itself as it appends that ID's
      # service block.
      ;;
    archive)
      echo "data/archive-processor data/archive-compaction"
      ;;
  esac
}

fetch_role() {
  local role="$1" role_dir="$2"
  local raw_base="https://raw.githubusercontent.com/BrentIO/SkyFollower/${REF}"
  local main_base="https://raw.githubusercontent.com/BrentIO/SkyFollower/main"

  echo "Fetching files for ${role}..."
  for rel_path in $(role_files "$role"); do
    local dest_path="${role_dir}/${rel_path}"
    mkdir -p "$(dirname "$dest_path")"
    # message-processor's compose file stops being a static fetched
    # artifact the moment collect_message_processor_env() appends this
    # node's per-ID service blocks into it -- re-fetching over it on a
    # later run would silently discard every already-running instance's
    # block. No-clobber it exactly like a config/*.example's real target
    # below: fetched fresh only the first time, left entirely alone once it
    # exists (delete it by hand to pick up template/anchor changes).
    if [ "$role" = "message-processor" ] && [ "$rel_path" = "docker-compose.message-processor.yaml" ] && [ -e "$dest_path" ]; then
      echo "  ${rel_path} (already exists -- left as-is, holds this node's generated processor list)"
      continue
    fi
    echo "  ${rel_path}"
    if http_get "${raw_base}/${rel_path}" > "$dest_path" 2>/dev/null; then
      continue
    fi
    if [ "$raw_base" = "$main_base" ]; then
      echo "    Not found." >&2
      exit 1
    fi
    echo "    Not in release ${REF} yet -- falling back to main for this file." >&2
    http_get "${main_base}/${rel_path}" > "$dest_path"
  done

  # No-clobber: skips any real file that already exists, so a re-run into
  # an existing install directory can never silently overwrite values
  # already filled in by the operator.
  if [ -d "${role_dir}/config" ]; then
    find "${role_dir}/config" -name "*.example" -exec bash -c '
      for example; do
        target="${example%.example}"
        if [ -e "$target" ]; then
          echo "    Skipping ${target} (already exists)."
        else
          cp "$example" "$target"
          echo "    Created ${target}."
        fi
      done
    ' _ {} +
  fi

  for data_dir in $(role_data_dirs "$role"); do
    mkdir -p "${role_dir}/${data_dir}"
  done
}

# ---------------------------------------------------------------------------
# Per-role .env body -- the values genuinely worth an interactive prompt.
# Everything else (LOG_LEVEL, the Athena names) has a sensible default and
# is written directly, matching "the installer asks for what it needs"
# rather than every value that could theoretically be tuned -- an operator
# who wants one of those can still just edit .env afterward. Internal timing
# values (publish cadence, key TTLs, retry backoffs) are not env vars at
# all: they are fixed constants in shared/timing.py. flight_ttl_seconds is
# the one tunable behavioural value and lives in the config:flight_ttl_seconds
# Redis key, not here.
# ---------------------------------------------------------------------------

collect_receiver_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (receiver) --"
  # RECEIVER_NAME itself was already prompted for once, up in the role
  # loop -- it doubles as the install folder name, so it has to be known
  # before this function's fetch_role/role_dir even exist. Nothing to do
  # here but write the value that's already in scope.
  RECEIVER_SOURCES="$(prompt_receiver_sources "$(existing_env_value "$env_file" RECEIVER_SOURCES)")"
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(existing_env_value "$env_file" RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(existing_env_value_or "$env_file" RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(existing_env_value_or "$env_file" RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(prompt_password_value RABBITMQ_PASSWORD "RabbitMQ password" "$(existing_env_value "$env_file" RABBITMQ_PASSWORD)")"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(existing_env_value "$env_file" MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(existing_env_value_or "$env_file" MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(existing_env_value "$env_file" MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(existing_env_value "$env_file" MQTT_PASSWORD)" 0)"
  # Optional -- leave REDIS_HOST blank to disable entirely: no
  # identity claim/heartbeat, no period-counter sensors, no core-health
  # registration, and RECEIVER_NAME stays purely cosmetic (the receiver
  # falls back to its own generated UUID identity). Same
  # optional/empty-default pattern MQTT_HOST already uses above.
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host (leave blank to disable identity claim + message counters)" "$(existing_env_value "$env_file" REDIS_HOST)" 0)"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(existing_env_value_or "$env_file" REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(prompt_password_value REDIS_PASSWORD "Redis password" "$(existing_env_value "$env_file" REDIS_PASSWORD)" 0)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# Operator-chosen name for this receiver -- also used as the install
# folder name. With REDIS_HOST set below, this is the receiver's actual
# identity (claimed via Redis SET NX on first boot, then persisted to
# data/receiver/receiver_id forever after); with REDIS_HOST unset, it's
# purely a Home Assistant display label and the receiver falls back to a
# generated UUID identity instead.
RECEIVER_NAME=${RECEIVER_NAME}

# Comma-separated host:port:source triples, one per readsb connection.
# source is one of 1090, 978, EXTERNAL.
RECEIVER_SOURCES=${RECEIVER_SOURCES}

RABBITMQ_HOST=${RABBITMQ_HOST}
RABBITMQ_PORT=${RABBITMQ_PORT}
RABBITMQ_USERNAME=${RABBITMQ_USERNAME}
RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}

MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# Optional -- leave REDIS_HOST blank to disable identity claim, message
# counters, and core-health registration entirely.
REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
REDIS_PASSWORD=${REDIS_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

# Reuses a password collect_core_env() already collected/generated earlier
# in this same run instead of a dependent role independently re-prompting
# against its own (often still-empty) .env -- see CORE_SELECTED_IN_THIS_RUN
# and where collect_core_env() stashes CORE_REDIS_PASSWORD/
# CORE_RABBITMQ_PASSWORD. Per the resolved scope, this skips the prompt
# entirely (no Enter-to-accept step) rather than merely pre-filling a
# default, since the value is already decided, not just a guess at one.
resolve_core_shared_password() {
  local core_var="$1" varname="$2" label="$3" env_file="$4"
  local core_val="${!core_var:-}"
  if [ -n "$core_val" ]; then
    printf '%s' "$core_val"
    return
  fi
  prompt_password_value "$varname" "$label" "$(existing_env_value "$env_file" "$varname")"
}

collect_core_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (core) --"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(existing_env_value_or "$env_file" RABBITMQ_USERNAME skyfollower)")"
  local existing_rmq_pw
  existing_rmq_pw="$(existing_env_value "$env_file" RABBITMQ_PASSWORD)"
  if [ "$NON_INTERACTIVE" -eq 0 ] && [ -z "$existing_rmq_pw" ]; then
    local gen
    read -r -p "  Generate a strong RabbitMQ password? [Y/n]: " gen </dev/tty
    if [ -z "$gen" ] || [[ "$gen" =~ ^[Yy] ]]; then
      RABBITMQ_PASSWORD="$(generate_password)"
      echo "  Generated (not shown -- it's written straight to .env, no reason for a human to see it)."
    else
      RABBITMQ_PASSWORD="$(prompt_password_value RABBITMQ_PASSWORD "RabbitMQ password" "")"
    fi
  else
    RABBITMQ_PASSWORD="$(prompt_password_value RABBITMQ_PASSWORD "RabbitMQ password" "$existing_rmq_pw")"
  fi
  # Stashed in a run-scoped variable so any dependent role's collect_*_env
  # processed later in this same invocation can reuse it silently -- see
  # resolve_core_shared_password() above.
  CORE_RABBITMQ_PASSWORD="$RABBITMQ_PASSWORD"
  # Fixed, not prompted -- nothing requires a human to choose the dashboard
  # admin's username any more than its password. Its credentials never
  # leave this host: no other role's .env ever references it, and no
  # component reads it, since RabbitMQ is provisioned by rabbitmqctl after
  # startup (see provision_rabbitmq_users), not by an image env var.
  RABBITMQ_ADMIN_USERNAME="$(existing_env_value_or "$env_file" RABBITMQ_ADMIN_USERNAME skyfollower-admin)"
  RABBITMQ_ADMIN_PASSWORD="$(existing_env_value "$env_file" RABBITMQ_ADMIN_PASSWORD)"
  if [ -z "$RABBITMQ_ADMIN_PASSWORD" ]; then
    RABBITMQ_ADMIN_PASSWORD="$(generate_password)"
  fi
  # core-health's own broker-wide read-only credential (RabbitMQ's built-in
  # `monitoring` tag), provisioned the same way as the two above -- fixed
  # username, generated password, never prompted, since there's no more a
  # reason for a human to choose this username than the dashboard admin's.
  RABBITMQ_MONITORING_USERNAME="$(existing_env_value_or "$env_file" RABBITMQ_MONITORING_USERNAME skyfollower-monitoring)"
  RABBITMQ_MONITORING_PASSWORD="$(existing_env_value "$env_file" RABBITMQ_MONITORING_PASSWORD)"
  if [ -z "$RABBITMQ_MONITORING_PASSWORD" ]; then
    RABBITMQ_MONITORING_PASSWORD="$(generate_password)"
  fi
  local existing_redis_pw
  existing_redis_pw="$(existing_env_value "$env_file" REDIS_PASSWORD)"
  if [ "$NON_INTERACTIVE" -eq 0 ] && [ -z "$existing_redis_pw" ]; then
    local gen_redis
    read -r -p "  Generate a strong Redis password? [Y/n]: " gen_redis </dev/tty
    if [ -z "$gen_redis" ] || [[ "$gen_redis" =~ ^[Yy] ]]; then
      REDIS_PASSWORD="$(generate_password)"
      echo "  Generated (not shown -- it's written straight to .env, no reason for a human to see it)."
    else
      REDIS_PASSWORD="$(prompt_password_value REDIS_PASSWORD "Redis password" "")"
    fi
  else
    REDIS_PASSWORD="$(prompt_password_value REDIS_PASSWORD "Redis password" "$existing_redis_pw")"
  fi
  # Stashed the same way as CORE_RABBITMQ_PASSWORD above.
  CORE_REDIS_PASSWORD="$REDIS_PASSWORD"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(existing_env_value "$env_file" MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(existing_env_value_or "$env_file" MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(existing_env_value "$env_file" MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(existing_env_value "$env_file" MQTT_PASSWORD)" 0)"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# The broker container is started with these, and the receiver, message
# processor and archive processor all authenticate with them too -- one
# pair, one file, so the two can no longer drift apart. RabbitMQ's image
# always creates this user as a full administrator on first boot; this
# script demotes it to SkyFollower's own scoped, tag-less permissions right
# after the container reports healthy (see provision_rabbitmq_users).
RABBITMQ_USERNAME=${RABBITMQ_USERNAME}
RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}

# Dashboard-only administrator, provisioned the same way. Never referenced
# by any other role's .env or read by any component -- the only way to use
# it is to log into http://<this-host>:15672 by hand.
RABBITMQ_ADMIN_USERNAME=${RABBITMQ_ADMIN_USERNAME}
RABBITMQ_ADMIN_PASSWORD=${RABBITMQ_ADMIN_PASSWORD}

# core-health's own broker-wide read-only credential (RabbitMQ's built-in
# "monitoring" tag -- see provision_rabbitmq_users), used only for polling
# the Management API on port 15672, never for AMQP.
RABBITMQ_MANAGEMENT_PORT=15672
RABBITMQ_MONITORING_USERNAME=${RABBITMQ_MONITORING_USERNAME}
RABBITMQ_MONITORING_PASSWORD=${RABBITMQ_MONITORING_PASSWORD}

# Redis as the runners on this host reach it: the compose service name,
# since they share this project's network.
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=${REDIS_PASSWORD}

# core-health authenticates with this same default-user credential for
# Redis INFO/MEMORY introspection too -- no separate scoped user. See
# core-health/README.md's Credentials section for why.

MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

collect_management_ui_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (management-ui) --"
  # If core is also selected in this run and lives in a sibling directory,
  # this defaults to reaching it via the host loopback address (Redis'
  # port is published to the host) rather than the "redis" service name,
  # which only resolves inside core's own Compose project network.
  local redis_default
  redis_default="$(existing_env_value "$env_file" REDIS_HOST)"
  if [ -z "$redis_default" ] && [ -n "${CORE_SELECTED_IN_THIS_RUN:-}" ]; then
    redis_default="localhost"
  fi
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$redis_default")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(existing_env_value_or "$env_file" REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file")"
  S3_BUCKET="$(prompt_string S3_BUCKET "S3 archive bucket name" "$(existing_env_value "$env_file" S3_BUCKET)")"
  AWS_DEFAULT_REGION="$(prompt_string AWS_DEFAULT_REGION "AWS region" "$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)")"
  AWS_ACCESS_KEY_ID="$(prompt_string AWS_ACCESS_KEY_ID "AWS access key ID" "$(existing_env_value "$env_file" AWS_ACCESS_KEY_ID)")"
  AWS_SECRET_ACCESS_KEY="$(prompt_password_value AWS_SECRET_ACCESS_KEY "AWS secret access key" "$(existing_env_value "$env_file" AWS_SECRET_ACCESS_KEY)")"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
REDIS_PASSWORD=${REDIS_PASSWORD}

# The archive bucket, read for flight objects and queried through Athena.
S3_BUCKET=${S3_BUCKET}
AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}

# Athena workgroup plus the Glue database/table holding the Parquet index.
ATHENA_WORKGROUP=skyfollower
ATHENA_DATABASE=skyfollower
ATHENA_TABLE=archive_flights

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

normalize_message_processor_id() {
  # Accepts either the full "skyfollower-message-processor-{id}" form or a
  # bare "{id}", and prints the bare id -- always what's actually stored/
  # compared, since it's what names the compose service/container, the
  # RabbitMQ queue, and the Redis heartbeat key are all built from at
  # generation time. Fails (no output, non-zero exit) on anything that
  # isn't a positive whole number once the prefix is stripped -- fleet IDs
  # start at 1, matching existing_count+1 as the first ID a fresh fleet
  # ever hands out.
  local raw="$1" id
  case "$raw" in
    skyfollower-message-processor-*)
      id="${raw#skyfollower-message-processor-}"
      ;;
    *)
      id="$raw"
      ;;
  esac
  [[ "$id" =~ ^[1-9][0-9]*$ ]] || return 1
  printf '%s' "$id"
}

existing_message_processor_ids() {
  # IDs already holding a generated service block in this node's compose
  # file, one per line -- empty (not an error) if the file doesn't exist
  # yet or has no service blocks appended. Used so a re-run only appends
  # the IDs it doesn't already find, whether that's this collection
  # function's own "how many currently implemented" arithmetic overlapping
  # a prior run, or a "replacing" ID that turns out to already be present.
  local compose_file="$1"
  [ -f "$compose_file" ] || return 0
  grep -E '^  skyfollower-message-processor-[0-9]+:' "$compose_file" 2>/dev/null \
    | sed -E 's/^  skyfollower-message-processor-([0-9]+):.*/\1/' || true
}

append_message_processor_service() {
  # Appends one concrete service block referencing this file's own
  # x-message-processor/x-message-processor-environment anchors -- YAML
  # anchors only resolve within the file that defines them, which is why
  # this can't be a second compose file merged in via COMPOSE_FILE.
  local compose_file="$1" id="$2"
  cat >> "$compose_file" <<SERVICE_EOF

  skyfollower-message-processor-${id}:
    <<: *message-processor
    container_name: skyfollower-message-processor-${id}
    volumes:
      - ./data/skyfollower-message-processor-${id}:/app/data
    environment:
      <<: *message-processor-environment
      MESSAGE_PROCESSOR_ID: ${id}
SERVICE_EOF
}

collect_message_processor_env() {
  local role_dir="$1" env_file="${1}/.env"
  local compose_file="${role_dir}/docker-compose.message-processor.yaml"
  echo "-- ${role_dir} (message-processor) --"

  local existing_ids
  existing_ids="$(existing_message_processor_ids "$compose_file")"

  local replacing=""
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    replacing="${MESSAGE_PROCESSOR_REPLACING:-n}"
  else
    read -r -p "  Are you replacing an existing message processor? [y/N]: " replacing </dev/tty
  fi

  local ids_to_add=()

  if [ -n "$replacing" ] && [[ "$replacing" =~ ^[Yy] ]]; then
    local raw_id norm_id=""
    if [ "$NON_INTERACTIVE" -eq 1 ]; then
      raw_id="${MESSAGE_PROCESSOR_REPLACE_ID:-}"
      if [ -z "$raw_id" ]; then
        record_problem "MESSAGE_PROCESSOR_REPLACE_ID is required but is not set"
      elif ! norm_id="$(normalize_message_processor_id "$raw_id")"; then
        record_problem "MESSAGE_PROCESSOR_REPLACE_ID must be skyfollower-message-processor-{id} or a bare positive whole-number id (got '${raw_id}')"
      fi
    else
      while true; do
        read -r -p "  What is the queue ID? " raw_id </dev/tty
        if norm_id="$(normalize_message_processor_id "$raw_id")"; then
          local confirm
          read -r -p "  The queue to use is skyfollower-message-processor-${norm_id} -- confirm? [Y/n]: " confirm </dev/tty
          if [ -z "$confirm" ] || [[ "$confirm" =~ ^[Yy] ]]; then
            break
          fi
          # Declined: loop back and ask for the ID again rather than
          # aborting -- this is a single value being re-entered, not the
          # multi-prompt fleet-count collection below, which is what the
          # design calls out for a full-abort-on-decline treatment.
        else
          echo "    Must be skyfollower-message-processor-{id} or a bare positive whole-number id." >&2
        fi
      done
    fi
    # Only non-interactive mode can reach here with norm_id still unset --
    # a validation failure recorded a problem above rather than exiting
    # immediately (matching every other prompt_* helper's non-interactive
    # behaviour: every problem across every selected role is collected and
    # reported together at the very end of main(), not one at a time), so
    # this must not append a malformed empty-id service block in the
    # meantime.
    [ -n "$norm_id" ] && ids_to_add=("$norm_id")
  else
    local existing_count num_new
    existing_count="$(prompt_int_range MESSAGE_PROCESSOR_EXISTING_COUNT "How many message processors are currently implemented across your whole fleet" "0" 0 100000)"
    num_new="$(prompt_int_range MESSAGE_PROCESSOR_NEW_COUNT "How many processors will be on this host" "1" 1 8)"

    local total=$(( existing_count + num_new ))
    local proceed=""
    if [ "$NON_INTERACTIVE" -eq 1 ]; then
      proceed="y"
    else
      read -r -p "  After installation, you will have ${total} message processors. Continue? [Y/n]: " proceed </dev/tty
    fi
    if [ -n "$proceed" ] && ! [[ "$proceed" =~ ^[Yy] ]]; then
      # Full abort, not a loop back to the top of this function: simpler to
      # implement, and every value collected so far (including RABBITMQ_*/
      # REDIS_*/MQTT_* below, never mind this function hasn't even reached
      # those yet) is still just local shell variables that vanish with the
      # process -- nothing has been written to disk yet for this role.
      echo "Aborted -- no message-processor configuration was written." >&2
      exit 1
    fi

    local i
    for (( i=existing_count+1; i<=total; i++ )); do
      ids_to_add+=("$i")
    done
  fi

  echo
  local id
  # ${arr[@]} directly under set -u throws "unbound variable" on bash 3.2
  # (macOS's default /bin/bash) when the array has zero elements -- the
  # non-interactive malformed-ID path above deliberately leaves ids_to_add
  # empty and defers to the top-level PROBLEMS_FILE check, so this has to
  # tolerate that instead of crashing here first.
  if [ "${#ids_to_add[@]}" -gt 0 ]; then
    for id in "${ids_to_add[@]}"; do
      if printf '%s\n' "$existing_ids" | grep -qx "$id"; then
        echo "  skyfollower-message-processor-${id} already has a service block in ${compose_file} -- leaving it as-is."
        continue
      fi
      mkdir -p "${role_dir}/data/skyfollower-message-processor-${id}"
      append_message_processor_service "$compose_file" "$id"
      echo "  Added skyfollower-message-processor-${id}."
    done
  fi

  LATITUDE="$(prompt_number_range LATITUDE "Receiver reference latitude (decimal degrees)" "$(existing_env_value "$env_file" LATITUDE)" -90 90)"
  LONGITUDE="$(prompt_number_range LONGITUDE "Receiver reference longitude (decimal degrees)" "$(existing_env_value "$env_file" LONGITUDE)" -180 180)"
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(existing_env_value "$env_file" RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(existing_env_value_or "$env_file" RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(existing_env_value_or "$env_file" RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(resolve_core_shared_password CORE_RABBITMQ_PASSWORD RABBITMQ_PASSWORD "RabbitMQ password" "$env_file")"
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$(existing_env_value "$env_file" REDIS_HOST)")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(existing_env_value_or "$env_file" REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file")"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(existing_env_value "$env_file" MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(existing_env_value_or "$env_file" MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(existing_env_value "$env_file" MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(existing_env_value "$env_file" MQTT_PASSWORD)" 0)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# Which processors run on this node -- and each one's MESSAGE_PROCESSOR_ID
# -- lives in docker-compose.message-processor.yaml as generated service
# blocks, not here. Re-run install.sh for this role to add more.

# Receiver's reference position, used to decode locally-referenced CPR
# positions. Decimal degrees.
LATITUDE=${LATITUDE}
LONGITUDE=${LONGITUDE}

RABBITMQ_HOST=${RABBITMQ_HOST}
RABBITMQ_PORT=${RABBITMQ_PORT}
RABBITMQ_USERNAME=${RABBITMQ_USERNAME}
RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}

REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
REDIS_PASSWORD=${REDIS_PASSWORD}

MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

collect_archive_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (archive) --"
  S3_BUCKET="$(prompt_string S3_BUCKET "S3 archive bucket name" "$(existing_env_value "$env_file" S3_BUCKET)")"
  AWS_DEFAULT_REGION="$(prompt_string AWS_DEFAULT_REGION "AWS region" "$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)")"
  # archive-processor and archive-compaction run under separate
  # least-privilege IAM identities (see specs/aws/iam-policies/), so each
  # gets its own key pair. S3_BUCKET and AWS_DEFAULT_REGION stay shared.
  ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID="$(prompt_string ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID "archive-processor AWS access key ID" "$(existing_env_value "$env_file" ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID)")"
  ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY="$(prompt_password_value ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY "archive-processor AWS secret access key" "$(existing_env_value "$env_file" ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY)")"
  ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID="$(prompt_string ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID "archive-compaction AWS access key ID" "$(existing_env_value "$env_file" ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID)")"
  ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY="$(prompt_password_value ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY "archive-compaction AWS secret access key" "$(existing_env_value "$env_file" ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY)")"
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(existing_env_value "$env_file" RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(existing_env_value_or "$env_file" RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(existing_env_value_or "$env_file" RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(resolve_core_shared_password CORE_RABBITMQ_PASSWORD RABBITMQ_PASSWORD "RabbitMQ password" "$env_file")"
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$(existing_env_value "$env_file" REDIS_HOST)")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(existing_env_value_or "$env_file" REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file")"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(existing_env_value "$env_file" MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(existing_env_value_or "$env_file" MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(existing_env_value "$env_file" MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(existing_env_value "$env_file" MQTT_PASSWORD)" 0)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# The archive bucket, shared by both archive services.
S3_BUCKET=${S3_BUCKET}
AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}

# archive-processor and archive-compaction each authenticate as their own
# least-privilege IAM identity (see specs/aws/iam-policies/). docker-compose
# maps the pair for each into the container as boto3's own
# AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY; no credentials are passed in
# code, so an instance role can replace a pair later by leaving it unset.
ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID=${ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID}
ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY=${ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY}
ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID=${ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID}
ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY=${ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY}

RABBITMQ_HOST=${RABBITMQ_HOST}
RABBITMQ_PORT=${RABBITMQ_PORT}
RABBITMQ_USERNAME=${RABBITMQ_USERNAME}
RABBITMQ_PASSWORD=${RABBITMQ_PASSWORD}

REDIS_HOST=${REDIS_HOST}
REDIS_PORT=${REDIS_PORT}
REDIS_PASSWORD=${REDIS_PASSWORD}

MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

write_env_header() {
  local env_file="$1" role_dir="$2"
  local compose_file
  compose_file="$(role_files "$ROLE_FOR_HEADER" | awk '{print $1}')"
  # umask, not a chmod afterwards: the file must never exist
  # world-readable, not even for the moment between creating and
  # tightening it.
  (
    umask 077
    cat > "$env_file" <<ENV_EOF
# Host-specific values for this SkyFollower deployment, read automatically
# by docker compose from this directory. Written by scripts/install.sh;
# re-run it (or edit this file directly) and \`docker compose up -d\` to
# change any of them.

# Tag every ghcr.io/brentio/skyfollower-* image resolves to. install.sh
# --upgrade rewrites this to the latest release and pulls it; set it to an
# older release tag and re-run \`docker compose up -d\` to roll back.
SKYFOLLOWER_VERSION=${IMAGE_VERSION}

# Compose project name -- the namespace every container and network on
# this host is named under.
COMPOSE_PROJECT_NAME=${PROJECT_NAME_FOR_HEADER}

# Which compose file \`docker compose\` acts on, so no -f flag is needed.
COMPOSE_FILE=${compose_file}

# Absolute path to this directory. Ofelia's scheduled jobs create their
# containers through the Docker Engine API, which accepts only an
# absolute host path as a bind-mount source -- it has no project
# directory to resolve a relative path against.
SKYFOLLOWER_ROOT=${role_dir}
ENV_EOF
  )
}

# ---------------------------------------------------------------------------
# Role directory naming and project name derivation
# ---------------------------------------------------------------------------

default_folder_for_role() {
  case "$1" in
    receiver) echo "receiver" ;;
    *) echo "$1" ;;
  esac
}

project_name_for_folder() {
  # Mirrors Compose's own project-name derivation, but sanitized and with
  # the "receiver" special case: Compose always appends the service name
  # ("receiver") when it builds a container name, so a folder called
  # "receiver" -- the common default -- would otherwise double up into
  # skyfollower-receiver-receiver-1.
  local folder_name="$1"
  local sanitized
  sanitized="$(sanitize_identifier "$folder_name")"
  case "$sanitized" in
    receiver) sanitized="" ;;
    *-receiver) sanitized="${sanitized%-receiver}" ;;
  esac
  if [ -n "$sanitized" ]; then
    echo "skyfollower-${sanitized}"
  else
    echo "skyfollower"
  fi
}

# ---------------------------------------------------------------------------
# Role selection
# ---------------------------------------------------------------------------

select_roles_interactively() {
  echo "Which roles does this host run? Select more than one only for roles"
  echo "that genuinely share a host, e.g. core + management-ui."
  echo
  local roles
  read -ra roles <<< "$ALL_ROLES"
  local i=1
  for r in "${roles[@]}"; do
    echo "  ${i}) ${r}"
    i=$((i+1))
  done
  echo
  local input
  read -r -p "Enter numbers separated by spaces or commas: " input </dev/tty
  input="$(echo "$input" | tr ',' ' ')"
  for n in $input; do
    if [[ "$n" =~ ^[0-9]+$ ]] && [ "$n" -ge 1 ] && [ "$n" -le "${#roles[@]}" ]; then
      SELECTED_ROLES+=("${roles[$((n-1))]}")
    fi
  done
  if [ "${#SELECTED_ROLES[@]}" -eq 0 ]; then
    echo "No valid roles selected." >&2
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# Finishing the job
# ---------------------------------------------------------------------------

offer_up() {
  local role="$1" role_dir="$2"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    (cd "$role_dir" && docker compose up -d)
    return
  fi
  local answer
  read -r -p "Bring ${role} up now (docker compose up -d in ${role_dir})? [Y/n]: " answer </dev/tty
  if [ -z "$answer" ] || [[ "$answer" =~ ^[Yy] ]]; then
    (cd "$role_dir" && docker compose up -d)
  fi
}

provision_rabbitmq_users() {
  # RabbitMQ's RABBITMQ_DEFAULT_USER/PASS env vars always create that user
  # as a full administrator on `/` -- there is no env var that creates it
  # scoped from the start. This runs once the container is actually up,
  # demoting the application user to SkyFollower's own resources and
  # creating a separate administrator for the dashboard, so a compromised
  # receiver/message-processor/archive host only ever holds a credential
  # that can publish/consume on known queue names, never one that can
  # reconfigure the broker. Every rabbitmqctl call below is idempotent, so
  # running this again (a second install.sh run, --upgrade, or manually
  # re-running just this role) is always safe -- including the migration
  # case, where an existing deployment's application user is still
  # full-admin from before this existed.
  local role_dir="$1"
  local rabbitmq_username rabbitmq_admin_username rabbitmq_admin_password
  local rabbitmq_monitoring_username rabbitmq_monitoring_password
  rabbitmq_username="$(existing_env_value "${role_dir}/.env" RABBITMQ_USERNAME)"
  rabbitmq_admin_username="$(existing_env_value "${role_dir}/.env" RABBITMQ_ADMIN_USERNAME)"
  rabbitmq_admin_password="$(existing_env_value "${role_dir}/.env" RABBITMQ_ADMIN_PASSWORD)"
  rabbitmq_monitoring_username="$(existing_env_value "${role_dir}/.env" RABBITMQ_MONITORING_USERNAME)"
  rabbitmq_monitoring_password="$(existing_env_value "${role_dir}/.env" RABBITMQ_MONITORING_PASSWORD)"
  if [ -z "$rabbitmq_username" ] || [ -z "$rabbitmq_admin_username" ] || [ -z "$rabbitmq_admin_password" ]; then
    echo "  ✗ ${role_dir}/.env is missing RabbitMQ credentials -- skipping user provisioning." >&2
    return
  fi

  local container_id
  container_id="$(cd "$role_dir" && docker compose ps -q rabbitmq)"
  if [ -z "$container_id" ]; then
    echo "RabbitMQ isn't running -- skipping user provisioning. Bring it up and" >&2
    echo "re-run this script for the core role to provision it." >&2
    return
  fi

  echo "Waiting for RabbitMQ to become healthy..."
  local waited=0 health=""
  while [ "$waited" -lt 60 ]; do
    health="$(docker inspect --format '{{.State.Health.Status}}' "$container_id" 2>/dev/null || echo "")"
    [ "$health" = "healthy" ] && break
    sleep 2
    waited=$((waited + 2))
  done
  if [ "$health" != "healthy" ]; then
    echo "  ✗ RabbitMQ did not report healthy within 60s -- skipping user provisioning." >&2
    echo "    Re-run this script for the core role once it's healthy." >&2
    return
  fi

  echo "Provisioning RabbitMQ users..."

  # configure/write/read, in that order -- amq.default is required for
  # write because completed flights are published to the archive queue
  # through the default exchange. skyfollower-message-processor-.* is each
  # processor's own queue (fleet-ID-named, not adsb-*-prefixed). Keep this
  # pattern in sync with shared/rabbitmq_topology.py's
  # SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN -- bash can't import that Python
  # constant directly, so the two copies have to be kept identical by hand;
  # core-health filters RabbitMQ's Management API queue list with that
  # constant, so a change here that isn't mirrored there (or vice versa)
  # silently drifts "what SkyFollower owns" apart between the two.
  if (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_user_tags "$rabbitmq_username") \
    && (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_permissions --vhost / "$rabbitmq_username" \
      '^(adsb.*|skyfollower-message-processor-.*|archive|amq\.default)$' '^(adsb.*|skyfollower-message-processor-.*|archive|amq\.default)$' '^(adsb.*|skyfollower-message-processor-.*|archive)$'); then
    echo "  ✓ ${rabbitmq_username}: no tags, scoped to SkyFollower's own resources"
  else
    echo "  ✗ Could not scope ${rabbitmq_username}'s tags/permissions -- check manually." >&2
  fi

  # add_user fails if the user already exists (a prior run already created
  # it, with its own password) -- that's fine, list_users first so this
  # doesn't print a scary error on every re-run.
  if ! (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl list_users 2>/dev/null | grep -q "^${rabbitmq_admin_username}[[:space:]]"); then
    (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl add_user "$rabbitmq_admin_username" "$rabbitmq_admin_password" >/dev/null) \
      || echo "  ✗ Could not create ${rabbitmq_admin_username} -- check manually." >&2
  fi
  if (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_user_tags "$rabbitmq_admin_username" administrator) \
    && (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_permissions --vhost / "$rabbitmq_admin_username" '.*' '.*' '.*'); then
    echo "  ✓ ${rabbitmq_admin_username}: administrator (dashboard login only -- see ${role_dir}/.env)"
  else
    echo "  ✗ Could not tag/grant permissions for ${rabbitmq_admin_username} -- check manually." >&2
  fi

  # core-health's broker-wide read-only credential. The "monitoring" tag
  # alone grants Management API visibility into every vhost/queue/
  # connection's aggregated stats -- no per-resource permission is needed
  # (or possible: `monitoring` is a role tag, not a permission scope), so
  # this is set to match nothing rather than left at RabbitMQ's own
  # all-matching default for a freshly add_user'd account.
  if [ -n "$rabbitmq_monitoring_username" ] && [ -n "$rabbitmq_monitoring_password" ]; then
    if ! (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl list_users 2>/dev/null | grep -q "^${rabbitmq_monitoring_username}[[:space:]]"); then
      (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl add_user "$rabbitmq_monitoring_username" "$rabbitmq_monitoring_password" >/dev/null) \
        || echo "  ✗ Could not create ${rabbitmq_monitoring_username} -- check manually." >&2
    fi
    if (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_user_tags "$rabbitmq_monitoring_username" monitoring) \
      && (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_permissions --vhost / "$rabbitmq_monitoring_username" '^$' '^$' '^$'); then
      echo "  ✓ ${rabbitmq_monitoring_username}: monitoring tag, no resource permissions (core-health only)"
    else
      echo "  ✗ Could not tag/grant permissions for ${rabbitmq_monitoring_username} -- check manually." >&2
    fi
  else
    echo "  ✗ ${role_dir}/.env is missing RabbitMQ monitoring credentials -- skipping core-health's RabbitMQ user." >&2
  fi
}

offer_ofelia_and_bulk_load() {
  local role_dir="$1"
  local answer
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    answer="y"
  else
    read -r -p "Start the runner scheduler (ofelia) too? [Y/n]: " answer </dev/tty
  fi
  if [ -n "$answer" ] && ! [[ "$answer" =~ ^[Yy] ]]; then
    return
  fi
  (cd "$role_dir" && docker compose --profile runners up -d ofelia)

  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    answer="n"
  else
    echo
    echo "First-time bulk load: seeds Redis by running every runner once"
    echo "(mictronics first -- most country runners resolve icao_hex against"
    echo "its index -- then the rest alphabetically, with uk-caa-registry last"
    echo "since it takes hours). Otherwise each runs on its own schedule and"
    echo "Redis fills in gradually."
    read -r -p "Run the bulk load now? [y/N]: " answer </dev/tty
  fi
  if [ -z "$answer" ] || ! [[ "$answer" =~ ^[Yy] ]]; then
    return
  fi

  # The runner list comes from `docker compose config --services`, not a
  # hardcoded list, so it cannot drift from what's actually declared.
  # `--profile runners` is required: every runner-* service is declared
  # `profiles: ["runners"]`, and `config --services` filters by active
  # profiles exactly as `up`/`pull`/`ps` do -- without it the list comes
  # back empty and the bulk load silently no-ops.
  # Every grep below is guarded with `|| true`: under set -e, a filter
  # that legitimately matches nothing (an unusual runner set, or none at
  # all) would otherwise take the whole script down right here instead of
  # just producing an empty list.
  local all_runners mictronics rest uk_last ordered
  all_runners="$(cd "$role_dir" && docker compose --profile runners config --services | grep '^runner-' || true)"
  mictronics="$(echo "$all_runners" | grep '^runner-mictronics$' || true)"
  uk_last="$(echo "$all_runners" | grep '^runner-uk-caa-registry$' || true)"
  rest="$(echo "$all_runners" | grep -v '^runner-mictronics$' | grep -v '^runner-uk-caa-registry$' | sort || true)"
  ordered="$(printf '%s\n%s\n%s\n' "$mictronics" "$rest" "$uk_last" | grep -v '^$' || true)"

  for r in $ordered; do
    echo "Running ${r}..."
    (cd "$role_dir" && docker compose run --rm "$r") || echo "  ${r} failed -- continuing with the rest." >&2
  done
}

# ---------------------------------------------------------------------------
# Upgrade mode
# ---------------------------------------------------------------------------

do_upgrade() {
  # REF/IMAGE_VERSION are already resolved by main() before dispatching
  # here -- resolving again would mean two GitHub API calls per --upgrade
  # run for the same answer.
  echo "Upgrading every role directory under ${INSTALL_ROOT} to ${REF}..."
  echo "(runner-* images are pulled too -- they sit behind the \"runners\" compose profile)"
  local found=0
  for env_file in "${INSTALL_ROOT}"/*/.env; do
    [ -e "$env_file" ] || continue
    found=1
    local role_dir
    role_dir="$(dirname "$env_file")"
    echo
    echo "-- ${role_dir} --"
    # Rewrite SKYFOLLOWER_VERSION in place -- every other line, including
    # any operator edits, is left exactly as it is.
    local tmp
    tmp="$(mktemp)"
    awk -v v="$IMAGE_VERSION" '
      /^SKYFOLLOWER_VERSION=/ { print "SKYFOLLOWER_VERSION=" v; next }
      { print }
    ' "$env_file" > "$tmp"
    (umask 077; mv "$tmp" "$env_file")
    # --profile runners on the pull so the runner-* services (all gated
    # behind profiles: ["runners"] in docker-compose.core.yaml) get their
    # images refreshed to the new tag -- without it every runner silently
    # stays on its old image after an upgrade. Blanket for every role dir:
    # a no-op where the compose file declares no such profile. The flag is
    # deliberately NOT passed to `up -d`: the runner services are one-shot
    # jobs (their CMD runs an import and exits), so `up` would kick off all
    # of them -- including the multi-hour uk-caa-registry -- on every
    # upgrade. Ofelia (always-on, no profile) is recreated by the `up -d`
    # below and spawns fresh runner containers from the pulled images on
    # schedule.
    (cd "$role_dir" && docker compose --profile runners pull && docker compose up -d)
  done
  if [ "$found" -eq 0 ]; then
    echo "No role directories found under ${INSTALL_ROOT} (looked for */.env)." >&2
    exit 1
  fi
  echo
  echo "Upgrade complete."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

print_banner() {
  echo
  cat <<'BANNER_EOF'
███████ ██   ██ ██    ██ ███████  ██████  ██      ██       ██████  ██     ██ ███████ ██████
██      ██  ██   ██  ██  ██      ██    ██ ██      ██      ██    ██ ██     ██ ██      ██   ██
███████ █████     ████   █████   ██    ██ ██      ██      ██    ██ ██  █  ██ █████   ██████
     ██ ██  ██     ██    ██      ██    ██ ██      ██      ██    ██ ██ ███ ██ ██      ██   ██
███████ ██   ██    ██    ██       ██████  ███████ ███████  ██████   ███ ███  ███████ ██   ██
BANNER_EOF
  echo
}

# Only asked when --root was not explicitly passed and the run is
# interactive -- confirms the resolved default (now $PWD, not
# ${HOME}/SkyFollower) before anything else happens, since preflight's
# writability check and every later step depend on INSTALL_ROOT being
# right from the start.
confirm_install_root() {
  local answer
  read -r -p "Use ${INSTALL_ROOT} as the root directory? [Y/n]: " answer </dev/tty
  case "$answer" in
    ""|[Yy]|[Yy][Ee][Ss])
      return
      ;;
  esac
  local path
  while true; do
    read -r -p "  Install root directory: " path </dev/tty
    if [ -n "$path" ]; then
      INSTALL_ROOT="$path"
      return
    fi
    echo "    Required." >&2
  done
}

main() {
  print_banner
  resolve_ref

  if [ "$NON_INTERACTIVE" -eq 0 ] && [ "$ROOT_EXPLICIT" -eq 0 ]; then
    confirm_install_root
  fi

  preflight

  if [ "$UPGRADE" -eq 1 ]; then
    do_upgrade
    return
  fi

  if [ "$NON_INTERACTIVE" -eq 1 ] && [ "${#SELECTED_ROLES[@]}" -eq 0 ]; then
    echo "--non-interactive requires at least one --role." >&2
    exit 1
  fi

  if [ "${#SELECTED_ROLES[@]}" -eq 0 ]; then
    select_roles_interactively
  fi

  for r in "${SELECTED_ROLES[@]}"; do
    case " $ALL_ROLES " in
      *" $r "*) ;;
      *)
        echo "Unknown role: $r" >&2
        usage
        ;;
    esac
    [ "$r" = "core" ] && CORE_SELECTED_IN_THIS_RUN=1
  done

  # collect_core_env() must actually run before any dependent role's
  # collect_*_env in the loop below, since those read CORE_REDIS_PASSWORD/
  # CORE_RABBITMQ_PASSWORD that only exist once core has stashed them (see
  # resolve_core_shared_password()) -- reorder core to the front regardless
  # of what order roles were selected/typed in, without disturbing the
  # relative order of the rest.
  if [ -n "${CORE_SELECTED_IN_THIS_RUN:-}" ]; then
    local reordered_roles=("core")
    for r in "${SELECTED_ROLES[@]}"; do
      [ "$r" = "core" ] || reordered_roles+=("$r")
    done
    SELECTED_ROLES=("${reordered_roles[@]}")
  fi

  mkdir -p "$INSTALL_ROOT"

  local installed_dirs=()
  local installed_roles=()

  for role in "${SELECTED_ROLES[@]}"; do
    local folder_name
    folder_name="$(default_folder_for_role "$role")"
    if [ "$role" = "receiver" ]; then
      # One unified prompt replaces the old separate "folder name"
      # and RECEIVER_NAME prompts -- the same operator-chosen name becomes
      # the install folder (sanitized the same way project_name_for_folder
      # already sanitizes it), the HA-displayed label, and (once Redis is
      # configured below) the name claimed via Redis SET NX, so there's
      # exactly one place to name a receiver instead of two prompts that
      # could disagree. default_receiver_name() (the machine's uppercased
      # short hostname) carries forward as this prompt's
      # suggested default. prompt_string is already non-interactive-safe
      # (reads RECEIVER_NAME from the environment instead of prompting),
      # so this isn't gated on NON_INTERACTIVE the way the old FOLDER_NAME
      # prompt was.
      echo
      RECEIVER_NAME="$(prompt_string RECEIVER_NAME "Receiver name (install folder + Home Assistant label)" "$(default_receiver_name)")"
      folder_name="$(sanitize_identifier "$RECEIVER_NAME")"
      [ -n "$folder_name" ] || folder_name="$(default_folder_for_role "$role")"
    fi
    local role_dir="${INSTALL_ROOT}/${folder_name}"
    mkdir -p "$role_dir"

    ROLE_FOR_HEADER="$role"
    PROJECT_NAME_FOR_HEADER="$(project_name_for_folder "$folder_name")"

    echo
    fetch_role "$role" "$role_dir"
    echo

    case "$role" in
      receiver) collect_receiver_env "$role_dir" ;;
      core) collect_core_env "$role_dir" ;;
      management-ui) collect_management_ui_env "$role_dir" ;;
      message-processor) collect_message_processor_env "$role_dir" ;;
      archive) collect_archive_env "$role_dir" ;;
    esac

    installed_dirs+=("$role_dir")
    installed_roles+=("$role")
  done

  if [ -s "$PROBLEMS_FILE" ]; then
    echo >&2
    echo "Missing required configuration:" >&2
    while IFS= read -r p; do
      echo "  - $p" >&2
    done < "$PROBLEMS_FILE"
    exit 1
  fi

  echo
  echo "Configuration written for: ${installed_roles[*]}"
  echo

  local i=0
  for role_dir in "${installed_dirs[@]}"; do
    local role="${installed_roles[$i]}"
    i=$((i+1))
    offer_up "$role" "$role_dir"
    if [ "$role" = "core" ]; then
      provision_rabbitmq_users "$role_dir"
      offer_ofelia_and_bulk_load "$role_dir"
    fi
  done

  echo
  echo "Summary:"
  i=0
  for role_dir in "${installed_dirs[@]}"; do
    echo "  ${installed_roles[$i]}: ${role_dir}"
    i=$((i+1))
  done
}

main
