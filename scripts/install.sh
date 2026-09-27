#!/usr/bin/env bash
#
# Interactive, non-root installer for SkyFollower: fetches config, prompts
# for credentials, and brings the stack up.
#
# Usage:
#   ./install.sh [--root <path>] [--role <role> ...] [--non-interactive] [--upgrade]
#
# Or, without cloning anything first:
#   curl -fsSL https://raw.githubusercontent.com/BrentIO/SkyFollower/main/scripts/install.sh | bash
#
# <role>: receiver, core, management-ui, message-processor, archive, map --
# may be repeated. Omit for an interactive multi-select prompt.
#
# --non-interactive reads every value from already-exported environment
# variables instead of prompting, and requires --role at least once. The
# receiver role configures exactly one instance per run; add more with a
# repeated run. Every missing required value is reported together at the
# end, not one per restart.
#
# --upgrade re-resolves the latest release tag, refreshes each role's
# fetched config/compose files, rewrites SKYFOLLOWER_VERSION, and runs
# `docker compose pull && up -d` in every role directory under the install
# root. No prompting.
#
# Files are fetched from the latest GitHub release tag by default: a
# docker-compose.*.yaml's image: :latest always matches that tag, not tip
# of main, so fetching from main risks a config/compose shape the pinned
# image doesn't understand. A file missing at the resolved tag falls back
# to main, with a warning.
#
# Dev build: set the `branch` env var to a branch name (or `main`) to fetch
# that branch's config/compose files and use the matching
# ghcr.io/brentio/skyfollower-*:dev-<branch> images -- one variable picks
# both so they can't desync. Those images must already be published via
# build-container-images.yaml's dev_mode; the installer checks GHCR first
# and stops with the exact command if they're missing. A real release tag
# (YYYY.MM.BB) as `branch` is a hard error. See docs/getting-started/
# index.md's "Testing a dev build" section.
#   curl -fsSL .../install.sh | branch=my-branch bash
#   curl -fsSL .../install.sh | branch=my-branch bash -s -- --upgrade

set -euo pipefail

# ---------------------------------------------------------------------------
# Never escalate: no step here may invoke sudo. A root precondition is
# detected, reported with the exact command to run, and the script exits
# so it can be re-run once that's done.
# ---------------------------------------------------------------------------

SCRIPT_NAME="$(basename "$0")"
NON_INTERACTIVE=0
UPGRADE=0
INSTALL_ROOT="$PWD"
ROOT_EXPLICIT=0
SELECTED_ROLES=()

# Set by resolve_ref(): DEV_BUILD=1 when the `branch` env var is present,
# REF/IMAGE_VERSION/BRANCH follow from it (see resolve_ref).
DEV_BUILD=0
BRANCH=""

ALL_ROLES="core management-ui archive message-processor receiver map"

# Fixed dependency order the selected roles are sorted into before the
# install loop runs: core stashes shared secrets the others read, and
# archive must deploy the CloudFormation stack before management-ui reads
# its outputs. map has no dependency on/from any other role.
ROLE_DEPENDENCY_ORDER="core receiver message-processor archive management-ui map"

# Exit code depends on why usage() is shown: 0 for --help, 1 otherwise.
usage() {
  local code="${1:-1}"
  cat >&2 <<USAGE
Usage: $SCRIPT_NAME [--root <path>] [--role <role> ...] [--non-interactive] [--upgrade]
  role: receiver | core | management-ui | message-processor | archive | map
  env: branch=<name>  install/upgrade a dev build from that branch instead
                      of the latest release (images must be published first
                      via build-container-images.yaml's dev_mode)
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

# wget is a fallback for a minimal image with wget but not curl.
if command -v curl >/dev/null 2>&1; then
  http_get() { curl -fsSL "${1}"; }
elif command -v wget >/dev/null 2>&1; then
  http_get() { wget -qO- "${1}"; }
else
  echo "Neither curl nor wget is available -- install one and re-run." >&2
  exit 1
fi

# Anonymous existence check for a public GHCR image tag via the registry
# v2 API. Returns 0 if the tag exists, 1 if it does not, 2 if GHCR could
# not be reached (caller warns and continues rather than hard-failing).
ghcr_tag_exists() {
  local repo="$1" tag="$2" token
  local accept='application/vnd.oci.image.index.v1+json,application/vnd.docker.distribution.manifest.list.v2+json,application/vnd.docker.distribution.manifest.v2+json'
  if command -v curl >/dev/null 2>&1; then
    token="$(curl -fsSL "https://ghcr.io/token?scope=repository:${repo}:pull" 2>/dev/null \
      | grep -o '"token":"[^"]*"' | head -1 | cut -d '"' -f 4 || true)"
    [ -n "$token" ] || return 2
    curl -fsS -o /dev/null -H "Authorization: Bearer ${token}" -H "Accept: ${accept}" \
      "https://ghcr.io/v2/${repo}/manifests/${tag}" 2>/dev/null && return 0 || return 1
  else
    token="$(wget -qO- "https://ghcr.io/token?scope=repository:${repo}:pull" 2>/dev/null \
      | grep -o '"token":"[^"]*"' | head -1 | cut -d '"' -f 4 || true)"
    [ -n "$token" ] || return 2
    wget -q -O /dev/null --header="Authorization: Bearer ${token}" --header="Accept: ${accept}" \
      "https://ghcr.io/v2/${repo}/manifests/${tag}" 2>/dev/null && return 0 || return 1
  fi
}

resolve_ref() {
  # `branch` env var present -> dev install (see header comment).
  if [ -n "${branch:-}" ]; then
    BRANCH="$branch"
    if printf '%s' "$BRANCH" | grep -qE '^[0-9]{4}\.[0-9]{2}\.[0-9]{2}$'; then
      echo "branch='${BRANCH}' looks like a release tag. Omit 'branch' entirely for a release install/upgrade (it resolves the latest release automatically); set 'branch' only to a branch name for a dev build." >&2
      exit 1
    fi
    DEV_BUILD=1
    REF="$BRANCH"
    # Docker tags can't contain '/'; build-container-images.yaml sanitizes
    # the same way when it publishes :dev-<branch>.
    local sanitized
    sanitized="$(printf '%s' "$BRANCH" | tr '/' '-')"
    IMAGE_VERSION="dev-${sanitized}"
    # One canary image is enough: dev_mode builds the whole matrix in one
    # workflow run.
    local canary="brentio/skyfollower-message-processor"
    local rc=0
    ghcr_tag_exists "$canary" "$IMAGE_VERSION" || rc=$?
    if [ "$rc" -eq 1 ]; then
      cat >&2 <<EOF
No dev build published for '${BRANCH}' (looked for ghcr.io/${canary}:${IMAGE_VERSION}). Run it first:

  gh workflow run build-container-images.yaml --ref ${BRANCH} -f dev_mode=true

then re-run this installer.
EOF
      exit 1
    elif [ "$rc" -eq 2 ]; then
      echo "Warning: could not reach GHCR to verify the '${IMAGE_VERSION}' dev build exists -- continuing; 'docker compose pull' will fail later if it is missing." >&2
    fi
    echo "Dev build: branch '${BRANCH}', images ${IMAGE_VERSION}"
    return
  fi
  # No jq assumption (not guaranteed on a minimal image); tag_name is a
  # plain top-level string, so grep/cut suffices. No `grep -m1` -- it would
  # close the pipe early and SIGPIPE curl before the body finishes, taking
  # the script down under pipefail. `|| true` stops a genuinely-absent
  # tag_name's no-match failure from doing the same under `set -e`.
  REF="$(http_get "https://api.github.com/repos/BrentIO/SkyFollower/releases/latest" \
    | grep '"tag_name"' | head -1 | cut -d '"' -f 4 || true)"
  if [ -z "$REF" ]; then
    echo "Could not determine the latest release tag from the GitHub API -- retry, or for a dev build set 'branch=<name>'." >&2
    exit 1
  fi
  IMAGE_VERSION="$REF"
  echo "Using latest release: ${REF}"
}

# ---------------------------------------------------------------------------
# Preflight -- runs before any prompting, so a host that can't work fails
# immediately instead of after twenty minutes of questions.
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
    # A successful `docker compose ...` (a `docker` subcommand, not the
    # separate legacy `docker-compose` binary) is itself the signal that
    # the modern Compose plugin is installed. Not pattern-matching the
    # version's leading digit: that would go stale the moment Compose
    # ships a 3.x/4.x/5.x release.
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

  # The install root, or its nearest existing ancestor, must be writable by
  # the current user -- Docker will happily create a missing bind-mount
  # source itself, but as root, unremovable by an operator merely in the
  # docker group.
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
  # Matches Docker Compose's own project-name character rules instead of
  # relying on Compose silently stripping anything else.
  local out
  out="$(printf '%s' "$1" | tr 'A-Z' 'a-z' | tr -c 'a-z0-9_-' '-')"
  while [ "${out#-}" != "$out" ]; do out="${out#-}"; done
  while [ "${out%-}" != "$out" ]; do out="${out%-}"; done
  printf '%s' "$out"
}

existing_env_value() {
  # Prints the last KEY=... line's value from an existing .env (last-write-
  # wins for a repeated key), or nothing if the file/key doesn't exist.
  local file="$1" key="$2"
  [ -f "$file" ] || return 0
  # `|| true`: a legitimately-absent key makes grep exit non-zero on zero
  # matches, which as this function's last command would otherwise take
  # the whole script down under `set -e` on every caller, not just fail to
  # find a default.
  grep -E "^${key}=" "$file" 2>/dev/null | tail -1 | cut -d= -f2- || true
}

existing_env_value_or() {
  # existing_env_value() always exits 0, so checks the printed value
  # itself rather than relying on exit status for the fallback.
  local file="$1" key="$2" default="$3" val
  val="$(existing_env_value "$file" "$key")"
  if [ -n "$val" ]; then
    printf '%s' "$val"
  else
    printf '%s' "$default"
  fi
}

default_receiver_name() {
  # Suggested RECEIVER_NAME default: the machine's short hostname,
  # uppercased. Falls back to stripping after the first '.' if `hostname
  # -s` isn't available. `tr`, not bash 4+ `${h^^}` -- this script targets
  # macOS's default bash 3.2 too.
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

detect_lan_ip() {
  # Asks the OS which local IP it would route a packet to a public address
  # through -- a UDP "connect" never sends anything, so this is a pure
  # routing-table lookup. Prints nothing if python3 is unavailable or
  # there's no route; the TLS cert's SAN then just covers localhost/hostname.
  command -v python3 >/dev/null 2>&1 || return 0
  python3 -c '
import socket
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    print(s.getsockname()[0])
except Exception:
    pass
' 2>/dev/null
}

tls_san_entry() {
  # One openssl `-addext subjectAltName=...` entry: IP:<addr> for a
  # dotted-quad, DNS:<name> otherwise.
  local value="$1"
  if [[ "$value" =~ ^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$ ]]; then
    printf 'IP:%s' "$value"
  else
    printf 'DNS:%s' "$value"
  fi
}

generate_self_signed_cert() {
  # Idempotent self-signed TLS cert+key generation shared by management-ui
  # and map. Fixed filenames (cert.pem/key.pem) double as the BYO-cert
  # path: an operator can drop their own pair into $1 before running
  # install.sh, and this function leaves them untouched.
  #
  # $1 = tls_dir, $2 = label (prompts/log lines), $3 = varname for the
  # optional extra SAN, kept per-role so a non-interactive run can set
  # MANAGEMENT_UI_TLS_EXTRA_SAN / MAP_TLS_EXTRA_SAN independently.
  local tls_dir="$1" label="$2" san_varname="$3"
  local cert_file="${tls_dir}/cert.pem" key_file="${tls_dir}/key.pem"

  mkdir -p "$tls_dir"

  if [ -s "$cert_file" ] && [ -s "$key_file" ]; then
    echo "  ${label} TLS cert already exists at ${tls_dir} -- left as-is."
    echo "  (This is also how to bring your own: drop cert.pem/key.pem there before running install.sh.)"
    return
  fi

  if ! command -v openssl >/dev/null 2>&1; then
    echo "  openssl not found -- skipping self-signed TLS cert generation for ${label}." >&2
    echo "  Drop your own cert.pem/key.pem into ${tls_dir}, or install openssl and re-run." >&2
    return
  fi

  local extra_san
  extra_san="$(prompt_string "$san_varname" "Extra hostname/IP for the ${label} TLS certificate (optional)" "" 0)"

  local lan_ip lan_host
  lan_ip="$(detect_lan_ip)"
  lan_host="$(hostname -s 2>/dev/null)"
  [ -z "$lan_host" ] && lan_host="$(hostname 2>/dev/null)"

  local san="DNS:localhost,IP:127.0.0.1"
  [ -n "$lan_host" ] && san="${san},DNS:${lan_host}"
  [ -n "$lan_ip" ] && san="${san},IP:${lan_ip}"
  [ -n "$extra_san" ] && san="${san},$(tls_san_entry "$extra_san")"

  echo "  Generating self-signed TLS certificate for ${label} (10-year validity; SAN: ${san})..."
  # umask so the private key is never briefly world/group-readable between
  # openssl creating it and the chmod below.
  if ! (umask 077 && openssl req -x509 -nodes -newkey rsa:2048 \
      -keyout "$key_file" -out "$cert_file" -days 3650 \
      -subj "/CN=${lan_host:-localhost}" \
      -addext "subjectAltName=${san}" >/dev/null 2>&1); then
    echo "  openssl failed to generate a TLS cert for ${label} -- drop your own cert.pem/key.pem into ${tls_dir} instead." >&2
    rm -f "$cert_file" "$key_file"
    return
  fi
  chmod 600 "$key_file"
  chmod 644 "$cert_file"
  echo "  Wrote ${cert_file} and ${key_file} (never printed to the terminal)."
}

# In --non-interactive mode, every prompt_* helper reads the named
# environment variable instead of calling `read`, and records a problem
# (rather than exiting immediately) if required and unset, so every
# missing value is reported together at the end.
#
# A file, not a bash array: every prompt_* function runs as
# X="$(prompt_string ...)", which forks a subshell -- an array append
# inside that subshell would vanish when it exits. A file survives the
# subshell boundary that in-memory shell state cannot cross.
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
  # required defaults to 1, but MQTT_PASSWORD passes 0: MQTT supports an
  # anonymous connection (blank username and password).
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
  # Decimal (lat/long), so validated with python3 rather than shell
  # integer comparison. `required=0` lets a blank answer mean "leave this
  # feature disabled" (see collect_map_env()'s center lat/long).
  local varname="$1" label="$2" default="$3" min="$4" max="$5" required="${6:-1}"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    local val="${!varname:-$default}"
    if [ -z "$val" ]; then
      if [ "$required" -eq 1 ]; then
        record_problem "$varname is required but is not set"
      fi
    elif ! python3 -c "import sys; v=float('$val'); sys.exit(0 if $min<=v<=$max else 1)" 2>/dev/null; then
      record_problem "$varname must be a number between $min and $max (got '$val')"
    fi
    printf '%s' "$val"
    return
  fi
  local input prompt_default="$default"
  if [ -z "$prompt_default" ] && [ "$required" -eq 0 ]; then
    prompt_default="blank to disable"
  fi
  while true; do
    read -r -p "  ${label} [${prompt_default:-required}]: " input </dev/tty
    input="${input:-$default}"
    if [ -z "$input" ] && [ "$required" -eq 0 ]; then
      printf '%s' ""
      return
    fi
    if [ -n "$input" ] && python3 -c "import sys; v=float('$input'); sys.exit(0 if $min<=v<=$max else 1)" 2>/dev/null; then
      printf '%s' "$input"
      return
    fi
    echo "    Must be a number between $min and $max." >&2
  done
}

validate_receiver_sources() {
  # Mirrors shared/config.py's parse_receiver_sources -- kept in sync by
  # hand; this validates input before it reaches that parser, not a
  # substitute for it.
  local raw="$1" triple host port tag
  IFS=',' read -ra triples <<< "$raw"
  [ "${#triples[@]}" -eq 0 ] && return 1
  for triple in "${triples[@]}"; do
    IFS=':' read -r host port tag <<< "$triple"
    [ -z "$host" ] && return 1
    [[ "$port" =~ ^[0-9]+$ ]] || return 1
    [ "$port" -ge 1 ] && [ "$port" -le 65535 ] || return 1
    # tr, not bash 4+ ${tag^^} -- this script targets bash 3.2+.
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
  # No dependency on GNU coreutils' `timeout` (absent on macOS by
  # default): a background watchdog kills the connection attempt after 3s
  # if it's still running, and `wait` below picks up the real exit status
  # either way.
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
  # `|| true`: the watchdog routinely has already fired and exited on its
  # own, and under `set -e` an unchecked failure here would kill the
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
  # Echoes "compose_file config_file..." for a role, mirroring
  # docs/deployment/index.md's Compose Files/Configuration tables --
  # update both together if it ever changes.
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
    map)
      echo "docker-compose.map.yaml"
      ;;
  esac
}

role_data_dirs() {
  case "$1" in
    receiver)
      # Nothing fixed here: which per-instance data dirs exist isn't known
      # until collect_receiver_env() creates them itself.
      ;;
    core)
      echo "data/rabbitmq data/redis"
      ;;
    management-ui)
      echo "data/management-ui"
      ;;
    message-processor)
      # Nothing fixed here: which per-ID data dirs exist isn't known until
      # collect_message_processor_env() creates them itself.
      ;;
    archive)
      echo "data/archive-processor data/archive-compaction data/archive-index-cache"
      ;;
    map)
      # map-redis is deliberately ephemeral (see docker-compose.map.yaml).
      echo "data/map/tls data/map/range-outline"
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
    # The message-processor/receiver compose files hold generated
    # per-instance service blocks once collect_*_env() has run --
    # re-fetching would discard them, so no-clobber like config/*.example
    # below (delete by hand to pick up template/anchor changes).
    if [ -e "$dest_path" ] && {
      { [ "$role" = "message-processor" ] && [ "$rel_path" = "docker-compose.message-processor.yaml" ]; } ||
      { [ "$role" = "receiver" ] && [ "$rel_path" = "docker-compose.receiver.yaml" ]; }
    }; then
      echo "  ${rel_path} (already exists -- left as-is, holds this node's generated service blocks)"
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
    echo "    Not in ${REF} yet -- falling back to main for this file." >&2
    http_get "${main_base}/${rel_path}" > "$dest_path"
  done

  # No-clobber: never overwrites a config file the operator already filled in.
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
# Per-role .env body -- only the values genuinely worth an interactive
# prompt. Everything else (LOG_LEVEL, the Athena names) gets a sensible
# default and is written directly; an operator can still edit .env
# afterward. Internal timing values live in shared/timing.py, not here;
# flight_ttl_seconds lives in the config:flight_ttl_seconds Redis key.
# ---------------------------------------------------------------------------

collect_receiver_env() {
  local role_dir="$1" env_file="${1}/.env"
  local compose_file="${role_dir}/docker-compose.receiver.yaml"
  echo "-- ${role_dir} (receiver) --"

  # One or more receiver instances share this host and this .env. Each
  # instance is a name (Home Assistant label + Redis identity) plus its own
  # RECEIVER_SOURCES, baked into a generated service block in
  # docker-compose.receiver.yaml; connection settings below are shared.
  # Re-running appends any name whose slug isn't already a block.
  local existing_slugs
  existing_slugs="$(existing_receiver_slugs "$compose_file")"

  local first=1
  while true; do
    if [ "$first" -eq 0 ]; then
      [ "$NON_INTERACTIVE" -eq 1 ] && break   # exactly one instance
      local another
      read -r -p "  Add another receiver on this host? [y/N]: " another </dev/tty
      { [ -n "$another" ] && [[ "$another" =~ ^[Yy] ]]; } || break
    fi

    RECEIVER_NAME="$(prompt_string RECEIVER_NAME "Receiver name (Home Assistant label + Redis identity)" "$(default_receiver_name)")"
    local slug
    slug="$(sanitize_identifier "$RECEIVER_NAME")"
    if [ -z "$slug" ]; then
      record_problem "RECEIVER_NAME must contain at least one letter or number (got '${RECEIVER_NAME}')"
    elif printf '%s\n' "$existing_slugs" | grep -qx "$slug"; then
      echo "  skyfollower-receiver-${slug} already has a service block in ${compose_file} -- leaving it as-is."
    else
      RECEIVER_SOURCES="$(prompt_receiver_sources "")"
      mkdir -p "${role_dir}/data/skyfollower-receiver-${slug}"
      append_receiver_service "$compose_file" "$RECEIVER_NAME" "$RECEIVER_SOURCES"
      existing_slugs="$(printf '%s\n%s' "$existing_slugs" "$slug")"
      echo "  Added skyfollower-receiver-${slug}."
    fi

    first=0
    [ "$NON_INTERACTIVE" -eq 1 ] && break
  done

  echo
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(shared_conn_default "$env_file" RABBITMQ_HOST SHARED_CONN_RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(shared_conn_default "$env_file" RABBITMQ_PORT SHARED_CONN_RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(shared_conn_default "$env_file" RABBITMQ_USERNAME SHARED_CONN_RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(prompt_password_value RABBITMQ_PASSWORD "RabbitMQ password" "$(shared_conn_default "$env_file" RABBITMQ_PASSWORD SHARED_CONN_RABBITMQ_PASSWORD)")"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(shared_conn_default "$env_file" MQTT_HOST SHARED_CONN_MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(shared_conn_default "$env_file" MQTT_PORT SHARED_CONN_MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(shared_conn_default "$env_file" MQTT_USERNAME SHARED_CONN_MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(shared_conn_default "$env_file" MQTT_PASSWORD SHARED_CONN_MQTT_PASSWORD)" 0)"
  # Optional -- blank disables identity claim/heartbeat, period-counter
  # sensors, and core-health registration; RECEIVER_NAME then stays purely
  # cosmetic (a generated UUID identity is used instead).
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host (leave blank to disable identity claim + message counters)" "$(shared_conn_default "$env_file" REDIS_HOST SHARED_CONN_REDIS_HOST)" 0)"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(shared_conn_default "$env_file" REDIS_PORT SHARED_CONN_REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(prompt_password_value REDIS_PASSWORD "Redis password" "$(shared_conn_default "$env_file" REDIS_PASSWORD SHARED_CONN_REDIS_PASSWORD)" 0)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"

  # Stashed so a later non-core role in this run defaults to it instead of
  # its own empty .env (see shared_conn_default()).
  SHARED_CONN_RABBITMQ_HOST="$RABBITMQ_HOST"
  SHARED_CONN_RABBITMQ_PORT="$RABBITMQ_PORT"
  SHARED_CONN_RABBITMQ_USERNAME="$RABBITMQ_USERNAME"
  SHARED_CONN_RABBITMQ_PASSWORD="$RABBITMQ_PASSWORD"
  SHARED_CONN_MQTT_HOST="$MQTT_HOST"
  SHARED_CONN_MQTT_PORT="$MQTT_PORT"
  SHARED_CONN_MQTT_USERNAME="$MQTT_USERNAME"
  SHARED_CONN_MQTT_PASSWORD="$MQTT_PASSWORD"
  SHARED_CONN_REDIS_HOST="$REDIS_HOST"
  SHARED_CONN_REDIS_PORT="$REDIS_PORT"
  SHARED_CONN_REDIS_PASSWORD="$REDIS_PASSWORD"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# Which receivers run on this node -- and each one's RECEIVER_NAME
# (Home Assistant label + Redis identity) and RECEIVER_SOURCES
# (comma-separated host:port:source triples; source is 1090, 978, or
# EXTERNAL) -- lives in docker-compose.receiver.yaml as generated service
# blocks, not here. Re-run install.sh for the receiver role to add more.
# The connection settings below are shared across every instance.

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

# ---------------------------------------------------------------------------
# Shared RabbitMQ/Redis/MQTT connection values, reused across non-core roles
# within one install.sh run.
# ---------------------------------------------------------------------------

# SHARED_CONN_* mirror the AWS_PROV_* pattern below: lets a second non-core
# role in the same run (e.g. archive then management-ui) default to what a
# sibling role already collected, instead of each value's bare fallback.
# core is not a participant -- it hosts RabbitMQ/Redis rather than dialing
# out, so it uses a separate mechanism (see resolve_core_shared_password).
# Initialised once before the role loop, blanked again at the end.
init_shared_conn_globals() {
  SHARED_CONN_RABBITMQ_HOST=""
  SHARED_CONN_RABBITMQ_PORT=""
  SHARED_CONN_RABBITMQ_USERNAME=""
  SHARED_CONN_RABBITMQ_PASSWORD=""
  SHARED_CONN_REDIS_HOST=""
  SHARED_CONN_REDIS_PORT=""
  SHARED_CONN_REDIS_PASSWORD=""
  SHARED_CONN_MQTT_HOST=""
  SHARED_CONN_MQTT_PORT=""
  SHARED_CONN_MQTT_USERNAME=""
  SHARED_CONN_MQTT_PASSWORD=""
}
clear_shared_conn_globals() { init_shared_conn_globals; }

# Prompt-default precedence: (1) this role's own existing .env, (2) the
# run-scoped SHARED_CONN_* cache an earlier non-core role in this run
# collected, (3) the hardcoded fallback. Always just a prompt default --
# never skips the prompt.
shared_conn_default() {
  local env_file="$1" key="$2" cache_var="$3" fallback="${4:-}"
  local existing
  existing="$(existing_env_value "$env_file" "$key")"
  if [ -n "$existing" ]; then
    printf '%s' "$existing"
    return
  fi
  local cached="${!cache_var:-}"
  if [ -n "$cached" ]; then
    printf '%s' "$cached"
    return
  fi
  printf '%s' "$fallback"
}

# Reuses a password collect_core_env() already collected/generated earlier
# in this run instead of a dependent role re-prompting against its own
# (often still-empty) .env -- skips the prompt entirely when a CORE_* value
# exists, since it's already decided. Falls back to shared_conn_default()
# when core wasn't selected in this run at all.
resolve_core_shared_password() {
  local core_var="$1" varname="$2" label="$3" env_file="$4" cache_var="$5"
  local core_val="${!core_var:-}"
  if [ -n "$core_val" ]; then
    printf '%s' "$core_val"
    return
  fi
  prompt_password_value "$varname" "$label" "$(shared_conn_default "$env_file" "$varname" "$cache_var")"
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
  # Stashed for a dependent role's collect_*_env to reuse silently -- see
  # resolve_core_shared_password() above.
  CORE_RABBITMQ_PASSWORD="$RABBITMQ_PASSWORD"
  # Fixed, not prompted -- never leaves this host (RabbitMQ is provisioned
  # by rabbitmqctl after startup; see provision_rabbitmq_users).
  RABBITMQ_ADMIN_USERNAME="$(existing_env_value_or "$env_file" RABBITMQ_ADMIN_USERNAME skyfollower-admin)"
  RABBITMQ_ADMIN_PASSWORD="$(existing_env_value "$env_file" RABBITMQ_ADMIN_PASSWORD)"
  if [ -z "$RABBITMQ_ADMIN_PASSWORD" ]; then
    RABBITMQ_ADMIN_PASSWORD="$(generate_password)"
  fi
  # core-health's own broker-wide read-only credential (RabbitMQ's
  # `monitoring` tag), provisioned the same way: fixed username, generated
  # password, never prompted.
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
  # If core is also selected in this run, default to the host loopback
  # address (Redis' port is published to the host) rather than the "redis"
  # service name, which only resolves inside core's own Compose network.
  # Only applies below the per-role .env and cross-role cache defaults.
  local redis_default
  redis_default="$(shared_conn_default "$env_file" REDIS_HOST SHARED_CONN_REDIS_HOST)"
  if [ -z "$redis_default" ] && [ -n "${CORE_SELECTED_IN_THIS_RUN:-}" ]; then
    redis_default="localhost"
  fi
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$redis_default")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(shared_conn_default "$env_file" REDIS_PORT SHARED_CONN_REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file" SHARED_CONN_REDIS_PASSWORD)"
  # Reads the archive stack's outputs so bucket/region/credentials
  # pre-fill the prompts below. Declining falls through unchanged.
  offer_aws_provisioning management-ui "$env_file"
  S3_BUCKET="$(prompt_string S3_BUCKET "S3 archive bucket name" "${AWS_PROV_S3_BUCKET:-$(existing_env_value "$env_file" S3_BUCKET)}")"
  AWS_DEFAULT_REGION="$(prompt_string AWS_DEFAULT_REGION "AWS region" "${AWS_PROV_REGION:-$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)}")"
  AWS_ACCESS_KEY_ID="$(prompt_string AWS_ACCESS_KEY_ID "AWS access key ID" "${AWS_PROV_MANAGEMENT_UI_KEY_ID:-$(existing_env_value "$env_file" AWS_ACCESS_KEY_ID)}")"
  AWS_SECRET_ACCESS_KEY="$(prompt_password_value AWS_SECRET_ACCESS_KEY "AWS secret access key" "${AWS_PROV_MANAGEMENT_UI_SECRET:-$(existing_env_value "$env_file" AWS_SECRET_ACCESS_KEY)}")"
  # Optional -- leave MQTT_HOST blank to disable MQTT entirely.
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host (blank to disable Home Assistant presence)" "$(shared_conn_default "$env_file" MQTT_HOST SHARED_CONN_MQTT_HOST)" 0)"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(shared_conn_default "$env_file" MQTT_PORT SHARED_CONN_MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(shared_conn_default "$env_file" MQTT_USERNAME SHARED_CONN_MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(shared_conn_default "$env_file" MQTT_PASSWORD SHARED_CONN_MQTT_PASSWORD)" 0)"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  SHARED_CONN_MQTT_HOST="$MQTT_HOST"
  SHARED_CONN_MQTT_PORT="$MQTT_PORT"
  SHARED_CONN_MQTT_USERNAME="$MQTT_USERNAME"
  SHARED_CONN_MQTT_PASSWORD="$MQTT_PASSWORD"

  generate_self_signed_cert "${role_dir}/data/management-ui/tls" "management-ui" MANAGEMENT_UI_TLS_EXTRA_SAN

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

# Optional -- leave MQTT_HOST blank to disable the Home Assistant presence
# (discovery + running version + start time). No telemetry is published
# either way.
MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

collect_map_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (map) --"

  # Single instance -- map is not horizontally scaled, so no per-instance
  # service blocks. No SHARED_CONN_* reuse for map's own settings (nothing
  # in common with the other roles' RabbitMQ/Redis); MQTT below is the
  # one exception, shared with every role that talks to the broker.
  MAP_LISTEN_HOST="$(prompt_string MAP_LISTEN_HOST "UDP listener bind address" "$(existing_env_value_or "$env_file" MAP_LISTEN_HOST 0.0.0.0)")"
  MAP_LISTEN_PORT="$(prompt_int_range MAP_LISTEN_PORT "UDP listener bind port (message-processor's MAP_UDP_PORT must point here)" "$(existing_env_value_or "$env_file" MAP_LISTEN_PORT 30500)" 1 65535)"
  MAP_HTTP_HOST="$(prompt_string MAP_HTTP_HOST "REST/WebSocket bind address" "$(existing_env_value_or "$env_file" MAP_HTTP_HOST 0.0.0.0)")"
  MAP_HTTP_PORT="$(prompt_int_range MAP_HTTP_PORT "REST/WebSocket bind port (HTTPS by default)" "$(existing_env_value_or "$env_file" MAP_HTTP_PORT 443)" 1 65535)"
  # Dedicated map-redis (bundled in docker-compose.map.yaml) -- never core
  # Redis. Defaults to the map-redis service name.
  MAP_REDIS_HOST="$(prompt_string MAP_REDIS_HOST "map-redis host" "$(existing_env_value_or "$env_file" MAP_REDIS_HOST map-redis)")"
  MAP_REDIS_PORT="$(prompt_int_range MAP_REDIS_PORT "map-redis port" "$(existing_env_value_or "$env_file" MAP_REDIS_PORT 6379)" 1 65535)"
  # Optional -- map-redis has no auth by default; only set this for an
  # external, already-secured Redis instead of the bundled one.
  MAP_REDIS_PASSWORD="$(prompt_password_value MAP_REDIS_PASSWORD "map-redis password (blank for none)" "$(existing_env_value "$env_file" MAP_REDIS_PASSWORD)" 0)"
  MAP_STALE_SECONDS="$(prompt_int_range MAP_STALE_SECONDS "Stale TTL, seconds (aircraft fades but stays visible)" "$(existing_env_value_or "$env_file" MAP_STALE_SECONDS 15)" 1 86400)"
  MAP_HIDE_SECONDS="$(prompt_int_range MAP_HIDE_SECONDS "Hide TTL, seconds (aircraft drops from view but trail data is kept)" "$(existing_env_value_or "$env_file" MAP_HIDE_SECONDS 60)" 1 86400)"
  # Should equal core Redis's config:flight_ttl_seconds for this
  # deployment -- this role never queries core Redis, so it's a reminder,
  # not an auto-detected value.
  MAP_EVICT_SECONDS="$(prompt_int_range MAP_EVICT_SECONDS "Evict TTL, seconds (aircraft fully removed -- should match this deployment's flight_ttl_seconds)" "$(existing_env_value_or "$env_file" MAP_EVICT_SECONDS 300)" 1 86400)"
  probe_tcp "$MAP_REDIS_HOST" "$MAP_REDIS_PORT" "map-redis"

  # Optional "center" reference point for the frontend's on-map marker,
  # initial camera position, and "Return to center" button. Both or
  # neither: leave blank to leave the feature disabled.
  MAP_CENTER_LATITUDE="$(prompt_number_range MAP_CENTER_LATITUDE "Center reference latitude, decimal degrees (blank to disable the center marker/recenter)" "$(existing_env_value "$env_file" MAP_CENTER_LATITUDE)" -90 90 0)"
  MAP_CENTER_LONGITUDE="$(prompt_number_range MAP_CENTER_LONGITUDE "Center reference longitude, decimal degrees (blank to disable the center marker/recenter)" "$(existing_env_value "$env_file" MAP_CENTER_LONGITUDE)" -180 180 0)"

  # Optional -- leave MQTT_HOST blank to disable MQTT entirely.
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host (blank to disable Home Assistant presence)" "$(shared_conn_default "$env_file" MQTT_HOST SHARED_CONN_MQTT_HOST)" 0)"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(shared_conn_default "$env_file" MQTT_PORT SHARED_CONN_MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(shared_conn_default "$env_file" MQTT_USERNAME SHARED_CONN_MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(shared_conn_default "$env_file" MQTT_PASSWORD SHARED_CONN_MQTT_PASSWORD)" 0)"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  SHARED_CONN_MQTT_HOST="$MQTT_HOST"
  SHARED_CONN_MQTT_PORT="$MQTT_PORT"
  SHARED_CONN_MQTT_USERNAME="$MQTT_USERNAME"
  SHARED_CONN_MQTT_PASSWORD="$MQTT_PASSWORD"

  generate_self_signed_cert "${role_dir}/data/map/tls" "map" MAP_TLS_EXTRA_SAN

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# UDP listener -- message-processor's MAP_UDP_HOST/MAP_UDP_PORT must point
# at this host, on this same port.
MAP_LISTEN_HOST=${MAP_LISTEN_HOST}
MAP_LISTEN_PORT=${MAP_LISTEN_PORT}

# REST (\`GET /api/flights\`) + WebSocket (\`/ws\`), same port.
MAP_HTTP_HOST=${MAP_HTTP_HOST}
MAP_HTTP_PORT=${MAP_HTTP_PORT}

# Dedicated Redis (map-redis, in docker-compose.map.yaml) -- never core
# Redis. MAP_REDIS_PASSWORD is optional; leave blank to match map-redis's
# no-auth default.
MAP_REDIS_HOST=${MAP_REDIS_HOST}
MAP_REDIS_PORT=${MAP_REDIS_PORT}
MAP_REDIS_PASSWORD=${MAP_REDIS_PASSWORD}

# Lifecycle TTLs, seconds. MAP_STALE_SECONDS < MAP_HIDE_SECONDS <
# MAP_EVICT_SECONDS must hold. MAP_EVICT_SECONDS should match this
# deployment's flight_ttl_seconds (core Redis's config:flight_ttl_seconds).
MAP_STALE_SECONDS=${MAP_STALE_SECONDS}
MAP_HIDE_SECONDS=${MAP_HIDE_SECONDS}
MAP_EVICT_SECONDS=${MAP_EVICT_SECONDS}

# Optional "center" reference point (on-map marker, initial camera position,
# "Return to center"). Leave both blank to disable.
MAP_CENTER_LATITUDE=${MAP_CENTER_LATITUDE}
MAP_CENTER_LONGITUDE=${MAP_CENTER_LONGITUDE}

# Optional -- leave MQTT_HOST blank to disable the Home Assistant presence
# (discovery + running version + start time). No telemetry is published
# either way.
MQTT_HOST=${MQTT_HOST}
MQTT_PORT=${MQTT_PORT}
MQTT_USERNAME=${MQTT_USERNAME}
MQTT_PASSWORD=${MQTT_PASSWORD}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

normalize_message_processor_id() {
  # Accepts either "skyfollower-message-processor-{id}" or a bare "{id}",
  # prints the bare id. Fails on anything that isn't a positive whole
  # number once the prefix is stripped.
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
  # IDs already holding a generated service block, one per line -- empty
  # (not an error) if the file doesn't exist yet. Lets a re-run only
  # append IDs it doesn't already find.
  local compose_file="$1"
  [ -f "$compose_file" ] || return 0
  grep -E '^  skyfollower-message-processor-[0-9]+:' "$compose_file" 2>/dev/null \
    | sed -E 's/^  skyfollower-message-processor-([0-9]+):.*/\1/' || true
}

append_message_processor_service() {
  # References this file's own x-message-processor anchors -- YAML anchors
  # only resolve within the file that defines them, so this can't be a
  # second compose file merged in via COMPOSE_FILE.
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

existing_receiver_slugs() {
  # Name-slugs already holding a generated service block, one per line --
  # empty (not an error) if the file doesn't exist yet.
  local compose_file="$1"
  [ -f "$compose_file" ] || return 0
  grep -E '^  skyfollower-receiver-[a-z0-9_-]+:' "$compose_file" 2>/dev/null \
    | sed -E 's/^  skyfollower-receiver-([a-z0-9_-]+):.*/\1/' || true
}

append_receiver_service() {
  # RECEIVER_NAME keeps the operator's original casing; the sanitized slug
  # is used only for the service/container name and data directory.
  local compose_file="$1" name="$2" sources="$3" slug
  slug="$(sanitize_identifier "$name")"
  cat >> "$compose_file" <<SERVICE_EOF

  skyfollower-receiver-${slug}:
    <<: *receiver
    container_name: skyfollower-receiver-${slug}
    volumes:
      - ./data/skyfollower-receiver-${slug}:/app/data
    environment:
      <<: *receiver-environment
      RECEIVER_NAME: ${name}
      RECEIVER_SOURCES: "${sources}"
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
        else
          echo "    Must be skyfollower-message-processor-{id} or a bare positive whole-number id." >&2
        fi
      done
    fi
    # Only non-interactive mode can reach here with norm_id still unset (a
    # validation failure recorded a problem instead of exiting immediately)
    # -- must not append a malformed empty-id service block meanwhile.
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
      # Full abort: nothing collected so far has been written to disk yet.
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
  # when the array has zero elements (the non-interactive malformed-ID
  # path above deliberately leaves it empty).
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
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(shared_conn_default "$env_file" RABBITMQ_HOST SHARED_CONN_RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(shared_conn_default "$env_file" RABBITMQ_PORT SHARED_CONN_RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(shared_conn_default "$env_file" RABBITMQ_USERNAME SHARED_CONN_RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(resolve_core_shared_password CORE_RABBITMQ_PASSWORD RABBITMQ_PASSWORD "RabbitMQ password" "$env_file" SHARED_CONN_RABBITMQ_PASSWORD)"
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$(shared_conn_default "$env_file" REDIS_HOST SHARED_CONN_REDIS_HOST)")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(shared_conn_default "$env_file" REDIS_PORT SHARED_CONN_REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file" SHARED_CONN_REDIS_PASSWORD)"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(shared_conn_default "$env_file" MQTT_HOST SHARED_CONN_MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(shared_conn_default "$env_file" MQTT_PORT SHARED_CONN_MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(shared_conn_default "$env_file" MQTT_USERNAME SHARED_CONN_MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(shared_conn_default "$env_file" MQTT_PASSWORD SHARED_CONN_MQTT_PASSWORD)" 0)"
  # Optional live position/metadata UDP feed toward the map role -- leave
  # MAP_UDP_HOST blank to disable. Its suggested default matches
  # collect_map_env()'s MAP_LISTEN_PORT default. Not probed with
  # probe_tcp: it's a UDP destination, and a TCP connect attempt would
  # misleadingly report "unreachable" even when correctly configured.
  MAP_UDP_HOST="$(prompt_string MAP_UDP_HOST "Map UDP destination host (leave blank to disable)" "$(existing_env_value "$env_file" MAP_UDP_HOST)" 0)"
  MAP_UDP_PORT="$(prompt_int_range MAP_UDP_PORT "Map UDP destination port" "$(existing_env_value_or "$env_file" MAP_UDP_PORT 30500)" 1 65535)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  # Stashed so a later non-core role in this run defaults to it instead of
  # its own empty .env (see shared_conn_default()).
  SHARED_CONN_RABBITMQ_HOST="$RABBITMQ_HOST"
  SHARED_CONN_RABBITMQ_PORT="$RABBITMQ_PORT"
  SHARED_CONN_RABBITMQ_USERNAME="$RABBITMQ_USERNAME"
  SHARED_CONN_RABBITMQ_PASSWORD="$RABBITMQ_PASSWORD"
  SHARED_CONN_REDIS_HOST="$REDIS_HOST"
  SHARED_CONN_REDIS_PORT="$REDIS_PORT"
  SHARED_CONN_REDIS_PASSWORD="$REDIS_PASSWORD"
  SHARED_CONN_MQTT_HOST="$MQTT_HOST"
  SHARED_CONN_MQTT_PORT="$MQTT_PORT"
  SHARED_CONN_MQTT_USERNAME="$MQTT_USERNAME"
  SHARED_CONN_MQTT_PASSWORD="$MQTT_PASSWORD"

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

# Optional -- leave MAP_UDP_HOST blank to disable this feed entirely.
# Must point at wherever the map role's own MAP_LISTEN_HOST/MAP_LISTEN_PORT
# are bound (see map/README.md's "Deliberately distinct variable names").
MAP_UDP_HOST=${MAP_UDP_HOST}
MAP_UDP_PORT=${MAP_UDP_PORT}

# "info" or "debug".
LOG_LEVEL=info
ENV_EOF
}

collect_archive_env() {
  local role_dir="$1" env_file="${1}/.env"
  echo "-- ${role_dir} (archive) --"
  # Offer to create/update the CloudFormation stack first, so its outputs
  # become the prompt defaults below. Declining falls through unchanged.
  offer_aws_provisioning archive "$env_file"
  S3_BUCKET="$(prompt_string S3_BUCKET "S3 archive bucket name" "${AWS_PROV_S3_BUCKET:-$(existing_env_value "$env_file" S3_BUCKET)}")"
  AWS_DEFAULT_REGION="$(prompt_string AWS_DEFAULT_REGION "AWS region" "${AWS_PROV_REGION:-$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)}")"
  # archive-processor and archive-compaction run under separate
  # least-privilege IAM identities, so each gets its own key pair.
  # S3_BUCKET and AWS_DEFAULT_REGION stay shared.
  ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID="$(prompt_string ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID "archive-processor AWS access key ID" "${AWS_PROV_ARCHIVE_PROCESSOR_KEY_ID:-$(existing_env_value "$env_file" ARCHIVE_PROCESSOR_AWS_ACCESS_KEY_ID)}")"
  ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY="$(prompt_password_value ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY "archive-processor AWS secret access key" "${AWS_PROV_ARCHIVE_PROCESSOR_SECRET:-$(existing_env_value "$env_file" ARCHIVE_PROCESSOR_AWS_SECRET_ACCESS_KEY)}")"
  ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID="$(prompt_string ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID "archive-compaction AWS access key ID" "${AWS_PROV_ARCHIVE_COMPACTION_KEY_ID:-$(existing_env_value "$env_file" ARCHIVE_COMPACTION_AWS_ACCESS_KEY_ID)}")"
  ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY="$(prompt_password_value ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY "archive-compaction AWS secret access key" "${AWS_PROV_ARCHIVE_COMPACTION_SECRET:-$(existing_env_value "$env_file" ARCHIVE_COMPACTION_AWS_SECRET_ACCESS_KEY)}")"
  RABBITMQ_HOST="$(prompt_string RABBITMQ_HOST "RabbitMQ host" "$(shared_conn_default "$env_file" RABBITMQ_HOST SHARED_CONN_RABBITMQ_HOST)")"
  RABBITMQ_PORT="$(prompt_int_range RABBITMQ_PORT "RabbitMQ port" "$(shared_conn_default "$env_file" RABBITMQ_PORT SHARED_CONN_RABBITMQ_PORT 5672)" 1 65535)"
  RABBITMQ_USERNAME="$(prompt_string RABBITMQ_USERNAME "RabbitMQ username" "$(shared_conn_default "$env_file" RABBITMQ_USERNAME SHARED_CONN_RABBITMQ_USERNAME skyfollower)")"
  RABBITMQ_PASSWORD="$(resolve_core_shared_password CORE_RABBITMQ_PASSWORD RABBITMQ_PASSWORD "RabbitMQ password" "$env_file" SHARED_CONN_RABBITMQ_PASSWORD)"
  REDIS_HOST="$(prompt_string REDIS_HOST "Redis host" "$(shared_conn_default "$env_file" REDIS_HOST SHARED_CONN_REDIS_HOST)")"
  REDIS_PORT="$(prompt_int_range REDIS_PORT "Redis port" "$(shared_conn_default "$env_file" REDIS_PORT SHARED_CONN_REDIS_PORT 6379)" 1 65535)"
  REDIS_PASSWORD="$(resolve_core_shared_password CORE_REDIS_PASSWORD REDIS_PASSWORD "Redis password" "$env_file" SHARED_CONN_REDIS_PASSWORD)"
  MQTT_HOST="$(prompt_string MQTT_HOST "MQTT broker host" "$(shared_conn_default "$env_file" MQTT_HOST SHARED_CONN_MQTT_HOST)")"
  MQTT_PORT="$(prompt_int_range MQTT_PORT "MQTT port" "$(shared_conn_default "$env_file" MQTT_PORT SHARED_CONN_MQTT_PORT 1883)" 1 65535)"
  MQTT_USERNAME="$(prompt_string MQTT_USERNAME "MQTT username" "$(shared_conn_default "$env_file" MQTT_USERNAME SHARED_CONN_MQTT_USERNAME)" 0)"
  MQTT_PASSWORD="$(prompt_password_value MQTT_PASSWORD "MQTT password" "$(shared_conn_default "$env_file" MQTT_PASSWORD SHARED_CONN_MQTT_PASSWORD)" 0)"
  probe_tcp "$RABBITMQ_HOST" "$RABBITMQ_PORT" "RabbitMQ"
  probe_tcp "$REDIS_HOST" "$REDIS_PORT" "Redis"
  probe_tcp "$MQTT_HOST" "$MQTT_PORT" "MQTT"

  # Stashed so a later non-core role in this run (management-ui) defaults
  # to it instead of its own empty .env (see shared_conn_default()).
  SHARED_CONN_RABBITMQ_HOST="$RABBITMQ_HOST"
  SHARED_CONN_RABBITMQ_PORT="$RABBITMQ_PORT"
  SHARED_CONN_RABBITMQ_USERNAME="$RABBITMQ_USERNAME"
  SHARED_CONN_RABBITMQ_PASSWORD="$RABBITMQ_PASSWORD"
  SHARED_CONN_REDIS_HOST="$REDIS_HOST"
  SHARED_CONN_REDIS_PORT="$REDIS_PORT"
  SHARED_CONN_REDIS_PASSWORD="$REDIS_PASSWORD"
  SHARED_CONN_MQTT_HOST="$MQTT_HOST"
  SHARED_CONN_MQTT_PORT="$MQTT_PORT"
  SHARED_CONN_MQTT_USERNAME="$MQTT_USERNAME"
  SHARED_CONN_MQTT_PASSWORD="$MQTT_PASSWORD"

  write_env_header "$env_file" "$role_dir"
  cat >> "$env_file" <<ENV_EOF

# The archive bucket, shared by both archive services.
S3_BUCKET=${S3_BUCKET}
AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}

# archive-processor and archive-compaction each authenticate as their own
# least-privilege IAM identity (both issued by the aws-setup CloudFormation
# stack -- see docs/aws-configuration.md). docker-compose
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
  # umask, not a chmod afterwards: never briefly world-readable.
  (
    umask 077
    cat > "$env_file" <<ENV_EOF
# Host-specific values for this SkyFollower deployment, read automatically
# by docker compose from this directory. Written by scripts/install.sh;
# re-run it (or edit this file directly) and \`docker compose up -d\` to
# change any of them.

# Tag every ghcr.io/brentio/skyfollower-* image resolves to. install.sh
# --upgrade rewrites this to the latest release (or to dev-<branch> when
# run with branch=<name>) and pulls it; set it to an older release tag and
# re-run \`docker compose up -d\` to roll back.
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
  # Every role's install folder is a fixed, boring name matching the role.
  echo "$1"
}

project_name_for_folder() {
  # Mirrors Compose's own project-name derivation, sanitized. Must agree
  # with the receiver/message-processor compose files' own top-level
  # `name:` (skyfollower-<folder>), which it does by construction.
  local folder_name="$1"
  local sanitized
  sanitized="$(sanitize_identifier "$folder_name")"
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

# For a dev build the image tag (dev-<branch>) is floating, so pull first
# every run or a re-run silently keeps a stale local image. --profile
# runners refreshes the core host's runner-* images too; NOT passed to
# `up -d` (would launch every one-shot runner). A release pin is
# immutable, so a plain release install skips the pull.
compose_bring_up() {
  local role_dir="$1"
  if [ "$DEV_BUILD" -eq 1 ]; then
    (cd "$role_dir" && docker compose --profile runners pull && docker compose up -d)
  else
    (cd "$role_dir" && docker compose up -d)
  fi
}

offer_up() {
  local role="$1" role_dir="$2"
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    compose_bring_up "$role_dir"
    return
  fi
  local answer prompt="docker compose up -d in ${role_dir}"
  [ "$DEV_BUILD" -eq 1 ] && prompt="docker compose pull && up -d in ${role_dir}"
  read -r -p "Bring ${role} up now (${prompt})? [Y/n]: " answer </dev/tty
  if [ -z "$answer" ] || [[ "$answer" =~ ^[Yy] ]]; then
    compose_bring_up "$role_dir"
  fi
}

provision_rabbitmq_users() {
  # RabbitMQ's RABBITMQ_DEFAULT_USER/PASS always creates that user as a
  # full administrator on `/`; there's no env var that scopes it from the
  # start. This runs once the container is up, demoting the application
  # user to SkyFollower's own resources and creating a separate dashboard
  # administrator, so a compromised host only ever holds a credential
  # scoped to known queue names. Every rabbitmqctl call is idempotent, so
  # re-running this is always safe.
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

  # configure/write/read, in that order. Keep this pattern in sync by hand
  # with shared/rabbitmq_topology.py's SKYFOLLOWER_RABBITMQ_RESOURCE_PATTERN
  # -- core-health filters RabbitMQ's queue list with that constant, so a
  # change here not mirrored there silently drifts what SkyFollower "owns".
  if (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_user_tags "$rabbitmq_username") \
    && (cd "$role_dir" && docker compose exec -T rabbitmq rabbitmqctl set_permissions --vhost / "$rabbitmq_username" \
      '^(skyfollower-adsb.*|skyfollower-message-processor-.*|skyfollower-archive|skyfollower-archive-raw-frames|amq\.default)$' '^(skyfollower-adsb.*|skyfollower-message-processor-.*|skyfollower-archive|skyfollower-archive-raw-frames|amq\.default)$' '^(skyfollower-adsb.*|skyfollower-message-processor-.*|skyfollower-archive|skyfollower-archive-raw-frames)$'); then
    echo "  ✓ ${rabbitmq_username}: no tags, scoped to SkyFollower's own resources"
  else
    echo "  ✗ Could not scope ${rabbitmq_username}'s tags/permissions -- check manually." >&2
  fi

  # add_user fails if the user already exists; list_users first so a
  # re-run doesn't print a scary error.
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
  # alone grants Management API visibility into every vhost/queue's
  # stats -- no per-resource permission is possible, so this is set to
  # match nothing rather than left at a freshly add_user'd account's
  # all-matching default.
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

# ---------------------------------------------------------------------------
# AWS provisioning (archive + management-ui hosts)
# ---------------------------------------------------------------------------

# Pulls one KEY=value line out of the aws-setup container's stdout.
# `|| true` so a legitimately absent key doesn't take the script down
# under `set -e`.
aws_setup_output_value() {
  local outputs="$1" key="$2"
  printf '%s\n' "$outputs" | grep -E "^${key}=" | tail -1 | cut -d= -f2- || true
}

# AWS_PROV_* carry the stack outputs from one provisioning run across every
# AWS-consuming role in the same run. AWS_PROV_DONE guards the single
# elevated-credential prompt: once that interaction has happened, no later
# role re-prompts. Initialised once before the role loop, blanked at the end.
init_aws_prov_globals() {
  AWS_PROV_DONE=0
  AWS_PROV_S3_BUCKET=""
  AWS_PROV_REGION=""
  AWS_PROV_ARCHIVE_PROCESSOR_KEY_ID=""
  AWS_PROV_ARCHIVE_PROCESSOR_SECRET=""
  AWS_PROV_ARCHIVE_COMPACTION_KEY_ID=""
  AWS_PROV_ARCHIVE_COMPACTION_SECRET=""
  AWS_PROV_MANAGEMENT_UI_KEY_ID=""
  AWS_PROV_MANAGEMENT_UI_SECRET=""
}
clear_aws_prov_globals() { init_aws_prov_globals; }

# After a successful one-time-IAM-user provisioning run, offers to delete
# that user (key, inline policy, user) with its own still-valid
# credentials. A no-op on the paste-a-session path (bootstrap_user empty).
offer_bootstrap_user_cleanup() {
  local image="$1" bootstrap_user="$2" key_id="$3" secret="$4" region="$5"
  [ -n "$bootstrap_user" ] || return 0

  local answer
  echo
  echo "  The one-time IAM user '${bootstrap_user}' has served its purpose."
  read -r -p "  Delete it now (key, inline policy, and user)? [Y/n]: " answer </dev/tty
  if [ -n "$answer" ] && ! [[ "$answer" =~ ^[Yy] ]]; then
    echo "  Left in place. Delete it later with:" >&2
    echo "    docker run --rm -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \\" >&2
    echo "      -e AWS_DEFAULT_REGION=${region} ${image} --delete-bootstrap-user ${bootstrap_user}" >&2
    return 0
  fi

  if docker run --rm \
      -e AWS_ACCESS_KEY_ID="$key_id" \
      -e AWS_SECRET_ACCESS_KEY="$secret" \
      -e AWS_DEFAULT_REGION="$region" \
      "$image" --delete-bootstrap-user "$bootstrap_user"; then
    echo "  ✓ One-time IAM user removed."
  else
    echo "  ✗ The one-time IAM user was not fully removed -- see the steps above." >&2
  fi
}

# Offers to create/update the archive's CloudFormation stack (archive
# role) or read an already-deployed stack's outputs (management-ui role),
# via a one-shot `docker run --rm ghcr.io/.../skyfollower-aws-setup`. Every
# failure path falls through to the manual AWS prompts unchanged.
#
# Prompts for the elevated provisioning credential exactly once per run:
# either an existing temporary session, or a printed least-privilege
# policy plus console steps to create a one-time IAM user (offered
# cleanup once provisioning succeeds).
#
# On success it sets AWS_PROV_* globals that collect_archive_env() /
# collect_management_ui_env() use as prompt defaults. It never writes the
# elevated credentials anywhere -- passed only as environment to that
# single --rm run.
offer_aws_provisioning() {
  local role="$1" env_file="$2"

  # Non-interactive runs read every AWS value straight from the
  # environment -- no container step, no prompting.
  if [ "$NON_INTERACTIVE" -eq 1 ]; then
    return 0
  fi

  # The one provisioning interaction for this run already happened (an
  # earlier role) -- reuse whatever AWS_PROV_* it captured.
  if [ "${AWS_PROV_DONE:-0}" = "1" ]; then
    return 0
  fi

  echo
  echo "  This role needs AWS infrastructure (Glue table, Athena workgroup, IAM identities)."
  local answer
  read -r -p "  Create or update it now? [Y/n]: " answer </dev/tty
  if [ -n "$answer" ] && ! [[ "$answer" =~ ^[Yy] ]]; then
    AWS_PROV_DONE=1
    return 0
  fi

  if ! command -v docker >/dev/null 2>&1; then
    echo "  ✗ docker not found on PATH -- skipping provisioning; you'll be prompted for the AWS values manually." >&2
    AWS_PROV_DONE=1
    return 0
  fi

  # From here on this is the single provisioning interaction: no later role
  # re-prompts, even if this one falls back to manual AWS prompts.
  AWS_PROV_DONE=1

  local image="ghcr.io/brentio/skyfollower-aws-setup:${IMAGE_VERSION}"
  # dev-<branch> is a floating tag; `docker run` alone won't refresh an
  # image already present locally.
  if [ "$DEV_BUILD" -eq 1 ]; then
    docker pull "$image" >/dev/null 2>&1 || true
  fi
  local prov_key_id prov_secret prov_token prov_region prov_prefix=""
  local prov_bucket="" prov_create="" bootstrap_user="" cred_choice

  echo
  echo "  Provisioning needs an elevated AWS credential (CloudFormation, S3, Glue, Athena, IAM)."
  echo "  How do you want to supply it?"
  echo "    1) Paste an existing temporary session (AWS access portal / SSO)."
  echo "    2) Create a one-time IAM user now -- the installer prints a"
  echo "       least-privilege policy and the console steps, and offers to"
  echo "       delete the user again once provisioning succeeds."
  read -r -p "  Choose [1/2]: " cred_choice </dev/tty

  if [ "$cred_choice" = "2" ]; then
    # Ask region/prefix first so the printed caller policy has no
    # placeholders left; the bucket too, so the deploy below has
    # everything it needs in one pass.
    echo
    prov_region="$(prompt_string AWS_DEFAULT_REGION "AWS region" "$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)")"
    prov_prefix="$(prompt_string RESOURCE_NAME_PREFIX "Resource name prefix (for the stack's IAM identity names)" "skyfollower")"
    if [ "$role" = "management-ui" ]; then
      prov_bucket="$(prompt_string S3_BUCKET "S3 archive bucket name (the one the archive host provisioned)" "$(existing_env_value "$env_file" S3_BUCKET)")"
      prov_create="No"
    else
      prov_bucket="$(prompt_string S3_BUCKET "S3 archive bucket name" "$(existing_env_value "$env_file" S3_BUCKET)")"
      read -r -p "  Create this bucket? [Y/n]: " answer </dev/tty
      if [ -z "$answer" ] || [[ "$answer" =~ ^[Yy] ]]; then prov_create="Yes"; else prov_create="No"; fi
    fi
    bootstrap_user="${prov_prefix}-bootstrap"

    local policy_json
    if ! policy_json="$(docker run --rm \
        -e AWS_DEFAULT_REGION="$prov_region" \
        -e ARCHIVE_BUCKET_NAME="$prov_bucket" \
        -e RESOURCE_NAME_PREFIX="$prov_prefix" \
        -e BOOTSTRAP_USER_NAME="$bootstrap_user" \
        "$image" --print-bootstrap-policy)"; then
      echo "  ✗ Could not render the caller policy. Falling back to manual prompts." >&2
      return 0
    fi

    echo
    echo "  ----------------------------------------------------------------------"
    echo "  One-time setup in the AWS console (https://console.aws.amazon.com/iam):"
    echo
    echo "    1. Access management -> Users -> Create user. Name it exactly: ${bootstrap_user}"
    echo "       Do NOT enable console access."
    echo "    2. On 'Set permissions' pick 'Attach policies directly' and leave it"
    echo "       empty -- don't attach anything yet. Finish creating the user."
    echo "    3. Open ${bootstrap_user} -> Permissions tab -> Add permissions ->"
    echo "       Create inline policy -> JSON tab, and paste this verbatim:"
    echo
    printf '%s\n' "$policy_json" | sed 's/^/         /'
    echo
    echo "       Name it (e.g. ${bootstrap_user}-policy) and create the policy."
    echo "    4. Open ${bootstrap_user} -> Security credentials -> Create access key"
    echo "       -> 'Application running outside AWS'. Copy the key ID and secret."
    echo "    5. Paste them below. A plain IAM user's key needs no session token."
    echo "  ----------------------------------------------------------------------"
    echo
    prov_key_id="$(prompt_string AWS_PROVISIONING_ACCESS_KEY_ID "AWS access key ID" "")"
    prov_secret="$(prompt_password_value AWS_PROVISIONING_SECRET_ACCESS_KEY "AWS secret access key" "")"
    prov_token=""
  else
    echo
    echo "  Paste temporary AWS credentials with permission to create these resources"
    echo "  (access key + secret + session token, as copied from the AWS access portal)."
    echo "  These are used for this one step only and are never saved."
    prov_key_id="$(prompt_string AWS_PROVISIONING_ACCESS_KEY_ID "AWS access key ID" "")"
    prov_secret="$(prompt_password_value AWS_PROVISIONING_SECRET_ACCESS_KEY "AWS secret access key" "")"
    prov_token="$(prompt_password_value AWS_PROVISIONING_SESSION_TOKEN "AWS session token" "")"
    # Must be prompted before any stack lookup -- can't come from the
    # stack's own AwsRegion output when finding the stack needs it first.
    prov_region="$(prompt_string AWS_DEFAULT_REGION "AWS region" "$(existing_env_value_or "$env_file" AWS_DEFAULT_REGION us-east-1)")"
  fi

  # boto3 rejects an empty-string AWS_SESSION_TOKEN rather than ignoring
  # it, so only pass it when non-empty.
  local cred_args=(
    -e AWS_ACCESS_KEY_ID="$prov_key_id"
    -e AWS_SECRET_ACCESS_KEY="$prov_secret"
    -e AWS_DEFAULT_REGION="$prov_region"
  )
  [ -n "$prov_token" ] && cred_args+=(-e AWS_SESSION_TOKEN="$prov_token")

  local outputs=""

  if [ "$role" = "management-ui" ]; then
    echo "  → Reading the archive stack's outputs..."
    if ! outputs="$(docker run --rm "${cred_args[@]}" "$image" --outputs-only)"; then
      echo "  ✗ Could not read the stack outputs -- is the archive host provisioned yet? Falling back to manual prompts." >&2
      _bootstrap_user_retry_hint "$bootstrap_user" "$image" "$prov_region"
      return 0
    fi
  else
    if [ -z "$prov_bucket" ]; then
      prov_bucket="$(prompt_string S3_BUCKET "S3 archive bucket name" "$(existing_env_value "$env_file" S3_BUCKET)")"
      read -r -p "  Create this bucket? [Y/n]: " answer </dev/tty
      if [ -z "$answer" ] || [[ "$answer" =~ ^[Yy] ]]; then prov_create="Yes"; else prov_create="No"; fi
    fi
    # RESOURCE_NAME_PREFIX is only passed for a non-default value, so the
    # array starts non-empty (safe under `set -u` on bash 3.2; see the
    # note in collect_message_processor_env about zero-element arrays).
    local -a deploy_args=(-e ARCHIVE_BUCKET_NAME="$prov_bucket" -e CREATE_ARCHIVE_BUCKET="$prov_create")
    if [ -n "$prov_prefix" ] && [ "$prov_prefix" != "skyfollower" ]; then
      deploy_args+=(-e "RESOURCE_NAME_PREFIX=$prov_prefix")
    fi
    echo "  → Deploying CloudFormation stack 'skyfollower' (this can take a few minutes)..."
    outputs="$(docker run --rm "${cred_args[@]}" "${deploy_args[@]}" "$image")" || true
    if [ -z "$outputs" ]; then
      echo "  ✗ Provisioning failed -- see the output above. Falling back to manual prompts." >&2
      _bootstrap_user_retry_hint "$bootstrap_user" "$image" "$prov_region"
      return 0
    fi
    echo "  ✓ Stack deployed."
  fi

  offer_bootstrap_user_cleanup "$image" "$bootstrap_user" "$prov_key_id" "$prov_secret" "$prov_region"

  AWS_PROV_S3_BUCKET="$(aws_setup_output_value "$outputs" ArchiveBucketName)"
  AWS_PROV_REGION="$(aws_setup_output_value "$outputs" AwsRegion)"
  AWS_PROV_ARCHIVE_PROCESSOR_KEY_ID="$(aws_setup_output_value "$outputs" ArchiveProcessorAccessKeyId)"
  AWS_PROV_ARCHIVE_PROCESSOR_SECRET="$(aws_setup_output_value "$outputs" ArchiveProcessorSecretAccessKey)"
  AWS_PROV_ARCHIVE_COMPACTION_KEY_ID="$(aws_setup_output_value "$outputs" ArchiveCompactionAccessKeyId)"
  AWS_PROV_ARCHIVE_COMPACTION_SECRET="$(aws_setup_output_value "$outputs" ArchiveCompactionSecretAccessKey)"
  AWS_PROV_MANAGEMENT_UI_KEY_ID="$(aws_setup_output_value "$outputs" ManagementUiAccessKeyId)"
  AWS_PROV_MANAGEMENT_UI_SECRET="$(aws_setup_output_value "$outputs" ManagementUiSecretAccessKey)"
  # Region prompt already succeeded; keep the operator's choice if the
  # stack output somehow came back blank.
  [ -n "$AWS_PROV_REGION" ] || AWS_PROV_REGION="$prov_region"
  echo "  ✓ AWS values captured -- the prompts below are pre-filled; press Enter to accept."
}

# Printed after a failed provisioning run on the one-time-user path: the
# user still exists, so keep it to retry or delete it once done.
_bootstrap_user_retry_hint() {
  local bootstrap_user="$1" image="$2" region="$3"
  [ -n "$bootstrap_user" ] || return 0
  echo "  Note: the one-time IAM user '${bootstrap_user}' still exists -- keep it to" >&2
  echo "  retry, or delete it once you're done:" >&2
  echo "    docker run --rm -e AWS_ACCESS_KEY_ID=... -e AWS_SECRET_ACCESS_KEY=... \\" >&2
  echo "      -e AWS_DEFAULT_REGION=${region} ${image} --delete-bootstrap-user ${bootstrap_user}" >&2
}

# Runs the first-time bulk-load runner sequence detached, so accepting
# doesn't require keeping the installer's session open for however long
# the full run takes, and doesn't block the later offer_up prompts.
#
# The generated script deletes itself as its last line; `nohup` survives
# the installer's session ending, `disown` drops it from this shell's job
# table. Output goes to a log file under role_dir the operator can tail.
run_bulk_load_detached() {
  local role_dir="$1" ordered="$2"
  local log_file script_file
  log_file="${role_dir}/bulk-load-$(date +%Y%m%dT%H%M%S).log"
  script_file="$(mktemp)"

  {
    printf '#!/usr/bin/env bash\n'
    printf 'cd %q || exit 1\n' "$role_dir"
    local r
    for r in $ordered; do
      printf 'echo "$(date "+%%Y-%%m-%%d %%H:%%M:%%S") Running %s..."\n' "$r"
      printf 'docker compose run --rm %q || echo "$(date "+%%Y-%%m-%%d %%H:%%M:%%S")   %s failed -- continuing with the rest."\n' "$r" "$r"
    done
    printf 'echo "$(date "+%%Y-%%m-%%d %%H:%%M:%%S") Bulk load complete."\n'
    printf 'rm -f %q\n' "$script_file"
  } > "$script_file"

  nohup bash "$script_file" >>"$log_file" 2>&1 </dev/null &
  disown

  echo "Bulk load started in the background -- it will keep running after this"
  echo "installer moves on or exits."
  echo "  Log:      ${log_file}"
  echo "  Progress: tail -f ${log_file}"
  echo "  Status:   (cd ${role_dir} && docker compose ps)"
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
    echo "its index -- then the rest alphabetically, then cz-caa-registry just"
    echo "before uk-caa-registry last, since both do slow per-record detail"
    echo "fetches). Otherwise each runs on its own schedule and Redis fills"
    echo "in gradually. This can take hours end to end, so it runs detached"
    echo "in the background -- the installer moves on immediately and this"
    echo "session does not need to stay open for it to finish."
    read -r -p "Run the bulk load now? [y/N]: " answer </dev/tty
  fi
  if [ -z "$answer" ] || ! [[ "$answer" =~ ^[Yy] ]]; then
    return
  fi

  # The runner list comes from `docker compose config --services`, not a
  # hardcoded list, so it can't drift from what's declared. `--profile
  # runners` is required or the list comes back empty. Every grep is
  # guarded with `|| true` since a legitimate no-match would otherwise
  # take the script down under `set -e`. cz-caa-registry/uk-caa-registry
  # are pulled out of the alphabetical batch and appended last: both do
  # slow per-record detail fetches.
  local all_runners mictronics rest cz_second_last uk_last ordered
  all_runners="$(cd "$role_dir" && docker compose --profile runners config --services | grep '^runner-' || true)"
  mictronics="$(echo "$all_runners" | grep '^runner-mictronics$' || true)"
  cz_second_last="$(echo "$all_runners" | grep '^runner-cz-caa-registry$' || true)"
  uk_last="$(echo "$all_runners" | grep '^runner-uk-caa-registry$' || true)"
  rest="$(echo "$all_runners" | grep -v '^runner-mictronics$' | grep -v '^runner-cz-caa-registry$' | grep -v '^runner-uk-caa-registry$' | sort || true)"
  ordered="$(printf '%s\n%s\n%s\n%s\n' "$mictronics" "$rest" "$cz_second_last" "$uk_last" | grep -v '^$' || true)"

  run_bulk_load_detached "$role_dir" "$ordered"
}

# ---------------------------------------------------------------------------
# Upgrade mode
# ---------------------------------------------------------------------------

do_upgrade() {
  # REF/IMAGE_VERSION are already resolved by main() before dispatching
  # here -- resolving again would mean two GitHub API calls for the same
  # answer.
  echo "Upgrading every role directory under ${INSTALL_ROOT} to ${IMAGE_VERSION}..."
  echo "(runner-* images are pulled too -- they sit behind the \"runners\" compose profile)"
  local found=0
  for env_file in "${INSTALL_ROOT}"/*/.env; do
    [ -e "$env_file" ] || continue
    found=1
    local role_dir
    role_dir="$(dirname "$env_file")"
    echo
    echo "-- ${role_dir} --"
    # Re-fetch this role's compose file (and any config/*.example) via
    # fetch_role, same as a first install -- an upgrade that only pulls
    # images can never deliver a new service, label, or port mapping to
    # an existing deployment. basename is a reliable way back to the role
    # fetch_role expects, since default_folder_for_role() guarantees
    # folder name == role name. fetch_role's own no-clobber logic applies
    # unchanged here.
    fetch_role "$(basename "$role_dir")" "$role_dir"
    # Rewrite SKYFOLLOWER_VERSION in place; every other line, including
    # operator edits, is left as-is. Also renames the map role's old
    # MAP_HOME_LATITUDE/MAP_HOME_LONGITUDE keys to MAP_CENTER_LATITUDE/
    # MAP_CENTER_LONGITUDE -- a no-op on every non-map role dir.
    local tmp
    tmp="$(mktemp)"
    awk -v v="$IMAGE_VERSION" '
      /^SKYFOLLOWER_VERSION=/ { print "SKYFOLLOWER_VERSION=" v; next }
      /^MAP_HOME_LATITUDE=/ { sub(/^MAP_HOME_LATITUDE=/, "MAP_CENTER_LATITUDE="); print; next }
      /^MAP_HOME_LONGITUDE=/ { sub(/^MAP_HOME_LONGITUDE=/, "MAP_CENTER_LONGITUDE="); print; next }
      { print }
    ' "$env_file" > "$tmp"
    (umask 077; mv "$tmp" "$env_file")
    # --profile runners on the pull refreshes the runner-* images too (a
    # no-op where the compose file declares no such profile). Deliberately
    # NOT passed to `up -d`: the runner services are one-shot jobs, so
    # `up` would kick off every one of them, including the multi-hour
    # uk-caa-registry. Ofelia is recreated by `up -d` and spawns fresh
    # runner containers from the pulled images on its own schedule.
    (cd "$role_dir" && docker compose --profile runners pull && docker compose up -d)
  done
  if [ "$found" -eq 0 ]; then
    echo "No role directories found under ${INSTALL_ROOT} (looked for */.env)." >&2
    exit 1
  fi
  echo
  echo "Upgrade complete."
  if [ "$DEV_BUILD" -eq 1 ]; then
    echo
    echo "⚠️  DEVELOPMENT BUILD (${IMAGE_VERSION}, branch '${BRANCH}') -- not a released version."
  fi
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

# Loud marker that this run is a dev build, not a release. Printed right
# after resolve_ref() (covers --upgrade too) and again in the summary.
print_dev_banner() {
  cat >&2 <<EOF

============================================================
        ⚠️   DEVELOPMENT BUILD   ⚠️   -- NOT A RELEASE
============================================================
  Branch (config + compose files) : ${BRANCH}
  Image tag (skyfollower-* images) : ${IMAGE_VERSION}
  SKYFOLLOWER_VERSION (.env)        : ${IMAGE_VERSION}
  Image VERSION / HA sw_version     : 9999.99.99
============================================================
EOF
}

# Only asked when --root wasn't explicitly passed and the run is
# interactive -- confirms the default before preflight's writability
# check and every later step depend on INSTALL_ROOT being right.
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
  [ "$DEV_BUILD" -eq 1 ] && print_dev_banner

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

  # Sort into ROLE_DEPENDENCY_ORDER: collect_core_env() must run before any
  # dependent role's collect_*_env (see resolve_core_shared_password()),
  # and archive must deploy its CloudFormation stack before
  # management-ui's collect_*_env reads its outputs.
  local reordered_roles=() want r
  for want in $ROLE_DEPENDENCY_ORDER; do
    for r in "${SELECTED_ROLES[@]}"; do
      [ "$r" = "$want" ] && reordered_roles+=("$r")
    done
  done
  SELECTED_ROLES=("${reordered_roles[@]}")

  mkdir -p "$INSTALL_ROOT"

  # One provisioning interaction per run; blanked again at the end.
  init_aws_prov_globals

  # Shared RabbitMQ/Redis/MQTT connection values collected by one non-core
  # role become the prompt defaults for the next; blanked again at the end.
  init_shared_conn_globals

  local installed_dirs=()
  local installed_roles=()

  for role in "${SELECTED_ROLES[@]}"; do
    local folder_name
    folder_name="$(default_folder_for_role "$role")"
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
      map) collect_map_env "$role_dir" ;;
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

  # Nothing elevated is left in the process environment once the run ends.
  clear_aws_prov_globals
  clear_shared_conn_globals

  echo
  echo "Summary:"
  i=0
  for role_dir in "${installed_dirs[@]}"; do
    echo "  ${installed_roles[$i]}: ${role_dir}"
    i=$((i+1))
  done
  if [ "$DEV_BUILD" -eq 1 ]; then
    echo
    echo "⚠️  DEVELOPMENT BUILD (${IMAGE_VERSION}, branch '${BRANCH}') -- not a released version."
  fi
}

main
