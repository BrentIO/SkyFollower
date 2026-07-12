#!/usr/bin/env bash
#
# Bulk-loads every data runner once, e.g. right after a fresh install, so
# Redis isn't sitting empty until each runner's first scheduled ofelia run
# (up to a week away for weekly runners).
#
# Runners are discovered by directory listing (any data-runners/*/ with a
# main.py) rather than a hardcoded list, so adding a new runner directory
# needs no edit here (see #322). mictronics always runs first since most
# country runners resolve icao_hex against its RediSearch index; everything
# else runs in alphabetical order.
#
# Requires containers already created (but stopped) via:
#   docker compose -f ../docker-compose.server.yaml --profile runners up --no-start

set -uo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

runners=()
for dir in */; do
    name="${dir%/}"
    [[ -f "$name/main.py" && "$name" != "mictronics" ]] || continue
    runners+=("$name")
done
sorted=()
while IFS= read -r name; do
    sorted+=("$name")
done < <(printf '%s\n' "${runners[@]}" | sort)
ordered=(mictronics "${sorted[@]}")

failed=()
for name in "${ordered[@]}"; do
    echo "=== $name ==="
    docker start -a "skyfollower-runner-$name" || failed+=("$name")
done

echo
if [[ ${#failed[@]} -eq 0 ]]; then
    echo "All ${#ordered[@]} runners completed."
else
    echo "${#failed[@]} of ${#ordered[@]} runners failed: ${failed[*]}"
    exit 1
fi
