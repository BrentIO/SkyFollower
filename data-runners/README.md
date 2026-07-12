# Data Runners

Each subdirectory is a self-contained data runner: download → normalize →
write to Redis with TTL → exit. Runners are scheduled on their own cadence
via the `ofelia` cron container (see `config/ofelia/config.ini`).

## Initial bulk load

On a fresh install, Redis stays empty for whichever runner's first
scheduled run is furthest away (up to a week) unless every runner is
triggered manually. `bootstrap.sh` does that:

```
docker compose -f ../docker-compose.server.yaml --profile runners up --no-start
data-runners/bootstrap.sh
```

It discovers runners by directory listing (any `data-runners/*/` containing
a `main.py`) — adding a new runner directory needs no edit here. `mictronics`
always runs first, since most country runners resolve `icao_hex` against its
RediSearch index; every other runner runs in alphabetical order. See #322.

A runner that fails doesn't block the rest of the bulk load; failures are
listed at the end and the script exits non-zero if any occurred.
