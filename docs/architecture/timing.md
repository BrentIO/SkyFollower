# Timing and Cadences

Every loop cadence, key TTL, I/O deadline, retry backoff and rolling window
SkyFollower depends on is a named constant in `shared/timing.py` — the
single place to read or change one. None of these are environment
variables. An operator cannot tune them from a `.env`, by design: they
govern internal behaviour that has one correct value across the whole
deployment, and several of them carry a cross-file invariant that a
divergent `.env` on one host would silently break.

The **one** timing value that stays adjustable is `flight_ttl_seconds`. It
is a genuine per-deployment behavioural tradeoff — too short fragments a
flight on a quick-turn aircraft, too long merges two — and it lives in the
`config:flight_ttl_seconds` Redis key, read once at startup by both the
message processor and the archive processor (which derives its
split-flight gap threshold from the same value). One Redis key cannot
drift the way two `.env` files can. Its fallback default is
`DEFAULT_FLIGHT_TTL_SECONDS` (300 s).

## Naming convention

`<SUBJECT>_<KIND>_SECONDS`, where `<KIND>` is one of:

| Kind | Meaning |
|---|---|
| `INTERVAL` | a recurring loop cadence |
| `TTL` | a key or record expiry |
| `TIMEOUT` | a single I/O deadline |
| `WINDOW` | a rolling span measured backwards from now |
| `BACKOFF` | the delay between retry attempts |

A few constants keep a domain term of art in place of one of those words
(`MAX_AGE`, `MAX_LAG`, `KEEPIDLE`, `KEEPINTVL`). Values that are counts
rather than durations omit the `_SECONDS` suffix
(`TCP_KEEPALIVE_PROBES`).

## The constants

| Constant | Value | What it governs |
|---|---|---|
| `HEALTHCHECK_INTERVAL_SECONDS` | 15 s | How often each long-running component rewrites its `/app/health/heartbeat` file while genuinely connected to its upstreams. One definition, shared by the receiver, message processor, archive processor and core-health. |
| `HEALTHCHECK_MAX_AGE_SECONDS` | 40 s | Docker's `HEALTHCHECK` treats the heartbeat file as stale — and the container unhealthy — once it is older than this. A shade under three write intervals, so one missed write is jitter and two in a row is not. Import-time assertion: must exceed `2 × HEALTHCHECK_INTERVAL_SECONDS`. |
| `MQTT_PUBLISH_INTERVAL_SECONDS` | 30 s | Cadence at which the receiver, message processor and archive processor publish their MQTT statistic topics. Purely time-based — no component publishes early on a message-count trigger. |
| `HEARTBEAT_INTERVAL_SECONDS` | 30 s | How often the receiver and message processor refresh the TTL on their Redis identity/registration key. |
| `HEARTBEAT_TTL_SECONDS` | 60 s | TTL set on that key — twice the refresh interval, so a single missed refresh never drops the claim. Named outright rather than computed as `interval × 2` at each call site. |
| `CONFIG_POLL_INTERVAL_SECONDS` | 30 s | How often the message processor polls `config:rules:version` / `config:areas:version` and reloads on a change. |
| `RECONNECT_BACKOFF_SECONDS` | 10 s | Wait between reconnect attempts to RabbitMQ / Redis / S3 after a drop. Unifies what used to be a scatter of `sleep(10)` / `sleep(5)` literals. |
| `RECONNECT_COUNT_RESET_AGE_SECONDS` | 30 s | How long a receiver source connection must stay continuously up before a reconnect resets that connection's accumulated `reconnect_count` to zero — so the metric reflects a current flapping episode, not one that ended long ago. 3× `RECONNECT_BACKOFF_SECONDS`. |
| `RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS` | 30 s | Deadline on the receiver's RabbitMQ connection sitting in the broker's blocked state — publishers halted by a resource alarm (disk-free / high memory) while the TCP connection stays up. pika tears the connection down once this elapses, so the receiver's sole publishing thread reconnects and re-validates instead of wedging inside a `basic_publish` that never returns. |
| `FALLBACK_RETRY_BACKOFF_SECONDS` | 30 s | Minimum spacing between drain attempts against one fallback-queue row, independent of how often the caller invokes `drain()` — keeps a flapping connection from burning the retry threshold in well under a real recovery window. |
| `RATE_WINDOW_SECONDS` | 30 s | Rolling window over which `_RateTracker` measures messages-per-second, in the receiver and the message processor. |
| `PARITY_ERROR_CONFIRM_WINDOW_SECONDS` | 30 s | Trailing window over which the message processor requires repeated sightings before trusting a reserved squawk, or an ident sourced from a message it could not CRC-verify. |
| `RULE_NOTIFICATION_MAX_LAG_SECONDS` | 30 s | Maximum age of the triggering message for a rule match to still be published to MQTT. An older match is still recorded in `matched_rules`, just not announced — the notification would no longer be actionable. |
| `RABBITMQ_POLL_INTERVAL_SECONDS` | 30 s | How often core-health polls RabbitMQ's HTTP management API. RabbitMQ aggregates its stats broker-side on a ~5 s internal interval, so this stays fresh without re-reading unchanged cached data. The RabbitMQ queue-depth HA entities set `expire_after` to `3 ×` this (90 s). |
| `REDIS_POLL_INTERVAL_SECONDS` | 30 s | How often core-health issues Redis `INFO` / `MEMORY STATS`. Redis's health signals don't move on a sub-minute timescale in ways that matter here. |
| `HTTP_TIMEOUT_SECONDS` | 10 s | Deadline on each core-health HTTP request to the RabbitMQ management API. |
| `TCP_KEEPIDLE_SECONDS` | 60 s | Idle time before the first TCP keepalive probe on a receiver source socket. |
| `TCP_KEEPINTVL_SECONDS` | 10 s | Interval between keepalive probes. |
| `TCP_KEEPALIVE_PROBES` | 3 | Unanswered probes before the kernel tears the connection down. Idle + interval × probes ≈ a 90 s detection budget for a peer that vanished without a clean FIN/RST — far below the ~2-hour OS default, without false-positiving on a legitimately quiet feed. |
| `UNPARSEABLE_WARNING_INTERVAL_SECONDS` | 60 s | Minimum spacing between "N unparseable lines" summary warnings, per source connection. |
| `STITCH_POINTER_TTL_SECONDS` | 86 400 s (1 day) | TTL on the `archive:last_segment:{icao_hex}` pointer used for split-flight stitching. |
| `ENRICHMENT_TTL_SECONDS` | 1 209 600 s (14 days) | TTL every data runner sets on the enrichment keys it writes — registration, operator, type, airport, livery. Long enough that a single missed weekly run never expires live data. |
| `ROUTE_TTL_SECONDS` | 259 200 s (3 days) | TTL the `vrs-standing-data` runner sets on `route:{ident}`. Deliberately shorter than `ENRICHMENT_TTL_SECONDS`: the upstream route data refreshes daily, so a 3-day ceiling keeps it from silently going stale for over a week if a run or two is missed. Import-time assertion: must be less than `ENRICHMENT_TTL_SECONDS`. |
| `RULE_TRIGGER_DAY_TTL_SECONDS` | 2 678 400 s (31 days) | TTL the message processor sets on each `rule_triggers:{identifier}:{YYYY-MM-DD}` day key. One day of margin past the 30-day window the management-ui backend sums for a rule's rolling-30-day trigger count, so a day key can't expire mid-read at the boundary. The lifetime key never expires. |
| `DEFAULT_FLIGHT_TTL_SECONDS` | 300 s | Fallback for `flight_ttl_seconds` when `config:flight_ttl_seconds` is unset. The one value behind the sole remaining operator knob — see above. |

## Component-local timing

Three values in the management-ui backend cross no component boundary and
stay in `management-ui/backend/main.py`, but follow the same convention:

| Constant | Value | What it governs |
|---|---|---|
| `ARCHIVE_SEARCH_TTL_SECONDS` | 7 days | How long an archive-search result record lives at `archive_search:{uuid}` in Redis. Must expire before the Athena results file it points at is aged out of the results bucket by that bucket's own lifecycle policy. |
| `ATHENA_POLL_DEADLINE_SECONDS` | 120 s | Overall deadline for polling one Athena query to completion. |
| `ATHENA_POLL_BACKOFF_SECONDS` | `[1, 2, 4, 8, 16]` | Per-attempt backoff schedule while polling an Athena query, capped at 30 s. |
