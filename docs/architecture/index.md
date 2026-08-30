# Architecture

The full message pipeline, from ADS-B reception through to archived
flights in S3, including the RabbitMQ offline-fallback path at each hop.

[![SkyFollower message pipeline](./images/pipeline.svg)](./images/pipeline.svg)

## Scaling Message Processors

Receivers publish every message to the durable `adsb` exchange, of type
`x-consistent-hash`, keyed by the aircraft's ICAO hex. Each message processor
declares and binds its own durable queue,
`skyfollower-message-processor-{MESSAGE_PROCESSOR_ID}`, with a weight of `1`.
The exchange hashes the routing key against the bound queues and delivers
each message to exactly one of them, so an aircraft stays with one message
processor and the per-aircraft flight state it holds.

**No receiver knows or cares how many message processors exist**, so no
resize ever restarts a receiver — which matters, because a receiver restart
is an ingest gap: it is reading live TCP streams, and whatever arrives while
it is down is gone.

`MESSAGE_PROCESSOR_ID` is a single flat, fleet-wide sequential number, and
that one ID is used verbatim — no local/global translation — as the compose
service name, container name, RabbitMQ queue name, Redis heartbeat key, and
data directory alike:
`skyfollower-message-processor-{id}` everywhere. `docker-compose.message-processor.yaml`,
as fetched from the repo, holds only the shared anchors every instance is
built from — no service definitions. `scripts/install.sh` generates the
concrete per-ID service list into each node's own copy of that file: it asks
whether this run is replacing an existing processor (adopts one specific ID)
or adding new ones (asks how many are currently implemented fleet-wide and
how many this host will add, then computes the new IDs as
`existing_count+1` through `existing_count+num_new`). To add a message
processor to an existing node, just re-run `scripts/install.sh` for that
role — it appends the new service block(s) without touching already-running
instances.

That is the entire procedure — no `COMPOSE_PROFILES` line to write, no block
to uncomment. Each instance gets its own bind-mounted data directory
(`./data/skyfollower-message-processor-{id}`), so no two share an active
flight store.

IDs are unique across the deployment by construction rather than by
convention: they're sequential and fleet-wide, decided by the operator
answering install.sh's prompts rather than derived from anything local to a
node (a hostname, in particular, would make replacing a node's hardware
awkward — the new node would compute a different value). Sequential, not
random, is deliberate: it preserves the ability to reason about creation
order, which the "remove the last-bound instance" scale-down guidance below
depends on. The message processor still claims its ID in Redis on startup
and exits if another instance already holds it.

Measured over 5,000 randomly generated ICAO hexes:

| Operation | Aircraft moving to a different message processor | Receiver restart |
|---|---|---|
| Add a message processor | ~20%, all onto the new one | None |
| Remove the last-bound message processor | ~20%, redistributed evenly | None |
| All message processors restart, any order | 0% | None |
| Remove a message processor from the middle | ~68% | None |

Slots inside the exchange are positional and assigned in binding order, which
is why removal must happen from the end: deleting a queue in the middle slides
every later queue down and rehashes most aircraft. Treat message processors as
a stack — add and remove at the end. A restart moves nothing at all, because
the binding is durable state held by RabbitMQ and rebinding an existing
binding is a no-op.

None of this is data loss. Nothing is dropped, and every message still routes
and processes. In-progress flights for reshuffled aircraft split: the old
message processor's partial state ages out after `flight_ttl_seconds` and
archives as one segment (via the crash-durable eviction path — see Crash
Recovery & Backlog Replay below), the new one starts another, and the archive
processor's split-flight stitching merges the adjacent segments. The cost is
degraded archive quality for a few minutes.

Stitching is keyed on a short-lived Redis pointer
(`archive:last_segment:{icao_hex}`, 1-day TTL) and fails soft: if Redis is
unreachable, the pointer lookup/update is skipped and the segments are simply
left unmerged rather than blocking the archive write. See
[Split-Flight Stitching](https://github.com/BrentIO/SkyFollower/blob/main/archive-processor/README.md#split-flight-stitching)
in the archive processor's README for the full behavior.

A decommissioned message processor's residual `active_flights.db` does not
need to be drained or preserved — any flights it was still tracking are
abandoned, and surviving message processors start fresh flights for those
aircraft the next time they are seen.

## Crash Recovery & Backlog Replay

The message processor's active flight store is file-backed and survives a
process restart — deliberate or a crash — without losing in-progress
flights. See
[Fault Tolerance](https://github.com/BrentIO/SkyFollower/blob/main/message-processor/README.md#fault-tolerance)
in the message processor's README for the full behavior, including how
recovery avoids archiving flights just because wall-clock time passed while
the container was down.

[![Message processor crash recovery and backlog replay](./images/crash-recovery-sequence.svg)](./images/crash-recovery-sequence.svg)

## Receiver — RabbitMQ Offline Fallback

Independent of whether a message processor is up, the receiver itself tolerates
RabbitMQ being unreachable. The threads reading the readsb sockets never
publish directly — they drop each parsed message on a bounded in-memory
queue and return straight to the socket, so intake is never blocked by the
broker or by backlog drain. A single dedicated thread publishes from that
queue, always draining live traffic before advancing the on-disk backlog by
one row; a publish that fails buffers to a local `queue.db` (SQLite WAL) and
the backlog drains oldest-first once RabbitMQ is reachable again.

[![Receiver — RabbitMQ offline fallback](./images/receiver-offline-fallback-sequence.svg)](./images/receiver-offline-fallback-sequence.svg)

## Message Processor — RabbitMQ Offline Fallback

Independent of the receiver, the message processor tolerates RabbitMQ
being unreachable on its own publish side (archiving completed flights):
publish attempts that fail buffer to a local `completed_flights.db`
(SQLite WAL) and drain oldest-first once RabbitMQ is reachable again — on
reconnect, and independently every `MQTT_PUBLISH_INTERVAL_SECONDS`, to catch
a publish-only failure that never dropped the underlying connection.

[![Message processor — RabbitMQ offline fallback](./images/message-processor-offline-fallback-sequence.svg)](./images/message-processor-offline-fallback-sequence.svg)
