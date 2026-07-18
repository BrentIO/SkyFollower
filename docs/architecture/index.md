# Architecture

The full message pipeline, from ADS-B reception through to archived
flights in S3, including the RabbitMQ offline-fallback path at each hop.

[![SkyFollower message pipeline](./images/pipeline.svg)](./images/pipeline.svg)

## Scaling Processors

Each processor handles the subset of aircraft whose ICAO hex modulo
`PROCESSOR_COUNT` equals the processor's ID. To add a second processor:

1. Uncomment the `processor-1` block in `docker-compose.processor.yaml` and its volume entry.
2. Increment `processor_count` in the receiver's `settings.json` (e.g. `"processor_count": 2`).
3. Restart the receiver — it will pre-declare the new queue and begin routing to it.
4. Start the new processor container: `docker compose -f docker-compose.processor.yaml up -d`.

Each processor must run on a separate host (or at least with a unique
`PROCESSOR_ID`). The processor claims its ID in Redis on startup and exits if
the ID is already claimed by another instance.

**Increasing `PROCESSOR_COUNT` reshuffles every aircraft's target processor**
(`icao_hex % PROCESSOR_COUNT`), not just the newly-added capacity — it's a
modulo over the *new* count, so most aircraft that used to hash to an
existing processor now hash somewhere else. Existing processors will see
some of their in-flight flights go stale as traffic reroutes to the new
processor; those flights age out and archive normally via the crash-durable
eviction path (see Crash Recovery & Backlog Replay below), while the new
processor starts a fresh flight for that aircraft the next time it's seen.
This is an accepted split — the same category of fragmentation
`flight_ttl_seconds` already causes elsewhere — not data loss or
corruption. No special drain is required to resize upward safely, though
performing it during a quiet period will minimize the number of flights
that get split.

**Removing a processor** works the same way in reverse:

1. Stop the processor container(s) being removed.
2. Decrement `processor_count` in the receiver's `settings.json`, and
   comment out (or remove) the corresponding block/volume in
   `docker-compose.processor.yaml`.
3. Restart the receiver so the new routing takes effect.
4. Decommission the removed processor's container and volume.

Splitting an in-progress flight across the resize boundary is an accepted
trade-off, same as resizing up. The removed processor's residual
`active_flights.db` data does **not** need to be manually drained or
preserved before decommissioning — any flights it was still tracking are
simply abandoned; surviving processors will start fresh flights for those
aircraft under the new routing the next time they're seen.

*Future enhancement (not built): the archive-processor could eventually
detect and stitch together adjacent split-flight records (same `icao_hex`,
contiguous time ranges, matching aircraft/operator) produced by a
resize-down event.*

## Crash Recovery & Backlog Replay

The processor's active flight store is file-backed and survives a process
restart — deliberate or a crash — without losing in-progress flights. See
[Fault Tolerance](https://github.com/BrentIO/SkyFollower/blob/main/processor/README.md#fault-tolerance)
in the processor's README for the full behavior, including how recovery
avoids archiving flights just because wall-clock time passed while the
container was down.

[![Processor crash recovery and backlog replay](./images/crash-recovery-sequence.svg)](./images/crash-recovery-sequence.svg)
