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

Each processor must run on a separate host (or at least with a unique
`PROCESSOR_ID`). The processor claims its ID in Redis on startup and exits if
the ID is already claimed by another instance.

## Crash Recovery & Backlog Replay

The processor's active flight store is file-backed and survives a process
restart — deliberate or a crash — without losing in-progress flights. See
[Fault Tolerance](https://github.com/BrentIO/SkyFollower/blob/main/processor/README.md#fault-tolerance)
in the processor's README for the full behavior, including how recovery
avoids archiving flights just because wall-clock time passed while the
container was down.

[![Processor crash recovery and backlog replay](./images/crash-recovery-sequence.svg)](./images/crash-recovery-sequence.svg)
