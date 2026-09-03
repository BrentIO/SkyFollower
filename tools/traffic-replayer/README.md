# Traffic Replayer

A standalone command-line tool that reads an NDJSON capture file produced by
`tools/traffic-recorder` and republishes its messages to RabbitMQ, using the
same `{raw, icao_hex, received_at, source}` envelope and the same routing the
receiver uses — one publish to the `skyfollower-adsb` consistent-hash exchange per
message, keyed by ICAO hex. It lets the rest of the pipeline (message
processor, rules engine, archive processor) be exercised repeatedly against a
known, recorded set of traffic — without live ADS-B reception or SDR
hardware, and byte-for-byte identical on every run.

It is a plain Python script, not a container — there is no Dockerfile or
Compose service for it. Run it from any host that can reach the target
RabbitMQ broker.

## Usage

```bash
pip install -r requirements.txt

python main.py --input capture.ndjson --mode relative \
    --rabbitmq-host 192.168.1.10

python main.py --input capture.ndjson --mode stress \
    --rabbitmq-host 192.168.1.10 \
    --rabbitmq-user skyfollower --rabbitmq-password secret
```

### Sample captures

Four ready-to-use captures are checked into the repo alongside this tool, so
you don't need to record your own to exercise the pipeline:

| File | Messages | Compressed size |
|------|----------|-----------------|
| `tools/100K.ndjson.gz` | ~100,000 | 1.3 MB |
| `tools/1M.ndjson.gz` | ~1,000,000 | 14 MB |
| `tools/3M.ndjson.gz` | ~3,000,000 | 42 MB |
| `tools/5M.ndjson.gz` | ~5,000,000 | 70 MB |

They are passed straight to `--input` — the `.gz` is decompressed on the fly,
no `gunzip` step:

```bash
python main.py --input ../100K.ndjson.gz --mode stress \
    --rabbitmq-host 192.168.1.10
```

Start with `100K` for a quick end-to-end check; use `3M`/`5M` in `stress`
mode for throughput and soak testing.

The full capture file is loaded into memory and sorted by `received_at`
before anything is published, so replay order is always chronological
regardless of the order messages happen to appear in the file (e.g. a
capture recorded from multiple concurrent sources). Progress (`messages
published`, rate, and — in `relative` mode — estimated time remaining) is
printed every 5 seconds. Stop early at any time with `Ctrl+C` (or
`SIGTERM`); messages already published are not affected.

The replayer has no notion of how many message processors exist — the
exchange decides which one receives each aircraft, exactly as it does for a
live receiver. The exchange and its `skyfollower-adsb-unroutable` alternate
exchange are declared (durable) on connect, so the replayer works against a
freshly created RabbitMQ vhost. With no message processors bound yet, every
replayed message lands in `skyfollower-adsb-unroutable` rather than being
discarded, which is itself a useful check that the capture is being
published at all.

## Modes

The two modes exist for different testing purposes and are not
interchangeable:

- **`relative`** — preserves the original inter-message timing recorded in
  `received_at`, sleeping between publishes so the pipeline sees traffic at
  (approximately) the same rate it originally arrived at. Use this to
  reproduce a specific real-world scenario realistically — e.g. replaying a
  capture that contains a rule-triggering event, and confirming the rule
  fires with the right timing/latency characteristics, or watching flight
  state build up at a natural pace.
- **`stress`** — publishes every message back-to-back as fast as RabbitMQ
  accepts them, with no delay. Use this for load testing — finding the
  message processor's throughput ceiling, checking for backlog/queue-depth
  behavior under sustained high volume, or soak-testing a long capture in a
  fraction of its original duration.

If replay falls behind schedule in `relative` mode (e.g. a slow publish, or
RabbitMQ applying backpressure), later messages publish immediately rather
than sleeping further — the tool never tries to "catch up" by bursting, but
it also never waits longer than necessary once it's behind.

## After a Replay Finishes

A message processor's flight eviction (and therefore archiving) is gated on
`message_clock` — the newest message `received_at` it has seen — not
wall-clock time (see `message-processor/README.md`'s "Active flight store
durability & crash recovery" section). A replay is just another source of
messages to it, so this applies the same way here as it does to a RabbitMQ
backlog drained after a restart: `message_clock` only advances while
messages are actually being consumed.

If a replay is the *only* traffic a message processor sees (no live
receiver attached), `message_clock` stops advancing the instant the last
message is published — there's nothing left to move it forward. Any flights
still active at that point sit in the active store indefinitely: not
evicted, not archived, `local_archive_queue_depth` and `active_flights`
completely flat, no matter how long you leave it running. This is expected,
not a defect — it will stay that way until either another replay runs
against the same message processor or a live receiver starts feeding it
real traffic, at which point `message_clock` jumps forward and eviction
catches up on its very next pass. Don't chase a flat post-replay
`local_archive_queue_depth` as a stuck or broken archive path without first
checking whether anything is still publishing.

## Arguments

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--input` | Yes | — | Path to the capture file to replay (see `tools/traffic-recorder`'s README for the file format). Either a plain `.ndjson` file or a gzip-compressed `.ndjson.gz` file is accepted; the compressed form is detected by the `.gz` extension and decompressed on the fly, so a large capture never has to be `gunzip`ped to a temp file first. A file with a `.gz` extension that isn't actually a valid gzip stream fails immediately with a clear error. |
| `--mode` | Yes | — | `relative` or `stress` (see [Modes](#modes) above). |
| `--rabbitmq-host` | Yes | — | RabbitMQ hostname or IP. |
| `--rabbitmq-port` | No | `5672` | RabbitMQ AMQP port. |
| `--rabbitmq-user` | No | `guest` | RabbitMQ username. |
| `--rabbitmq-password` | No | `guest` | RabbitMQ password. |

Malformed lines in the input file (invalid JSON) print a warning to stderr
and are skipped, rather than aborting the whole replay.

## Tests

```bash
python -m pytest tools/traffic-replayer/tests/
```

`replay()` — the function that decides, per message, whether to sleep and
what to publish — is covered directly with a fake RabbitMQ channel and a
fake clock, for both modes: that a `stress`-mode replay never sleeps, that a
`relative`-mode replay sleeps for exactly the gap between each message's
original `received_at`, that it stops sleeping (without erroring) once it has
fallen behind schedule, that every message goes to the `skyfollower-adsb` exchange keyed
by its own ICAO hex (never to the default exchange), and that setting
`stop_event` before starting halts replay immediately.

`_load_capture()` — extension-based gzip detection, decompression, blank-line
skipping, the malformed-line skip, and the clear error a `.gz` file that
isn't really gzip must produce instead of a per-line warning storm — is
covered directly, including that a gzip-compressed capture parses to exactly
the same message list as its uncompressed equivalent.

`main()` itself — argument parsing, opening the real `pika` connection, and
sorting the loaded capture — is intentionally not covered by a separate
test. It is thin sequential glue with no branching logic of its own beyond
what's already exercised above; the only way to meaningfully test it further
would be to fake the `pika.BlockingConnection` itself, which would mostly be
asserting that `main()` calls the functions it obviously calls. This is why,
unlike `traffic-recorder`, this tool went undocumented as having "no tests"
before now: its core logic (`replay()`) was always unit-testable, it simply
hadn't been done yet.

## Documentation Site

Tool directories under `tools/` are picked up automatically by the
[documentation site](https://brentio.github.io/SkyFollower/)'s
`docs/scripts/discover.mjs` `tools/*` scan, which generates a page per tool
from this README with no change to `discover.mjs` needed. The hand-authored
`docs/tools/index.md` overview page lists each tool by name and is updated
alongside this file.
