# Traffic Replayer

A standalone command-line tool that reads an NDJSON capture file produced by
`tools/traffic-recorder` and republishes its messages to RabbitMQ, using the
same `{raw, icao_hex, received_at, source}` envelope and the same routing the
receiver uses — one publish to the `adsb` consistent-hash exchange per
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

The full capture file is loaded into memory and sorted by `received_at`
before anything is published, so replay order is always chronological
regardless of the order messages happen to appear in the file (e.g. a
capture recorded from multiple concurrent sources). Progress (`messages
published`, rate, and — in `relative` mode — estimated time remaining) is
printed every 5 seconds. Stop early at any time with `Ctrl+C` (or
`SIGTERM`); messages already published are not affected.

The replayer has no notion of how many message processors exist — the
exchange decides which one receives each aircraft, exactly as it does for a
live receiver. The exchange and its `adsb-unroutable` alternate exchange are
declared (durable) on connect, so the replayer works against a freshly
created RabbitMQ vhost. With no message processors bound yet, every replayed
message lands in `adsb-unroutable` rather than being discarded, which is
itself a useful check that the capture is being published at all.

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

## Arguments

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--input` | Yes | — | Path to the NDJSON capture file to replay (see `tools/traffic-recorder`'s README for the file format). |
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
fallen behind schedule, that every message goes to the `adsb` exchange keyed
by its own ICAO hex (never to the default exchange), and that setting
`stop_event` before starting halts replay immediately.

`main()` itself — argument parsing, opening the real `pika` connection, and
loading/sorting the capture file — is intentionally not covered by a
separate test. It is thin sequential glue with no branching logic of its own
beyond what's already exercised above (file loading has no interesting logic
except the malformed-line skip, which is a two-line `try`/`except` around
`json.loads`); the only way to meaningfully test it further would be to fake
the `pika.BlockingConnection` itself, which would mostly be asserting that
`main()` calls the functions it obviously calls. This is why, unlike
`traffic-recorder`, this tool went undocumented as having "no tests" before
now: its core logic (`replay()`) was always unit-testable, it simply hadn't
been done yet.

## Documentation Site

This tool is deliberately repo-only documentation — it is developer/testing
tooling, not one of the deployed pipeline components the
[documentation site](https://brentio.github.io/SkyFollower/) covers
(deployment topology, running components, and their configuration
reference). `docs/scripts/discover.mjs` does not include `tools/*`.
