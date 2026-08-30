# TCP Traffic Replayer

A standalone command-line tool that reads an NDJSON capture file produced by
`tools/traffic-recorder` and serves its messages over a raw TCP **listen**
socket in readsb's wire format (`*<hex>;`), so an unmodified `receiver` can
be pointed at this tool instead of a live readsb instance.

Where `tools/traffic-replayer` republishes a capture to RabbitMQ — exercising
the message processor, rules engine, and archive processor, but skipping the
receiver entirely — this tool sits one stage earlier and stands in for
readsb itself. It drives the receiver's own code: the socket read loop,
`shared/adsb_1090.py`'s `parse_tcp_stream`, message routing, the RabbitMQ
publish, and the offline fallback queue. That makes it the way to throw a
known, recorded, repeatable message stream at the receiver's actual TCP
ingest path — for finding its throughput ceiling, or confirming a backlog
drain never starves live intake.

It is a plain Python script, not a container — there is no Dockerfile or
Compose service for it. It has no third-party dependencies (Python standard
library only). Run it on any host the receiver can open a TCP connection to.

## Usage

```bash
python main.py --input capture.ndjson --mode relative --port 30002

python main.py --input capture.ndjson.gz --mode stress --port 30002 --bind 127.0.0.1
```

Then point a receiver at it by setting that host and port as a `1090` source
in the receiver's `RECEIVER_SOURCES` (e.g. `RECEIVER_SOURCES=<tool-host>:30002:1090`).
No receiver code or config changes beyond that are needed — to the receiver
this is just another readsb raw 1090 feed.

The full capture is loaded into memory, filtered to `source == "1090"` rows
only, and sorted by `received_at` before anything is served, so replay order
is always chronological regardless of the order messages appear in the file.
Non-`1090` rows (`978`, `EXTERNAL`) are discarded with a count printed at
startup — the tool is deliberately single-listener/single-source and does
not attempt the mixed-source case:

```
Loaded 100,000 messages (99,642 source=1090, 358 discarded).
```

The tool listens on one port and accepts one receiver connection at a time.
Every fresh connection replays the capture **from the beginning** — there is
no resume-from-where-it-left-off concept; a repeatable from-the-top replay
is the point, matching `traffic-replayer`'s "byte-for-byte identical on every
run" property. If the receiver disconnects and reconnects (a container
restart, a crash, reconnect-loop testing), the tool simply accepts the new
connection and replays again from the start — it does not need to be
restarted itself.

Stop the tool at any time with `Ctrl+C` (or `SIGTERM`).

## Modes

The two modes match `tools/traffic-replayer`'s and exist for different
testing purposes:

- **`relative`** — preserves the original inter-message timing recorded in
  `received_at`, sleeping between sends so the receiver sees traffic at
  (approximately) the rate it originally arrived. If a send falls behind
  schedule (e.g. TCP backpressure from a slow receiver), later messages go
  out immediately rather than bursting to catch up — the tool never tries to
  make up lost time, and never waits longer than necessary once it is
  behind.
- **`stress`** — sends every message back-to-back with **no artificial
  pacing** — no sleep, no rate cap, no waiting for an application-level ack.
  Send-side formatting and `sendall()` on localhost run at tens of thousands
  of messages per second and well beyond, so the receiver's own CPU, not
  this tool, is always the limiting factor. This is what a "find the
  receiver's actual throughput ceiling" measurement needs.

`stress` mode still respects TCP flow control: once the kernel send buffer
and the receiver's advertised window fill, a blocking `sendall()` naturally
stalls until the receiver reads more. That is correct backpressure — the
same a real overloaded receiver would feel from any upstream feed — not a
bug. The tool deliberately does **not** switch to non-blocking sends or drop
messages on a full buffer: that would inject artificial loss unrelated to
the receiver's own code and corrupt exactly the sent-vs-received comparison
this tool exists to enable.

## Sent Counter and Summaries

The tool tracks a running count of messages successfully handed to
`sendall()`, printed every 5 seconds during a replay and again as a final
summary with elapsed time and average rate.

Because TCP is lossless for as long as the connection stays open, this
"sent" count and the receiver's own per-connection received-message
telemetry (published over MQTT) always converge to equal once the receiver
has drained everything — a live gap during a run is buffering/backpressure
lag, not loss.

The one case that *is* real loss — the connection dropping before every
message was sent — is reported distinctly, never folded into the normal
completion output:

```
Done: 5,000,000/5,000,000 messages sent in 74.2s (67,384 msg/s average)
```
```
Replay interrupted: 3,204,551 / 5,000,000 sent before the connection closed (48.1s, 66,622 msg/s average)
```

After a full replay the tool holds the connection open (rather than closing
it and forcing the receiver into a reconnect) until the receiver itself
disconnects, then goes back to waiting for the next connection.

## Arguments

| Flag | Required | Default | Description |
|------|----------|---------|-------------|
| `--input` | Yes | — | Path to the capture file. A `.ndjson` file is read as plain text; a `.ndjson.gz` (or any `.gz`) file is transparently read gzip-compressed — selected by extension, no separate flag. See `tools/traffic-recorder`'s README for the file format. |
| `--mode` | Yes | — | `relative` or `stress` (see [Modes](#modes) above). |
| `--port` | No | `30002` | TCP port to listen on. `30002` is readsb's raw 1090 port. |
| `--bind` | No | `0.0.0.0` | Address to bind the listen socket to. |

Malformed lines in the input file (invalid JSON) print a warning to stderr
and are skipped, rather than aborting the replay.

## Tests

```bash
python -m pytest tools/tcp-traffic-replayer/tests/
```

The tool's core logic is separated from the listen socket and `main()` glue
so it is covered without opening a real socket or sleeping in wall-clock
time:

- `format_frame()` — that a raw hex string is wrapped as `*<hex>;` + newline,
  and that the result round-trips through `shared/adsb_1090.py`'s
  `parse_tcp_stream` (the receiver's own parser) even when fed one byte at a
  time.
- `load_messages()` — that non-`1090` rows are filtered out and counted, that
  kept rows are sorted by `received_at` regardless of file order, that a
  `.gz` capture is read by extension, and that malformed JSON lines are
  skipped.
- `replay()` — driven with a fake sink and a fake clock: that `stress` mode
  never sleeps, that `relative` mode sleeps for exactly the gap between each
  message's original `received_at`, that it stops pacing (without erroring)
  once it has fallen behind, that a sink failure mid-replay returns a
  distinct partial outcome (`sent` vs `total`), and that a pre-set
  `stop_event` halts replay immediately.

`main()` and the accept/reconnect loop are intentionally not covered by a
separate test — they are thin glue around a live socket with no branching
logic that isn't already exercised above.

## Documentation Site

Tool directories under `tools/` are picked up automatically by the
[documentation site](https://brentio.github.io/SkyFollower/)'s
`docs/scripts/discover.mjs` `tools/*` scan, which generates a page per tool
from this README with no change to `discover.mjs` needed. The hand-authored
`docs/tools/index.md` overview page lists each tool by name and is updated
alongside this file.
