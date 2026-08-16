# Traffic Recorder

A standalone command-line tool that connects to one or more readsb/dump978-fa
TCP streams (the same raw feeds the `receiver` component consumes) and
captures every message to an NDJSON file, one JSON object per line. The
captured file can later be replayed against a real RabbitMQ/message-processor
pipeline with `tools/traffic-replayer`, so the pipeline can be exercised
repeatedly, offline, and byte-for-byte identically, without live ADS-B
traffic or SDR hardware present.

It is a plain Python script, not a container — there is no Dockerfile or
Compose service for it. Run it directly on any host that can reach the
readsb/dump978-fa TCP ports you want to capture from (typically Host A, or a
laptop pointed at Host A over the network).

## Usage

```bash
pip install -r requirements.txt

python main.py --output capture.ndjson --duration 7200 \
    --sources "localhost:30002:1090" "localhost:30978:978" "localhost:30105:EXTERNAL"
```

Each source runs on its own thread and captures concurrently; the tool prints
a running per-source message count every 30 seconds and a final summary line
when it stops. Stop early at any time with `Ctrl+C` (or `SIGTERM`) — output
captured so far is preserved, since each record is written and flushed as
soon as it's decoded rather than buffered until exit.

## Arguments

| Flag | Required | Description |
|------|----------|-------------|
| `--output` | Yes | Path to the NDJSON file to write. Opened in append mode; parent directories are created automatically. |
| `--sources` | Yes | One or more `HOST:PORT:SOURCE_TAG` triples, space-separated. `SOURCE_TAG` must be exactly one of `1090`, `978`, or `EXTERNAL` (case-sensitive). Use `1090` for a readsb raw 1090 MHz port, `978` for a dump978-fa raw UAT port, and `EXTERNAL` for any other 1090-style Beast/raw-Mode-S feed (parsed the same way as `1090` — it arrives as ordinary Mode S frames, just tagged by source rather than by frame content). |
| `--duration` | No | Capture duration in seconds. Omit for unlimited (capture until interrupted). |

## File Format

One JSON object per line (NDJSON), matching the RabbitMQ message envelope
the receiver publishes:

```json
{"raw":"8d4840d6202cc371c32ce0576098","icao_hex":"4840D6","received_at":1731020400.123456,"source":"1090"}
```

| Field | Type | Description |
|-------|------|-------------|
| `raw` | string | Raw Mode S/UAT frame hex, exactly as received on the wire. |
| `icao_hex` | string | 6-character uppercase ICAO hex address decoded from the frame. |
| `received_at` | float | Unix timestamp (seconds, fractional) when the message was captured. |
| `source` | string | `"1090"`, `"978"`, or `"EXTERNAL"` — whichever source tag was configured for the connection this message arrived on. |

1090/EXTERNAL frames are decoded once via pyModeS purely to extract `icao_hex`
for the envelope; malformed frames, and any frame pyModeS can't extract an
ICAO address from, are silently skipped rather than written. 978 (UAT) lines
are parsed with `shared/uat.py`'s `parse_978_line`, which supplies its own
`received_at` from the dump978-fa line itself rather than capture-time
`time.time()`.

## Tests

```bash
python -m pytest tools/traffic-recorder/tests/
```

Covers `SourceCapture`'s thread lifecycle (including a regression test for a
bug where naming the stop event `self._stop` shadowed `threading.Thread`'s
own private `_stop()` method, breaking `join()`), source-tag validation, and
that an `EXTERNAL`-tagged source is routed through the same decode path as
`1090`.

## Documentation Site

This tool is deliberately repo-only documentation — it is developer/testing
tooling, not one of the deployed pipeline components the
[documentation site](https://brentio.github.io/SkyFollower/) covers
(deployment topology, running components, and their configuration
reference). `docs/scripts/discover.mjs` does not include `tools/*`.
