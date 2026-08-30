# Tools

Standalone command-line utilities that support testing the pipeline —
plain Python scripts, not containers; there's no Dockerfile or Compose
service for any of them.

- [TCP Traffic Replayer](/tools/tcp-traffic-replayer) — serves a captured
  NDJSON file over a raw TCP listen socket in readsb's wire format, standing
  in for readsb so the receiver's own TCP ingest path can be exercised
- [Traffic Recorder](/tools/traffic-recorder) — captures raw readsb/dump978-fa
  messages to an NDJSON file
- [Traffic Replayer](/tools/traffic-replayer) — republishes a captured NDJSON
  file to RabbitMQ, preserving original timing or as fast as possible

Used together, a capture from Traffic Recorder can be replayed against a real
RabbitMQ/message-processor pipeline with Traffic Replayer, exercising it
repeatedly and byte-for-byte identically without live ADS-B traffic or SDR
hardware present. TCP Traffic Replayer covers the stage upstream of that —
the receiver itself — by replaying the same capture as the raw TCP feed the
receiver normally consumes from readsb.
