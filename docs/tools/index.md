# Tools

Standalone command-line utilities that support testing the pipeline —
plain Python scripts, not containers; there's no Dockerfile or Compose
service for any of them.

- [Traffic Recorder](/tools/traffic-recorder) — captures raw readsb/dump978-fa
  messages to an NDJSON file
- [Traffic Replayer](/tools/traffic-replayer) — republishes a captured NDJSON
  file to RabbitMQ, preserving original timing or as fast as possible

Used together, a capture from Traffic Recorder can be replayed against a real
RabbitMQ/message-processor pipeline with Traffic Replayer, exercising it
repeatedly and byte-for-byte identically without live ADS-B traffic or SDR
hardware present.
