#!/usr/bin/env python3
"""
SkyFollower Traffic Replayer

Reads a captured NDJSON file (from traffic-recorder) and publishes messages to
RabbitMQ queues in one of two modes:

  relative  — preserves original inter-message timing
  stress    — publishes as fast as RabbitMQ accepts (no delays)

Usage:
    python main.py --input capture.ndjson --mode relative \
        --processor-count 1 --rabbitmq-host 192.168.1.10

    python main.py --input capture.ndjson --mode stress \
        --processor-count 4 --rabbitmq-host 192.168.1.10 \
        --rabbitmq-user skyfollower --rabbitmq-password secret
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
import time

import pika


# ---------------------------------------------------------------------------
# RabbitMQ helpers
# ---------------------------------------------------------------------------

def _connect(host: str, port: int, user: str, password: str) -> pika.BlockingConnection:
    creds = pika.PlainCredentials(user, password)
    params = pika.ConnectionParameters(
        host=host,
        port=port,
        credentials=creds,
        heartbeat=60,
    )
    return pika.BlockingConnection(params)


def _declare_queues(channel, processor_count: int) -> None:
    for i in range(processor_count):
        channel.queue_declare(queue=f"adsb-{i}", durable=True)


def _queue_name(icao_hex: str, processor_count: int) -> str:
    return f"adsb-{int(icao_hex, 16) % processor_count}"


# ---------------------------------------------------------------------------
# Replay
# ---------------------------------------------------------------------------

def replay(
    messages: list[dict],
    channel,
    processor_count: int,
    mode: str,
    stop_event: threading.Event,
) -> int:
    if not messages:
        return 0

    first_ts = messages[0]["received_at"]
    replay_start = time.monotonic()
    count = 0
    last_progress = time.monotonic()

    for msg in messages:
        if stop_event.is_set():
            break

        if mode == "relative":
            target = replay_start + (msg["received_at"] - first_ts)
            now = time.monotonic()
            if target > now:
                time.sleep(target - now)

        queue = _queue_name(msg["icao_hex"], processor_count)
        body = json.dumps(
            {
                "raw": msg["raw"],
                "icao_hex": msg["icao_hex"],
                "received_at": msg["received_at"],
                "source": msg["source"],
            },
            separators=(",", ":"),
        )
        channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=body,
            properties=pika.BasicProperties(delivery_mode=2),
        )
        count += 1

        now = time.monotonic()
        if now - last_progress >= 5:
            elapsed = now - replay_start
            rate = count / elapsed if elapsed > 0 else 0
            if mode == "relative":
                total_duration = messages[-1]["received_at"] - first_ts
                remaining = total_duration - (msg["received_at"] - first_ts)
                print(
                    f"  {count}/{len(messages)} messages  {rate:.0f} msg/s  "
                    f"~{remaining:.0f}s remaining",
                    flush=True,
                )
            else:
                print(f"  {count}/{len(messages)} messages  {rate:.0f} msg/s", flush=True)
            last_progress = now

    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="SkyFollower Traffic Replayer")
    parser.add_argument("--input", required=True, help="Input NDJSON capture file")
    parser.add_argument(
        "--mode",
        choices=["relative", "stress"],
        required=True,
        help="relative: preserve original timing; stress: publish as fast as possible",
    )
    parser.add_argument(
        "--processor-count",
        type=int,
        required=True,
        help="Number of processor queues (must match receiver/processor config)",
    )
    parser.add_argument("--rabbitmq-host", required=True)
    parser.add_argument("--rabbitmq-port", type=int, default=5672)
    parser.add_argument("--rabbitmq-user", default="guest")
    parser.add_argument("--rabbitmq-password", default="guest")
    args = parser.parse_args()

    print(f"Loading {args.input} ...", flush=True)
    messages = []
    with open(args.input) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                messages.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"Warning: skipping line {lineno}: {e}", file=sys.stderr)

    if not messages:
        print("No messages found in capture file.", file=sys.stderr)
        sys.exit(1)

    messages.sort(key=lambda m: m["received_at"])
    print(f"Loaded {len(messages)} messages.", flush=True)

    print(f"Connecting to RabbitMQ at {args.rabbitmq_host}:{args.rabbitmq_port} ...", flush=True)
    try:
        connection = _connect(
            args.rabbitmq_host,
            args.rabbitmq_port,
            args.rabbitmq_user,
            args.rabbitmq_password,
        )
    except Exception as e:
        print(f"Failed to connect to RabbitMQ: {e}", file=sys.stderr)
        sys.exit(1)

    channel = connection.channel()
    _declare_queues(channel, args.processor_count)

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    print(f"Replaying in {args.mode} mode ...", flush=True)
    start = time.monotonic()

    try:
        count = replay(messages, channel, args.processor_count, args.mode, stop_event)
    except KeyboardInterrupt:
        stop_event.set()
        count = 0
    finally:
        try:
            connection.close()
        except Exception:
            pass

    elapsed = time.monotonic() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"\nDone: {count} messages in {elapsed:.1f}s ({rate:.0f} msg/s average)")


if __name__ == "__main__":
    main()
