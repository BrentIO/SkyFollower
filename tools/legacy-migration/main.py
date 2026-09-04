#!/usr/bin/env python3
"""
SkyFollower Legacy Migration

One-time tool that copies the legacy MongoDB-tracked flight archive into
this repo's S3/Parquet archive format (see the GitHub issue this tool
implements, "Migrate ~8M legacy flights (MongoDB) to the new S3 archive
format", for the full design -- and README.md for the operator runbook:
RabbitMQ user provisioning, IAM setup, and how the two passes fit
together).

Three roles, one binary:

    python main.py produce --start-date 2022-07-11 --end-date 2026-09-01
    python main.py work
    python main.py verify --start-date 2022-07-11 --end-date 2026-09-01
"""

from __future__ import annotations

import argparse
import logging
import os
import socket
import sys

# Add /app to sys.path so shared/ is importable. Two levels up, not one:
# unlike archive-processor/message-processor/etc. (directly under /app),
# this tool lives under tools/legacy-migration -- one directory deeper.
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))

import producer  # noqa: E402
import verify  # noqa: E402
import worker  # noqa: E402

from shared.logging_setup import configure_logging  # noqa: E402

logger = logging.getLogger("legacy-migration")

_ROLES = {"produce": producer, "work": worker, "verify": verify}


def _add_file_handler() -> None:
    """
    Bind-mounted ./logs/ (see docker-compose.legacy-migration.yaml), one
    file per worker container -- named by hostname so `--scale` produces
    one file each -- in addition to stdout.
    """
    log_dir = "/app/logs"
    os.makedirs(log_dir, exist_ok=True)
    handler = logging.FileHandler(os.path.join(log_dir, f"{socket.gethostname()}.log"))
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    logging.getLogger().addHandler(handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="SkyFollower legacy MongoDB -> S3 migration")
    subparsers = parser.add_subparsers(dest="role", required=True)
    for name, module in _ROLES.items():
        module.add_arguments(subparsers.add_parser(name))

    args = parser.parse_args()

    configure_logging(os.environ.get("LOG_LEVEL", "info"))
    _add_file_handler()

    logger.info("Starting SkyFollower Legacy Migration (%s)", args.role)
    _ROLES[args.role].run(args)


if __name__ == "__main__":
    main()
