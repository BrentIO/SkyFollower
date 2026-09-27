"""
Shared logging configuration for data runners.

Runners should load config first, then call
`configure_logging(cfg.get("log_level"))`.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO


def configure_logging(log_level: str | None = None, stream: TextIO = sys.stdout) -> None:
    """Configure the root logger's format and level.

    `log_level == "debug"` maps to DEBUG; anything else (including None
    or unrecognized) maps to INFO.
    """
    logging.basicConfig(
        level=logging.DEBUG if log_level == "debug" else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=stream,
        force=True,
    )
    # pika re-emits connection/channel/transport workflow at INFO on every
    # connect/reconnect/shutdown, burying sparser caller INFO lines.
    logging.getLogger("pika").setLevel(logging.WARNING)
