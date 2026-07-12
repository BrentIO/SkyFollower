"""
Shared logging configuration for data runners (issue #331).

Every runner's `main()` used to call `logging.basicConfig(level=logging.INFO,
...)` before loading settings.json, so the documented `log_level` config field
had no way to actually reach the logger. Runners should load config first,
then call `configure_logging(cfg.get("log_level"))`.
"""

from __future__ import annotations

import logging
import sys
from typing import TextIO


def configure_logging(log_level: str | None = None, stream: TextIO = sys.stdout) -> None:
    """Configure the root logger's format and level.

    `log_level == "debug"` maps to DEBUG; anything else (including None,
    missing, or an unrecognized value) maps to INFO — matching the
    receiver/processor convention this replaces.
    """
    logging.basicConfig(
        level=logging.DEBUG if log_level == "debug" else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=stream,
        force=True,
    )
