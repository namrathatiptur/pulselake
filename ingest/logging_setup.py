"""
One place to configure logging so every entry point behaves the same way.

Logs go to two destinations at once: the console, so you can watch a run, and
a rotating file under logs/, so a loop left running overnight does not fill the
disk and you still have the history the next morning.
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

from ingest.config import LOG_PATH, ensure_directories

LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)-18s %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

MAX_LOG_BYTES = 5 * 1024 * 1024
BACKUP_COUNT = 3


def configure_logging(
    level: int = logging.INFO,
    log_path: Path | None = None,
    to_console: bool = True,
) -> logging.Logger:
    """
    Set up root logging. Safe to call more than once: existing handlers are
    cleared first so repeated calls do not produce duplicated log lines.
    """
    ensure_directories()
    path = log_path or LOG_PATH

    root = logging.getLogger()
    root.setLevel(level)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    file_handler = RotatingFileHandler(
        path, maxBytes=MAX_LOG_BYTES, backupCount=BACKUP_COUNT, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    root.addHandler(file_handler)

    if to_console:
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        root.addHandler(console)

    # urllib3 logs every connection at DEBUG, which drowns out our own lines.
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    return root
