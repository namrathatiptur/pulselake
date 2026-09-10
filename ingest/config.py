"""
Central configuration for PulseLake.

Everything that another module might want to tweak lives here so that there is
one obvious place to look. Values can be overridden with environment variables,
which is the pattern you would use later when this runs in a container.
"""

import os
from pathlib import Path

# Project root is the parent of the ingest package, so paths work no matter
# which directory you launch Python from.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_DIR = PROJECT_ROOT / "data"
LOG_DIR = PROJECT_ROOT / "logs"

# The single DuckDB file that acts as our local lake plus warehouse.
DUCKDB_PATH = Path(os.getenv("PULSELAKE_DB", DATA_DIR / "pulselake.duckdb"))

LOG_PATH = Path(os.getenv("PULSELAKE_LOG", LOG_DIR / "ingest.log"))

# MTA publishes NYC subway GTFS-Realtime as a set of protobuf endpoints, one
# per group of lines. As of 2023 these are fully open: no API key, no account,
# no registration. The key of each entry is a short name we store alongside the
# data so we know which feed a row came from.
FEED_BASE = "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/"

FEEDS = {
    "1234567S": FEED_BASE + "nyct%2Fgtfs",
    "ACE": FEED_BASE + "nyct%2Fgtfs-ace",
    "BDFM": FEED_BASE + "nyct%2Fgtfs-bdfm",
    "G": FEED_BASE + "nyct%2Fgtfs-g",
    "JZ": FEED_BASE + "nyct%2Fgtfs-jz",
    "NQRW": FEED_BASE + "nyct%2Fgtfs-nqrw",
    "L": FEED_BASE + "nyct%2Fgtfs-l",
    "SIR": FEED_BASE + "nyct%2Fgtfs-si",
}

# Which feeds to pull by default. The numbered lines feed is the one referenced
# in the project brief and is enough on its own. Set PULSELAKE_FEEDS=all to
# ingest the whole subway system, or pass a comma separated list of keys.
DEFAULT_FEEDS = ["1234567S"]


def resolve_feeds(selection: str | None = None) -> dict[str, str]:
    """
    Turn a feed selection string into a mapping of feed key to URL.

    Accepts "all", a comma separated list of keys such as "1234567S,ACE", or
    None, which falls back to the PULSELAKE_FEEDS environment variable and then
    to DEFAULT_FEEDS. Raises ValueError on an unknown key so a typo fails loudly
    instead of silently ingesting nothing.
    """
    if selection is None:
        selection = os.getenv("PULSELAKE_FEEDS", "")

    selection = selection.strip()

    if not selection:
        keys = DEFAULT_FEEDS
    elif selection.lower() == "all":
        keys = list(FEEDS)
    else:
        keys = [part.strip() for part in selection.split(",") if part.strip()]

    unknown = [key for key in keys if key not in FEEDS]
    if unknown:
        raise ValueError(
            f"Unknown feed key(s): {', '.join(unknown)}. "
            f"Valid keys are: {', '.join(FEEDS)} (or 'all')."
        )

    return {key: FEEDS[key] for key in keys}


# Network tuning. The feed updates roughly every 30 seconds, so there is no
# value in polling much faster than that.
REQUEST_TIMEOUT_SECONDS = 20
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2.0

# Default gap between ingestion cycles for the loop scheduler.
POLL_INTERVAL_SECONDS = int(os.getenv("PULSELAKE_INTERVAL", "45"))


def ensure_directories() -> None:
    """Create the data and logs directories if they do not exist yet."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
