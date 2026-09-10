"""
Run one full ingestion cycle: fetch every selected feed, decode, store.

This is the unit of work the scheduler in run_loop.py repeats. Keeping it a
single importable function means the loop, a future Airflow task, and the
command line all execute exactly the same code path.

    python -m ingest.run_once
    python -m ingest.run_once --feeds all
"""

from __future__ import annotations

import argparse
import logging

import duckdb

from ingest.config import resolve_feeds
from ingest.fetch import FeedFetchError, build_session, fetch_and_parse
from ingest.logging_setup import configure_logging
from ingest.parse import FeedParseError
from ingest.storage import (
    initialize_schema,
    open_db,
    record_error,
    store_snapshot,
    summarize_database,
)

logger = logging.getLogger("pulselake.ingest")


def run_cycle(
    con: duckdb.DuckDBPyConnection,
    feeds: dict[str, str],
    session=None,
) -> dict:
    """
    Fetch and store every feed once.

    A failure on one feed is logged and recorded but does not abort the others.
    When you are polling eight endpoints, one of them returning a 503 should
    not cost you the other seven.

    Returns a counters dictionary suitable for logging or for a test to assert
    against.
    """
    session = session or build_session()
    counters = {"inserted": 0, "duplicate": 0, "failed": 0, "rows": 0}

    for feed_key, feed_url in feeds.items():
        try:
            snapshot = fetch_and_parse(feed_key, feed_url, session=session)
        except FeedFetchError as exc:
            counters["failed"] += 1
            logger.error("Fetch failed for feed %s: %s", feed_key, exc)
            record_error(con, feed_key, feed_url, type(exc).__name__, str(exc))
            continue
        except FeedParseError as exc:
            counters["failed"] += 1
            logger.error("Decode failed for feed %s: %s", feed_key, exc)
            record_error(con, feed_key, feed_url, type(exc).__name__, str(exc))
            continue

        try:
            result = store_snapshot(con, snapshot)
        except Exception as exc:
            # A storage failure is more serious than a feed failure, so it is
            # logged with the traceback rather than a one line message.
            counters["failed"] += 1
            logger.exception("Storage failed for feed %s", feed_key)
            record_error(con, feed_key, feed_url, type(exc).__name__, str(exc))
            continue

        counters[result["status"]] += 1
        counters["rows"] += result["rows"]

        if result["status"] == "inserted":
            logger.info(
                "feed=%s stored fetch_id=%s rows=%d (vehicles=%d stops=%d alerts=%d) "
                "published=%s",
                feed_key,
                result["fetch_id"],
                result["rows"],
                len(snapshot.vehicle_positions),
                len(snapshot.trip_stop_updates),
                len(snapshot.alerts),
                snapshot.header_timestamp,
            )
        else:
            logger.info(
                "feed=%s unchanged since last poll (fetch_id=%s seen %d times, "
                "published=%s)",
                feed_key,
                result["fetch_id"],
                result["poll_count"],
                snapshot.header_timestamp,
            )

    return counters


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feeds",
        default=None,
        help="Comma separated feed keys, or 'all'. Defaults to the numbered lines feed.",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Path to the DuckDB file. Defaults to data/pulselake.duckdb.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Log to the file only, not the console.",
    )
    args = parser.parse_args(argv)

    configure_logging(to_console=not args.quiet)

    try:
        feeds = resolve_feeds(args.feeds)
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return 2

    with open_db(args.db) as con:
        initialize_schema(con)
        counters = run_cycle(con, feeds)
        stats = summarize_database(con)

    logger.info(
        "cycle complete: inserted=%d duplicate=%d failed=%d new_rows=%d",
        counters["inserted"],
        counters["duplicate"],
        counters["failed"],
        counters["rows"],
    )
    logger.info(
        "database now holds %d snapshots, %d vehicle rows, %d stop updates, "
        "%d alert rows, %d errors",
        stats["raw_feed_fetches"],
        stats["raw_vehicle_positions"],
        stats["raw_trip_stop_updates"],
        stats["raw_alerts"],
        stats["ingest_errors"],
    )

    # Non zero exit only if nothing at all worked, so a single flaky feed does
    # not make a scheduled run look like a total failure.
    if counters["failed"] and not (counters["inserted"] or counters["duplicate"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
