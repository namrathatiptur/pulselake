"""
The scheduler: run the ingestion cycle repeatedly until told to stop.

This is deliberately a plain Python loop and not Airflow or Dagster. For a
single task on a fixed interval, a loop is the right amount of machinery, and
being able to explain why you did not reach for a heavier tool is worth more
in an interview than having reached for one. The upgrade path is in the README.

Three details that make this more than a while True with a sleep:

  * Drift correction. Sleeping for a fixed interval after variable length work
    means the actual period is interval plus work time, so cycles slowly slide.
    We sleep for whatever is left of the interval instead, so cycle starts stay
    on a fixed cadence.

  * Graceful shutdown. SIGINT and SIGTERM set a flag rather than killing the
    process mid write, so Ctrl+C finishes the current cycle and closes the
    DuckDB connection cleanly instead of leaving a lock behind.

  * The loop never dies on an error. An unexpected exception in one cycle is
    logged with its traceback and the loop continues. A poller that exits at
    3am because of one bad response is a poller that was not worth writing.

Usage:

    python -m ingest.run_loop
    python -m ingest.run_loop --interval 30 --feeds all
    python -m ingest.run_loop --max-cycles 5      # useful for a smoke test
"""

from __future__ import annotations

import argparse
import logging
import signal
import time
from datetime import datetime, timezone
from types import FrameType

from ingest.config import POLL_INTERVAL_SECONDS, resolve_feeds
from ingest.fetch import build_session
from ingest.logging_setup import configure_logging
from ingest.run_once import run_cycle
from ingest.storage import initialize_schema, open_db, summarize_database

logger = logging.getLogger("pulselake.loop")


class GracefulShutdown:
    """
    Flips to stopped when the process receives SIGINT or SIGTERM.

    The loop checks this between cycles and during the sleep, so a Ctrl+C is
    honoured within about a second without interrupting a database write that
    is already in progress.
    """

    def __init__(self) -> None:
        self.stopped = False
        signal.signal(signal.SIGINT, self._handle)
        signal.signal(signal.SIGTERM, self._handle)

    def _handle(self, signum: int, frame: FrameType | None) -> None:
        name = signal.Signals(signum).name
        if self.stopped:
            # A second Ctrl+C means the user is impatient. Respect that.
            logger.warning("Received %s again. Exiting immediately.", name)
            raise SystemExit(130)
        logger.info("Received %s. Finishing the current cycle then stopping.", name)
        self.stopped = True

    def sleep(self, seconds: float) -> None:
        """Sleep in short slices so a shutdown signal is noticed promptly."""
        deadline = time.monotonic() + seconds
        while not self.stopped:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(1.0, remaining))


def run_forever(
    interval_seconds: int,
    feeds: dict[str, str],
    db_path: str | None = None,
    max_cycles: int | None = None,
) -> dict:
    """
    Poll every feed on a fixed cadence.

    Args:
        interval_seconds: target gap between the start of each cycle
        feeds:            mapping of feed key to URL
        db_path:          override the DuckDB location, mainly for tests
        max_cycles:       stop after this many cycles. None means run until
                          interrupted. Having a bound makes the loop testable
                          and gives you a quick smoke test.

    Returns cumulative counters for the whole run.
    """
    shutdown = GracefulShutdown()
    session = build_session()
    totals = {"cycles": 0, "inserted": 0, "duplicate": 0, "failed": 0, "rows": 0}

    logger.info(
        "Starting ingestion loop: feeds=%s interval=%ds max_cycles=%s",
        ",".join(feeds),
        interval_seconds,
        max_cycles if max_cycles is not None else "unlimited",
    )

    with open_db(db_path) as con:
        initialize_schema(con)

        while not shutdown.stopped:
            cycle_started = time.monotonic()
            totals["cycles"] += 1

            try:
                counters = run_cycle(con, feeds, session=session)
                for key in ("inserted", "duplicate", "failed", "rows"):
                    totals[key] += counters[key]
            except Exception:
                # Catching bare Exception is normally a smell. Here it is the
                # point: this is the top of a long running process, and the
                # alternative is the loop dying on an error nobody predicted.
                totals["failed"] += 1
                logger.exception("Unhandled error during cycle %d", totals["cycles"])

            if max_cycles is not None and totals["cycles"] >= max_cycles:
                logger.info("Reached max_cycles=%d. Stopping.", max_cycles)
                break

            # Subtract the work time so cycle starts stay on a fixed cadence
            # instead of drifting later by the duration of every cycle.
            elapsed = time.monotonic() - cycle_started
            remaining = interval_seconds - elapsed
            if remaining > 0:
                shutdown.sleep(remaining)
            else:
                logger.warning(
                    "Cycle %d took %.1fs, longer than the %ds interval. "
                    "Starting the next one immediately.",
                    totals["cycles"],
                    elapsed,
                    interval_seconds,
                )

        stats = summarize_database(con)

    logger.info(
        "Loop finished at %s after %d cycles: inserted=%d duplicate=%d "
        "failed=%d new_rows=%d",
        datetime.now(timezone.utc).isoformat(timespec="seconds"),
        totals["cycles"],
        totals["inserted"],
        totals["duplicate"],
        totals["failed"],
        totals["rows"],
    )
    logger.info(
        "database now holds %d snapshots, %d vehicle rows, %d stop updates",
        stats["raw_feed_fetches"],
        stats["raw_vehicle_positions"],
        stats["raw_trip_stop_updates"],
    )
    return totals


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--interval",
        type=int,
        default=POLL_INTERVAL_SECONDS,
        help=f"Seconds between cycle starts. Default {POLL_INTERVAL_SECONDS}.",
    )
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
        "--max-cycles",
        type=int,
        default=None,
        help="Stop after this many cycles. Omit to run until interrupted.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Log to the file only, not the console.",
    )
    args = parser.parse_args(argv)

    configure_logging(to_console=not args.quiet)

    if args.interval < 5:
        logger.error(
            "Interval %ds is too aggressive. MTA publishes every few seconds "
            "and there is no value in polling faster. Use 15 or more.",
            args.interval,
        )
        return 2

    try:
        feeds = resolve_feeds(args.feeds)
    except ValueError as exc:
        logger.error("Configuration error: %s", exc)
        return 2

    totals = run_forever(
        interval_seconds=args.interval,
        feeds=feeds,
        db_path=args.db,
        max_cycles=args.max_cycles,
    )

    if totals["failed"] and not (totals["inserted"] or totals["duplicate"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
