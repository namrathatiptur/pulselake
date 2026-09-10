"""
Command line preview of the live feed. Fetches, decodes, prints, stores nothing.

This exists so you can confirm the ingestion half works on its own, before any
database is involved. Run it with:

    python -m ingest.preview
    python -m ingest.preview --feeds ACE,L --rows 10
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter

from ingest.config import resolve_feeds
from ingest.fetch import FeedFetchError, build_session, fetch_and_parse
from ingest.parse import FeedParseError, ParsedSnapshot


def summarize(snapshot: ParsedSnapshot, sample_rows: int) -> None:
    """Print a human readable summary of one decoded snapshot."""
    print(f"\n=== feed {snapshot.feed_key} ===")
    print(f"  url                {snapshot.feed_url}")
    print(f"  fetched at         {snapshot.fetched_at.isoformat()}")
    print(f"  feed published at  {snapshot.header_timestamp}")
    print(f"  payload            {snapshot.payload_bytes:,} bytes")
    print(f"  entities           {snapshot.entity_count:,}")
    print(f"  vehicle positions  {len(snapshot.vehicle_positions):,}")
    print(f"  stop time updates  {len(snapshot.trip_stop_updates):,}")
    print(f"  alert rows         {len(snapshot.alerts):,}")

    by_route = Counter(
        row["route_id"] for row in snapshot.vehicle_positions if row["route_id"]
    )
    if by_route:
        pretty = ", ".join(
            f"{route}={count}" for route, count in sorted(by_route.items())
        )
        print(f"  trains per route   {pretty}")

    by_status = Counter(row["current_status"] for row in snapshot.vehicle_positions)
    if by_status:
        pretty = ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
        print(f"  train status mix   {pretty}")

    alert_texts = Counter(
        row["alert_header"] for row in snapshot.alerts if row["alert_header"]
    )
    if alert_texts:
        print("  alerts:")
        for text, count in alert_texts.most_common(5):
            print(f"      {count:>4}  {text}")

    if sample_rows and snapshot.vehicle_positions:
        print(f"\n  first {sample_rows} vehicle rows:")
        for row in snapshot.vehicle_positions[:sample_rows]:
            print(
                f"      route {row['route_id']:<4} "
                f"dir {row['direction'] or '?'}  "
                f"stop {row['stop_id'] or '?':<6} "
                f"{row['current_status']:<14} "
                f"trip {row['trip_id']}"
            )

    if sample_rows and snapshot.trip_stop_updates:
        print(f"\n  first {sample_rows} stop time updates:")
        for row in snapshot.trip_stop_updates[:sample_rows]:
            arrival = row["arrival_time"].strftime("%H:%M:%S") if row["arrival_time"] else "  none  "
            print(
                f"      route {row['route_id']:<4} "
                f"stop {row['stop_id'] or '?':<6} "
                f"idx {row['stop_index']:<3} "
                f"arrival(UTC) {arrival}  "
                f"trip {row['trip_id']}"
            )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--feeds",
        default=None,
        help="Comma separated feed keys, or 'all'. Defaults to the numbered lines feed.",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=5,
        help="How many sample rows to print per section. Use 0 for none.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    try:
        feeds = resolve_feeds(args.feeds)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    session = build_session()
    failures = 0

    for feed_key, feed_url in feeds.items():
        try:
            snapshot = fetch_and_parse(feed_key, feed_url, session=session)
        except FeedFetchError as exc:
            failures += 1
            print(f"\n=== feed {feed_key} ===\n  NETWORK FAILURE: {exc}", file=sys.stderr)
            continue
        except FeedParseError as exc:
            failures += 1
            print(f"\n=== feed {feed_key} ===\n  BAD PAYLOAD: {exc}", file=sys.stderr)
            continue

        summarize(snapshot, args.rows)

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
