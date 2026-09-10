"""
Unit tests for the DuckDB storage layer.

Each test gets its own throwaway database file under pytest's tmp_path, so
nothing here touches data/pulselake.duckdb and the tests can run in any order.

The bulk of these are about idempotency, because "run this repeatedly without
duplicating or crashing" is a correctness requirement of the pipeline rather
than a nice-to-have, and it is impossible to eyeball once the tables hold a
hundred thousand rows.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ingest.parse import parse_feed
from ingest.storage import (
    initialize_schema,
    make_fetch_id,
    open_db,
    record_error,
    store_snapshot,
    summarize_database,
)

NOW = datetime(2026, 9, 10, 21, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(temp_db):
    """An initialised, empty database."""
    with open_db(temp_db) as con:
        initialize_schema(con)
        yield con


def make_snapshot(feed_builder, header_timestamp=1789075148, fetched_at=NOW, **kwargs):
    payload = feed_builder(header_timestamp=header_timestamp, **kwargs)
    return parse_feed(payload, "TEST", "http://feed", fetched_at)


DEFAULT_CONTENT = {
    "vehicles": [
        {"trip_id": "098150_1..N15R", "route_id": "1", "stop_id": "101N"},
        {"trip_id": "098250_1..S15R", "route_id": "1", "stop_id": "140S"},
    ],
    "trip_updates": [
        {
            "trip_id": "098150_1..N15R",
            "stops": [
                {"stop_id": "101N", "arrival": 1789075200},
                {"stop_id": "103N", "arrival": 1789075500},
            ],
        }
    ],
    "alerts": [
        {"header_text": "Train delayed", "informed": [{"trip_id": "098150_1..N15R"}]}
    ],
}


# ---------------------------------------------------------------------------
# Fetch ids
# ---------------------------------------------------------------------------


def test_fetch_id_is_deterministic():
    header = datetime(2026, 9, 10, 21, 19, 8, tzinfo=timezone.utc)
    assert make_fetch_id("A", header, NOW) == make_fetch_id("A", header, NOW)


def test_fetch_id_ignores_our_fetch_time():
    """
    The id must key on when MTA published, not on when we asked. Otherwise
    two polls of the same unchanged snapshot would produce different ids and
    the deduplication would never fire.
    """
    header = datetime(2026, 9, 10, 21, 19, 8, tzinfo=timezone.utc)
    later = NOW + timedelta(minutes=5)
    assert make_fetch_id("A", header, NOW) == make_fetch_id("A", header, later)


def test_fetch_id_differs_by_feed_and_by_snapshot():
    header = datetime(2026, 9, 10, 21, 19, 8, tzinfo=timezone.utc)
    other = datetime(2026, 9, 10, 21, 19, 38, tzinfo=timezone.utc)

    assert make_fetch_id("A", header, NOW) != make_fetch_id("B", header, NOW)
    assert make_fetch_id("A", header, NOW) != make_fetch_id("A", other, NOW)


def test_fetch_id_falls_back_to_fetch_time_when_header_is_missing():
    """
    A feed with no header timestamp gives up deduplication for that row,
    which is better than collapsing every timestamp-less snapshot into one id
    and silently discarding all but the first.
    """
    later = NOW + timedelta(seconds=30)
    assert make_fetch_id("A", None, NOW) != make_fetch_id("A", None, later)


# ---------------------------------------------------------------------------
# Writing
# ---------------------------------------------------------------------------


def test_store_snapshot_writes_every_table(db, feed_builder):
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)
    result = store_snapshot(db, snapshot)

    assert result["status"] == "inserted"
    assert result["rows"] == snapshot.row_count

    counts = summarize_database(db)
    assert counts["raw_feed_fetches"] == 1
    assert counts["raw_vehicle_positions"] == 2
    assert counts["raw_trip_stop_updates"] == 2
    assert counts["raw_alerts"] == 1


def test_stored_values_survive_the_round_trip(db, feed_builder):
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)
    store_snapshot(db, snapshot)

    row = db.execute(
        """
        SELECT trip_id, route_id, direction, origin_departure_seconds,
               current_status, stop_id
        FROM raw_vehicle_positions
        WHERE trip_id = '098150_1..N15R'
        """
    ).fetchone()

    assert row == ("098150_1..N15R", "1", "N", 58890, "STOPPED_AT", "101N")


def test_header_timestamp_is_stored_as_utc(db, feed_builder):
    snapshot = make_snapshot(feed_builder, header_timestamp=1789075148, **DEFAULT_CONTENT)
    store_snapshot(db, snapshot)

    stored = db.execute("SELECT header_timestamp FROM raw_feed_fetches").fetchone()[0]
    assert stored.astimezone(timezone.utc) == datetime(
        2026, 9, 10, 21, 19, 8, tzinfo=timezone.utc
    )


# ---------------------------------------------------------------------------
# Idempotency, which is the point of the whole module
# ---------------------------------------------------------------------------


def test_storing_the_same_snapshot_twice_does_not_duplicate_rows(db, feed_builder):
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)

    first = store_snapshot(db, snapshot)
    second = store_snapshot(db, snapshot)

    assert first["status"] == "inserted"
    assert second["status"] == "duplicate"
    assert second["rows"] == 0

    counts = summarize_database(db)
    assert counts["raw_feed_fetches"] == 1
    assert counts["raw_vehicle_positions"] == 2
    assert counts["raw_trip_stop_updates"] == 2


def test_duplicate_poll_increments_the_poll_counter(db, feed_builder):
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)

    store_snapshot(db, snapshot)
    store_snapshot(db, snapshot)
    result = store_snapshot(db, snapshot)

    assert result["poll_count"] == 3
    stored = db.execute("SELECT poll_count FROM raw_feed_fetches").fetchone()[0]
    assert stored == 3


def test_a_later_fetch_of_the_same_snapshot_is_still_a_duplicate(db, feed_builder):
    """
    Same published snapshot, fetched five minutes apart. Our fetch time
    differs but MTA's header timestamp does not, so this must dedupe.
    """
    first = make_snapshot(feed_builder, fetched_at=NOW, **DEFAULT_CONTENT)
    second = make_snapshot(
        feed_builder, fetched_at=NOW + timedelta(minutes=5), **DEFAULT_CONTENT
    )

    assert store_snapshot(db, first)["status"] == "inserted"
    assert store_snapshot(db, second)["status"] == "duplicate"


def test_a_new_published_snapshot_is_inserted(db, feed_builder):
    first = make_snapshot(feed_builder, header_timestamp=1789075148, **DEFAULT_CONTENT)
    second = make_snapshot(feed_builder, header_timestamp=1789075178, **DEFAULT_CONTENT)

    assert store_snapshot(db, first)["status"] == "inserted"
    assert store_snapshot(db, second)["status"] == "inserted"

    counts = summarize_database(db)
    assert counts["raw_feed_fetches"] == 2
    assert counts["raw_vehicle_positions"] == 4


def test_the_same_snapshot_from_two_feeds_is_not_a_duplicate(db, feed_builder):
    """
    Two feeds can publish with the same header timestamp. They are different
    snapshots and both must land, which is why feed_key is part of the id.
    """
    payload = feed_builder(header_timestamp=1789075148, **DEFAULT_CONTENT)
    a = parse_feed(payload, "FEED_A", "http://a", NOW)
    b = parse_feed(payload, "FEED_B", "http://b", NOW)

    assert store_snapshot(db, a)["status"] == "inserted"
    assert store_snapshot(db, b)["status"] == "inserted"
    assert summarize_database(db)["raw_feed_fetches"] == 2


# ---------------------------------------------------------------------------
# Failure handling
# ---------------------------------------------------------------------------


def test_a_failed_write_leaves_no_partial_snapshot(db, feed_builder, monkeypatch):
    """
    A snapshot is written in one transaction, so a crash partway through must
    roll the whole thing back. Otherwise a fetch row would exist in the audit
    table implying detail rows that are not there, and every downstream count
    would be quietly short.
    """
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)

    import ingest.storage as storage

    original = storage._insert_many
    calls = {"n": 0}

    def explode_on_the_second_table(con, table, columns, rows):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("disk full")
        return original(con, table, columns, rows)

    monkeypatch.setattr(storage, "_insert_many", explode_on_the_second_table)

    with pytest.raises(RuntimeError, match="disk full"):
        store_snapshot(db, snapshot)

    counts = summarize_database(db)
    assert counts["raw_feed_fetches"] == 0, "audit row survived a rolled back write"
    assert counts["raw_vehicle_positions"] == 0
    assert counts["raw_trip_stop_updates"] == 0


def test_retrying_after_a_rollback_succeeds(db, feed_builder, monkeypatch):
    """The rollback must leave the database usable, not just empty."""
    snapshot = make_snapshot(feed_builder, **DEFAULT_CONTENT)

    import ingest.storage as storage

    original = storage._insert_many
    fail = {"yes": True}

    def maybe_explode(con, table, columns, rows):
        if fail["yes"] and table == "raw_trip_stop_updates":
            raise RuntimeError("transient")
        return original(con, table, columns, rows)

    monkeypatch.setattr(storage, "_insert_many", maybe_explode)
    with pytest.raises(RuntimeError):
        store_snapshot(db, snapshot)

    fail["yes"] = False
    result = store_snapshot(db, snapshot)

    assert result["status"] == "inserted"
    assert summarize_database(db)["raw_trip_stop_updates"] == 2


def test_record_error_writes_a_queryable_row(db):
    record_error(db, "TEST", "http://x", "FeedFetchError", "connection refused")

    row = db.execute(
        "SELECT feed_key, error_type, error_message FROM ingest_errors"
    ).fetchone()

    assert row[0] == "TEST"
    assert row[1] == "FeedFetchError"
    assert "connection refused" in row[2]


def test_long_error_messages_are_truncated(db):
    record_error(db, "TEST", "http://x", "Boom", "x" * 5000)
    stored = db.execute("SELECT error_message FROM ingest_errors").fetchone()[0]
    assert len(stored) == 1000


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------


def test_initialize_schema_is_safe_to_run_repeatedly(temp_db, feed_builder):
    """
    The loop calls this on every cycle, so it has to be a no op once the
    tables exist, and it must not wipe data.
    """
    with open_db(temp_db) as con:
        initialize_schema(con)
        store_snapshot(con, make_snapshot(feed_builder, **DEFAULT_CONTENT))
        initialize_schema(con)
        initialize_schema(con)

        assert summarize_database(con)["raw_vehicle_positions"] == 2


def test_summarize_reports_the_time_window(db, feed_builder):
    early = make_snapshot(feed_builder, header_timestamp=1789075148, fetched_at=NOW,
                          **DEFAULT_CONTENT)
    late = make_snapshot(
        feed_builder,
        header_timestamp=1789075600,
        fetched_at=NOW + timedelta(minutes=8),
        **DEFAULT_CONTENT,
    )
    store_snapshot(db, early)
    store_snapshot(db, late)

    stats = summarize_database(db)
    assert stats["distinct_feeds"] == 1
    assert stats["first_fetch_at"] < stats["last_fetch_at"]


def test_empty_feed_stores_an_audit_row_with_no_detail_rows(db, feed_builder):
    """
    An empty but valid snapshot is evidence that we polled and there was
    nothing there, which is different from not having polled. The audit row
    must exist even when no trains do.
    """
    snapshot = make_snapshot(feed_builder)
    result = store_snapshot(db, snapshot)

    assert result["status"] == "inserted"
    counts = summarize_database(db)
    assert counts["raw_feed_fetches"] == 1
    assert counts["raw_vehicle_positions"] == 0
