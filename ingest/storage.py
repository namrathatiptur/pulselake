"""
Persistence layer: write decoded snapshots into a local DuckDB file.

Design decisions worth being able to defend in an interview:

1. One DuckDB file acts as both the landing zone and the warehouse. At this
   scale that is the honest choice. Splitting storage from compute is a real
   concern at terabytes, not at a laptop full of subway trains.

2. Raw tables are append only and keep the original feed values. Nothing is
   cleaned on the way in. Cleaning happens in dbt, where it is version
   controlled, testable, and rerunnable against history. If the cleaning logic
   turns out to be wrong, the raw data is still there to rebuild from.

3. Deduplication uses the feed's own header timestamp, not our fetch time.
   MTA republishes roughly every 30 seconds. If the poller runs faster than
   that, or a retry succeeds after we already stored the snapshot, we would
   otherwise insert the same 7000 rows twice and quietly double every count on
   the dashboard. A deterministic fetch_id derived from
   (feed_key, header_timestamp) makes reingestion idempotent.

4. Every write happens in one transaction per snapshot. A crash halfway
   through leaves no partial snapshot behind, so a fetch row in
   raw_feed_fetches always implies its detail rows are present.

5. raw_feed_fetches doubles as an ingestion audit log. It records what was
   polled, when, how big it was, and how many rows landed. That is what lets
   you answer "did we silently stop receiving data" instead of guessing.

A grain warning, found by querying the stored data rather than by assuming:

    trip_id is NOT unique within a snapshot.

Roughly two trains per snapshot on the numbered lines share a trip_id with
another train that is at a completely different station. For example
102550_7..S appeared twice in one feed, as entity 000445 at stop 723S and
entity 000447 at stop 719S. These are two physical trains that NYCT has
assigned the same trip identifier.

The unique key within a snapshot is entity_id, not trip_id:

    raw_vehicle_positions   unique on (fetch_id, entity_id)
    raw_trip_stop_updates   unique on (fetch_id, entity_id, stop_index)

Building the models on trip_id would double count those trains. The dbt layer
tests the correct grain and also tracks how often the reuse happens, because
it is a genuine upstream data quality signal and not noise to be swept away.
"""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from ingest.config import DUCKDB_PATH, ensure_directories
from ingest.parse import ParsedSnapshot

logger = logging.getLogger(__name__)


# Column order is declared once per table and reused for both the CREATE TABLE
# and the INSERT, so the two can never drift apart.
VEHICLE_COLUMNS = [
    "fetch_id",
    "feed_key",
    "fetched_at",
    "header_timestamp",
    "entity_id",
    "trip_id",
    "route_id",
    "start_date",
    "direction",
    "origin_departure_seconds",
    "trip_path",
    "current_stop_sequence",
    "current_status",
    "stop_id",
    "vehicle_timestamp",
]

TRIP_STOP_COLUMNS = [
    "fetch_id",
    "feed_key",
    "fetched_at",
    "header_timestamp",
    "entity_id",
    "trip_id",
    "route_id",
    "start_date",
    "direction",
    "origin_departure_seconds",
    "trip_path",
    "stop_index",
    "stop_id",
    "arrival_time",
    "departure_time",
    "schedule_relationship",
]

ALERT_COLUMNS = [
    "fetch_id",
    "feed_key",
    "fetched_at",
    "header_timestamp",
    "entity_id",
    "alert_header",
    "trip_id",
    "route_id",
    "start_date",
    "direction",
    "origin_departure_seconds",
    "trip_path",
]


SCHEMA_STATEMENTS = [
    # One row per successfully ingested snapshot. This is the audit table.
    """
    CREATE TABLE IF NOT EXISTS raw_feed_fetches (
        fetch_id            VARCHAR PRIMARY KEY,
        feed_key            VARCHAR      NOT NULL,
        feed_url            VARCHAR      NOT NULL,
        fetched_at          TIMESTAMPTZ  NOT NULL,
        header_timestamp    TIMESTAMPTZ,
        payload_bytes       BIGINT,
        entity_count        INTEGER,
        vehicle_rows        INTEGER,
        trip_stop_rows      INTEGER,
        alert_rows          INTEGER,
        -- How many times we polled and got this same snapshot back. A value
        -- above 1 means we are polling faster than MTA publishes.
        poll_count          INTEGER      NOT NULL DEFAULT 1,
        last_polled_at      TIMESTAMPTZ  NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_vehicle_positions (
        fetch_id                    VARCHAR     NOT NULL,
        feed_key                    VARCHAR     NOT NULL,
        fetched_at                  TIMESTAMPTZ NOT NULL,
        header_timestamp            TIMESTAMPTZ,
        entity_id                   VARCHAR,
        trip_id                     VARCHAR,
        route_id                    VARCHAR,
        start_date                  VARCHAR,
        direction                   VARCHAR,
        origin_departure_seconds    INTEGER,
        trip_path                   VARCHAR,
        current_stop_sequence       INTEGER,
        current_status              VARCHAR,
        stop_id                     VARCHAR,
        vehicle_timestamp           TIMESTAMPTZ
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_trip_stop_updates (
        fetch_id                    VARCHAR     NOT NULL,
        feed_key                    VARCHAR     NOT NULL,
        fetched_at                  TIMESTAMPTZ NOT NULL,
        header_timestamp            TIMESTAMPTZ,
        entity_id                   VARCHAR,
        trip_id                     VARCHAR,
        route_id                    VARCHAR,
        start_date                  VARCHAR,
        direction                   VARCHAR,
        origin_departure_seconds    INTEGER,
        trip_path                   VARCHAR,
        stop_index                  INTEGER,
        stop_id                     VARCHAR,
        arrival_time                TIMESTAMPTZ,
        departure_time              TIMESTAMPTZ,
        schedule_relationship       VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_alerts (
        fetch_id                    VARCHAR     NOT NULL,
        feed_key                    VARCHAR     NOT NULL,
        fetched_at                  TIMESTAMPTZ NOT NULL,
        header_timestamp            TIMESTAMPTZ,
        entity_id                   VARCHAR,
        alert_header                VARCHAR,
        trip_id                     VARCHAR,
        route_id                    VARCHAR,
        start_date                  VARCHAR,
        direction                   VARCHAR,
        origin_departure_seconds    INTEGER,
        trip_path                   VARCHAR
    )
    """,
    # One row per failed poll. Failures are data too: without this table a
    # gap in the raw tables is indistinguishable from "no trains were running".
    """
    CREATE TABLE IF NOT EXISTS ingest_errors (
        occurred_at     TIMESTAMPTZ NOT NULL,
        feed_key        VARCHAR     NOT NULL,
        feed_url        VARCHAR,
        error_type      VARCHAR     NOT NULL,
        error_message   VARCHAR
    )
    """,
]


def make_fetch_id(feed_key: str, header_timestamp: datetime | None,
                  fallback: datetime) -> str:
    """
    Build a deterministic id for a snapshot.

    Derived from the feed's own published timestamp so that fetching the same
    snapshot twice produces the same id and the second write is a no op.

    If a feed ever omits its header timestamp we fall back to our fetch time,
    which is unique per poll. That gives up deduplication for that one row
    rather than collapsing every timestamp-less snapshot into a single id.
    """
    anchor = header_timestamp.isoformat() if header_timestamp else f"nohdr:{fallback.isoformat()}"
    digest = hashlib.sha1(f"{feed_key}|{anchor}".encode("utf-8")).hexdigest()
    return digest[:16]


def connect(
    db_path: Path | str | None = None,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating the data directory if needed."""
    ensure_directories()
    path = Path(db_path) if db_path else DUCKDB_PATH
    return duckdb.connect(str(path), read_only=read_only)


def connect_with_retry(
    db_path: Path | str | None = None,
    read_only: bool = False,
    timeout_seconds: float = 30.0,
    poll_seconds: float = 0.5,
) -> duckdb.DuckDBPyConnection:
    """
    Open a connection, waiting for another process to release the file lock.

    DuckDB takes an exclusive lock on the database file. While the ingestion
    loop holds it, no other process can open the file at all, not even read
    only. That is a real constraint of an embedded database and it is why the
    loop closes its connection between cycles rather than holding it open for
    the whole run.

    The gap between cycles is still only a few tens of seconds wide, so
    readers need to be willing to wait for their turn instead of failing on
    the first attempt. dbt gets the same behaviour from the `retries` block in
    transform/profiles.yml.
    """
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    while True:
        attempt += 1
        try:
            return connect(db_path, read_only=read_only)
        except (duckdb.IOException, duckdb.Error) as exc:
            if "lock" not in str(exc).lower() or time.monotonic() >= deadline:
                raise
            if attempt == 1:
                logger.info(
                    "Database is locked by another process. Waiting up to %.0fs "
                    "for it to be released.",
                    timeout_seconds,
                )
            time.sleep(poll_seconds)


@contextmanager
def open_db(
    db_path: Path | str | None = None,
    read_only: bool = False,
    timeout_seconds: float = 30.0,
):
    """
    Context manager that waits for the file lock and always closes.

    Retrying applies to the writer as well as to readers. The dashboard opens
    a read only connection every few seconds, and while it holds one the
    poller cannot write. Both sides waiting their turn is what lets the
    ingestion loop, dbt, and the dashboard all run at the same time against a
    single embedded database.
    """
    con = connect_with_retry(
        db_path, read_only=read_only, timeout_seconds=timeout_seconds
    )
    try:
        yield con
    finally:
        con.close()


def initialize_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create every table if it does not already exist.

    Safe to call on every run. Cheap, and it means a fresh clone of the repo
    works without a separate migration step.
    """
    for statement in SCHEMA_STATEMENTS:
        con.execute(statement)


def _rows_for(columns: list[str], records: list[dict], header: dict) -> list[tuple]:
    """
    Turn parsed dictionaries into tuples matching a table's column order.

    Values from `header` (fetch_id, feed_key, and the two timestamps) are the
    same for every row in a snapshot, so they are merged in here rather than
    being duplicated into every record by the parser.
    """
    return [
        tuple(header.get(col, record.get(col)) for col in columns)
        for record in records
    ]


def _insert_many(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    rows: list[tuple],
) -> None:
    """Bulk insert, skipping the round trip entirely when there is nothing to write."""
    if not rows:
        return
    placeholders = ", ".join("?" for _ in columns)
    column_list = ", ".join(columns)
    con.executemany(
        f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})",
        rows,
    )


def store_snapshot(
    con: duckdb.DuckDBPyConnection,
    snapshot: ParsedSnapshot,
) -> dict:
    """
    Write one parsed snapshot to DuckDB inside a single transaction.

    Returns a small result dictionary the caller can log:
        {"status": "inserted" | "duplicate", "fetch_id": ..., "rows": int}

    A "duplicate" result is not an error. It means MTA has not published a new
    snapshot since the last poll, which is completely normal when the poll
    interval is shorter than the publish interval.
    """
    fetch_id = make_fetch_id(
        snapshot.feed_key, snapshot.header_timestamp, snapshot.fetched_at
    )
    now = datetime.now(timezone.utc)

    existing = con.execute(
        "SELECT poll_count FROM raw_feed_fetches WHERE fetch_id = ?", [fetch_id]
    ).fetchone()

    if existing is not None:
        # Already stored. Record that we saw it again, but do not reinsert the
        # detail rows. This is what makes the script safe to run on a loop.
        con.execute(
            """
            UPDATE raw_feed_fetches
               SET poll_count = poll_count + 1,
                   last_polled_at = ?
             WHERE fetch_id = ?
            """,
            [now, fetch_id],
        )
        return {
            "status": "duplicate",
            "fetch_id": fetch_id,
            "rows": 0,
            "poll_count": existing[0] + 1,
        }

    header = {
        "fetch_id": fetch_id,
        "feed_key": snapshot.feed_key,
        "fetched_at": snapshot.fetched_at,
        "header_timestamp": snapshot.header_timestamp,
    }

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            """
            INSERT INTO raw_feed_fetches (
                fetch_id, feed_key, feed_url, fetched_at, header_timestamp,
                payload_bytes, entity_count, vehicle_rows, trip_stop_rows,
                alert_rows, poll_count, last_polled_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            """,
            [
                fetch_id,
                snapshot.feed_key,
                snapshot.feed_url,
                snapshot.fetched_at,
                snapshot.header_timestamp,
                snapshot.payload_bytes,
                snapshot.entity_count,
                len(snapshot.vehicle_positions),
                len(snapshot.trip_stop_updates),
                len(snapshot.alerts),
                now,
            ],
        )

        _insert_many(
            con,
            "raw_vehicle_positions",
            VEHICLE_COLUMNS,
            _rows_for(VEHICLE_COLUMNS, snapshot.vehicle_positions, header),
        )
        _insert_many(
            con,
            "raw_trip_stop_updates",
            TRIP_STOP_COLUMNS,
            _rows_for(TRIP_STOP_COLUMNS, snapshot.trip_stop_updates, header),
        )
        _insert_many(
            con,
            "raw_alerts",
            ALERT_COLUMNS,
            _rows_for(ALERT_COLUMNS, snapshot.alerts, header),
        )

        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    return {
        "status": "inserted",
        "fetch_id": fetch_id,
        "rows": snapshot.row_count,
        "poll_count": 1,
    }


def record_error(
    con: duckdb.DuckDBPyConnection,
    feed_key: str,
    feed_url: str | None,
    error_type: str,
    error_message: str,
) -> None:
    """
    Log a failed poll to the database as well as to the log file.

    Keeping failures queryable next to the data means a dashboard can show
    "last successful fetch was 14 minutes ago" instead of showing a stale
    number with no warning.
    """
    con.execute(
        """
        INSERT INTO ingest_errors (
            occurred_at, feed_key, feed_url, error_type, error_message
        ) VALUES (?, ?, ?, ?, ?)
        """,
        [datetime.now(timezone.utc), feed_key, feed_url, error_type, error_message[:1000]],
    )


def summarize_database(con: duckdb.DuckDBPyConnection) -> dict:
    """Return a few headline numbers about what is currently stored."""
    tables = [
        "raw_feed_fetches",
        "raw_vehicle_positions",
        "raw_trip_stop_updates",
        "raw_alerts",
        "ingest_errors",
    ]
    counts = {
        table: con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        for table in tables
    }

    window = con.execute(
        """
        SELECT min(fetched_at), max(fetched_at), count(DISTINCT feed_key)
          FROM raw_feed_fetches
        """
    ).fetchone()

    counts["first_fetch_at"] = window[0]
    counts["last_fetch_at"] = window[1]
    counts["distinct_feeds"] = window[2]
    return counts
