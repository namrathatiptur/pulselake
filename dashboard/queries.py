"""
Every database read the dashboard performs, in one place.

Two reasons this is a separate module from app.py:

1. The queries can be run and checked without launching Streamlit, which
   makes debugging a wrong number much faster than clicking through a UI.

2. It keeps the SQL out of the layout code. The dashboard file then reads as
   a description of the page rather than a mixture of presentation and data
   access.

Every function opens a read only connection, reads, and closes immediately.
Holding a connection open would block the ingestion loop from writing, since
DuckDB takes an exclusive lock on the file.
"""

from __future__ import annotations

import pandas as pd

from ingest.config import DUCKDB_PATH
from ingest.storage import open_db

# The dbt marts the dashboard depends on. Checked up front so a missing model
# produces a clear "run dbt" message rather than a raw SQL error.
REQUIRED_MODELS = [
    "fct_current_trains",
    "fct_line_activity",
    "fct_prediction_drift",
    "dq_ingestion_health",
]


class ModelsNotBuilt(Exception):
    """Raised when the dbt models have not been created yet."""


def database_exists() -> bool:
    return DUCKDB_PATH.exists()


def _read(sql: str, params: list | None = None) -> pd.DataFrame:
    with open_db(read_only=True, timeout_seconds=20) as con:
        return con.execute(sql, params or []).df()


def missing_models() -> list[str]:
    """Return the names of any required dbt model that does not exist yet."""
    with open_db(read_only=True, timeout_seconds=20) as con:
        existing = {
            row[0]
            for row in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
    return [name for name in REQUIRED_MODELS if name not in existing]


def check_ready() -> None:
    """Raise ModelsNotBuilt if the dashboard cannot render yet."""
    missing = missing_models()
    if missing:
        raise ModelsNotBuilt(", ".join(missing))


def load_line_activity() -> pd.DataFrame:
    return _read(
        """
        SELECT
            route_id,
            trains_running,
            trains_northbound,
            trains_southbound,
            trains_at_station,
            trains_in_transit,
            trains_arriving,
            trains_delayed,
            pct_delayed,
            median_minutes_to_next_stop,
            max_minutes_to_next_stop,
            trains_missing_prediction,
            trains_with_reused_trip_id,
            snapshot_at
        FROM fct_line_activity
        ORDER BY route_id
        """
    )


def load_current_trains() -> pd.DataFrame:
    return _read(
        """
        SELECT
            route_id            AS line,
            direction,
            status_label        AS status,
            current_stop_id     AS at_stop,
            following_stop_id   AS next_stop,
            minutes_to_next_stop,
            trip_id,
            has_reused_trip_id,
            alert_text,
            snapshot_at
        FROM fct_current_trains
        ORDER BY route_id, direction, minutes_to_next_stop
        """
    )


def load_drift_by_line() -> pd.DataFrame:
    """
    Median prediction drift per line, plus how many trips it is based on.

    Aggregated to the trip first so a train with thirty observed stops does
    not outvote a train with three.
    """
    return _read(
        """
        WITH per_trip AS (
            SELECT
                route_id,
                trip_key,
                any_value(trip_median_drift_minutes) AS trip_drift_minutes
            FROM fct_prediction_drift
            WHERE route_id IS NOT NULL
            GROUP BY 1, 2
        )
        SELECT
            route_id,
            count(*)                                    AS trips_observed,
            round(median(trip_drift_minutes), 2)        AS median_drift_minutes,
            count(*) FILTER (WHERE trip_drift_minutes >  2) AS trips_losing_time,
            count(*) FILTER (WHERE trip_drift_minutes < -2) AS trips_making_up_time
        FROM per_trip
        GROUP BY 1
        ORDER BY 1
        """
    )


def load_drift_leaders(limit: int = 10) -> pd.DataFrame:
    """The trips whose predictions have slipped the most."""
    return _read(
        """
        SELECT
            trip_id,
            route_id                    AS line,
            direction,
            trip_stops_observed         AS stops_observed,
            trip_median_drift_minutes   AS drift_minutes
        FROM fct_prediction_drift
        GROUP BY ALL
        ORDER BY drift_minutes DESC
        LIMIT ?
        """,
        [limit],
    )


def load_pipeline_health(limit: int = 60) -> pd.DataFrame:
    return _read(
        """
        SELECT
            snapshot_at,
            feed_key,
            vehicle_rows,
            trip_stop_rows,
            delay_alert_rows,
            poll_count,
            seconds_since_previous_snapshot,
            pct_rows_with_reused_trip_id,
            pct_rows_with_unparsed_trip_id,
            avg_ingest_lag_seconds,
            avg_position_age_seconds,
            payload_bytes,
            is_after_ingestion_gap
        FROM dq_ingestion_health
        ORDER BY snapshot_at DESC
        LIMIT ?
        """,
        [limit],
    )


def load_freshness() -> dict:
    """
    How current the data is, and whether ingestion looks alive.

    This is the first thing the page renders, because every other number on
    it is meaningless if this one is bad. A stale dashboard and a quiet
    system look identical unless something checks the clock.
    """
    row = _read(
        """
        SELECT
            max(header_timestamp)                                   AS latest_snapshot_at,
            max(last_polled_at)                                     AS last_poll_at,
            count(*)                                                AS total_snapshots,
            count(DISTINCT feed_key)                                AS feeds
        FROM raw_feed_fetches
        """
    ).iloc[0]

    errors = _read(
        """
        SELECT count(*) AS recent_errors
        FROM ingest_errors
        WHERE occurred_at > now() - INTERVAL 15 MINUTE
        """
    ).iloc[0]

    now = pd.Timestamp.now(tz="UTC")
    latest = row["latest_snapshot_at"]
    age_seconds = None
    if pd.notna(latest):
        age_seconds = (now - pd.Timestamp(latest).tz_convert("UTC")).total_seconds()

    return {
        "latest_snapshot_at": latest,
        "last_poll_at": row["last_poll_at"],
        "total_snapshots": int(row["total_snapshots"]),
        "feeds": int(row["feeds"]),
        "age_seconds": age_seconds,
        "recent_errors": int(errors["recent_errors"]),
    }


def load_model_staleness() -> dict:
    """
    How far the dbt models lag behind the raw data.

    These are two different clocks and conflating them is an easy way to
    mislead yourself. Ingestion runs on a loop; dbt runs when somebody runs
    it. So the raw tables can be twenty seconds old while the marts the
    dashboard reads are twenty minutes old, and every number on the page is
    correct as of a time that is not now.

    Surfacing the gap is the difference between a dashboard that is stale and
    a dashboard that is stale and says so.
    """
    row = _read(
        """
        SELECT
            (SELECT max(header_timestamp) FROM raw_feed_fetches) AS raw_latest_at,
            (SELECT max(snapshot_at) FROM fct_current_trains)    AS model_latest_at
        """
    ).iloc[0]

    raw_latest = row["raw_latest_at"]
    model_latest = row["model_latest_at"]

    lag_seconds = None
    if pd.notna(raw_latest) and pd.notna(model_latest):
        lag_seconds = (
            pd.Timestamp(raw_latest).tz_convert("UTC")
            - pd.Timestamp(model_latest).tz_convert("UTC")
        ).total_seconds()

    return {
        "raw_latest_at": raw_latest,
        "model_latest_at": model_latest,
        "lag_seconds": lag_seconds,
    }


def load_recent_errors(limit: int = 20) -> pd.DataFrame:
    return _read(
        """
        SELECT occurred_at, feed_key, error_type, error_message
        FROM ingest_errors
        ORDER BY occurred_at DESC
        LIMIT ?
        """,
        [limit],
    )
