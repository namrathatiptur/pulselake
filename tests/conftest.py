"""
Shared test fixtures and builders.

Nothing in the test suite touches the network or the real database. There are
two sources of test data:

  * A captured slice of the real MTA feed, in tests/fixtures/sample_feed.pb.
    Regenerate it with `python -m tests.make_fixture`. Testing against real
    bytes is what catches the difference between the format as documented and
    the format as actually served, which is where the interesting bugs in this
    project have all been.

  * Synthetic FeedMessage builders below, for the edge cases the live feed
    does not conveniently produce on demand: a missing arrival time, an alert
    with no informed entity, an unparseable trip id.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from google.transit import gtfs_realtime_pb2

FIXTURE_DIR = Path(__file__).parent / "fixtures"
SAMPLE_FEED_PATH = FIXTURE_DIR / "sample_feed.pb"

FIXED_NOW = datetime(2026, 9, 10, 21, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def fetched_at() -> datetime:
    """A fixed fetch time, so assertions never depend on the wall clock."""
    return FIXED_NOW


@pytest.fixture
def sample_feed_bytes() -> bytes:
    """A real, captured MTA payload trimmed to seven entities."""
    return SAMPLE_FEED_PATH.read_bytes()


def build_feed(
    header_timestamp: int = 1789075148,
    vehicles: list[dict] | None = None,
    trip_updates: list[dict] | None = None,
    alerts: list[dict] | None = None,
    gtfs_version: str = "1.0",
) -> bytes:
    """
    Build a synthetic GTFS-Realtime payload from plain dictionaries.

    Keeping this a builder rather than a set of frozen fixture files means a
    test can state exactly the one condition it cares about, in the test
    itself, instead of the reader having to go and open a binary file to find
    out what is in it.
    """
    message = gtfs_realtime_pb2.FeedMessage()
    if gtfs_version:
        message.header.gtfs_realtime_version = gtfs_version
    message.header.timestamp = header_timestamp

    counter = 0

    for spec in vehicles or []:
        counter += 1
        entity = message.entity.add()
        entity.id = spec.get("entity_id", f"{counter:06d}")
        vehicle = entity.vehicle
        vehicle.trip.trip_id = spec.get("trip_id", "098150_1..N15R")
        vehicle.trip.route_id = spec.get("route_id", "1")
        vehicle.trip.start_date = spec.get("start_date", "20260910")
        if spec.get("stop_id") is not None:
            vehicle.stop_id = spec["stop_id"]
        vehicle.current_status = spec.get("current_status", 1)
        if spec.get("current_stop_sequence") is not None:
            vehicle.current_stop_sequence = spec["current_stop_sequence"]
        if spec.get("timestamp") is not None:
            vehicle.timestamp = spec["timestamp"]

    for spec in trip_updates or []:
        counter += 1
        entity = message.entity.add()
        entity.id = spec.get("entity_id", f"{counter:06d}")
        trip_update = entity.trip_update
        trip_update.trip.trip_id = spec.get("trip_id", "098150_1..N15R")
        trip_update.trip.route_id = spec.get("route_id", "1")
        trip_update.trip.start_date = spec.get("start_date", "20260910")
        for stop in spec.get("stops", []):
            stu = trip_update.stop_time_update.add()
            if stop.get("stop_id") is not None:
                stu.stop_id = stop["stop_id"]
            if stop.get("arrival") is not None:
                stu.arrival.time = stop["arrival"]
            if stop.get("departure") is not None:
                stu.departure.time = stop["departure"]
            if stop.get("schedule_relationship") is not None:
                stu.schedule_relationship = stop["schedule_relationship"]

    for spec in alerts or []:
        counter += 1
        entity = message.entity.add()
        entity.id = spec.get("entity_id", f"{counter:06d}")
        alert = entity.alert
        if spec.get("header_text") is not None:
            translation = alert.header_text.translation.add()
            translation.text = spec["header_text"]
            translation.language = "en"
        for informed in spec.get("informed", []):
            entry = alert.informed_entity.add()
            if informed.get("trip_id") is not None:
                entry.trip.trip_id = informed["trip_id"]
            if informed.get("route_id") is not None:
                entry.route_id = informed["route_id"]

    # SerializePartialToString rather than SerializeToString, because
    # gtfs_realtime_version is a required field and the builder needs to be
    # able to omit it to exercise the "decoded but is not our feed" case.
    #
    # Worth knowing: protobuf enforces required fields on serialization but
    # NOT on parsing. A payload with no version parses cleanly into an empty
    # FeedMessage, which is exactly why parse_feed checks for the version
    # explicitly instead of trusting the decode to have failed.
    return message.SerializePartialToString()


@pytest.fixture
def feed_builder():
    return build_feed


@pytest.fixture
def temp_db(tmp_path) -> Path:
    """A throwaway DuckDB path, unique per test."""
    return tmp_path / "test_pulselake.duckdb"
