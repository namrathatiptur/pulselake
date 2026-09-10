"""
Unit tests for the protobuf decoding layer.

No network, no database. These are the tests that would run on every commit in
CI, and they are the reason parse.py contains no I/O.

Several of these encode bugs that actually happened while building this
project. Those are marked, because a test that documents a real failure is
worth more than a test that documents an intention.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from ingest.parse import (
    FeedParseError,
    epoch_to_utc,
    parse_feed,
    parse_nyct_trip_id,
)


# ---------------------------------------------------------------------------
# Timestamp handling
# ---------------------------------------------------------------------------


def test_epoch_to_utc_converts_to_aware_datetime():
    result = epoch_to_utc(1789075148)
    assert result == datetime(2026, 9, 10, 21, 19, 8, tzinfo=timezone.utc)
    assert result.tzinfo is not None


@pytest.mark.parametrize("missing", [0, None])
def test_epoch_to_utc_treats_absent_as_none_not_1970(missing):
    """
    Protobuf scalars default to 0 rather than null, so an absent timestamp
    arrives as 0. Converting that literally would put trains in 1970 and make
    every downstream freshness calculation wrong by 56 years.
    """
    assert epoch_to_utc(missing) is None


# ---------------------------------------------------------------------------
# NYCT trip id decoding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "trip_id,direction,origin_seconds,path",
    [
        # The documented, common form.
        ("098150_1..N15R", "N", 58890, "N15R"),
        ("102550_7..S15R", "S", 61530, "S15R"),
        # REGRESSION: the 42nd Street shuttle uses ONE dot, not two. The first
        # version of this parser required two and silently returned nulls for
        # every shuttle train.
        ("104150_GS.N04R", "N", 62490, "N04R"),
        ("104450_GS.S04R", "S", 62670, "S04R"),
        # REGRESSION: some 7 trips carry a terminal name where the direction
        # letter should be. Direction is genuinely unknown here, but the
        # scheduled origin is still recoverable and must not be thrown away
        # along with it.
        ("065600_7..MAIN ST34", None, 39360, "MAIN ST34"),
        # Direction letter with nothing after it.
        ("049250_7..S", "S", 29550, "S"),
        # Express route code containing a letter.
        ("050000_7X..S", "S", 30000, "S"),
    ],
)
def test_parse_nyct_trip_id_handles_every_real_format(
    trip_id, direction, origin_seconds, path
):
    result = parse_nyct_trip_id(trip_id)
    assert result["direction"] == direction
    assert result["origin_departure_seconds"] == origin_seconds
    assert result["trip_path"] == path


@pytest.mark.parametrize("bad", ["garbage", "", None, "12_1..N", "abcdef_1..N"])
def test_parse_nyct_trip_id_returns_nulls_rather_than_raising(bad):
    """
    A single malformed trip id must never take down an ingestion run. The feed
    is somebody else's and it will change without warning.
    """
    result = parse_nyct_trip_id(bad)
    assert result == {
        "direction": None,
        "origin_departure_seconds": None,
        "trip_path": None,
    }


def test_origin_departure_decodes_hundredths_of_a_minute():
    """
    The six digit prefix is hundredths of a minute after midnight, which is an
    unusual enough unit to be worth pinning down. 098150 is 981.50 minutes,
    which is 16:21:30, which is 58890 seconds.
    """
    assert parse_nyct_trip_id("098150_1..N")["origin_departure_seconds"] == 58890
    assert 58890 == 16 * 3600 + 21 * 60 + 30


# ---------------------------------------------------------------------------
# Rejecting bad payloads
# ---------------------------------------------------------------------------


def test_empty_payload_raises(fetched_at):
    with pytest.raises(FeedParseError, match="Empty payload"):
        parse_feed(b"", "TEST", "http://example", fetched_at)


def test_html_error_page_raises(fetched_at):
    """
    The failure mode that matters most: an endpoint returning an HTML error
    page with a 200 status. Without this check the pipeline would record a
    successful fetch containing zero trains, and the dashboard would show an
    empty system rather than an error.
    """
    payload = b"<html><body>503 Service Unavailable</body></html>"
    with pytest.raises(FeedParseError):
        parse_feed(payload, "TEST", "http://example", fetched_at)


def test_valid_protobuf_that_is_not_gtfs_raises(fetched_at, feed_builder):
    """
    Protobuf is permissive: unrelated bytes can decode into an empty message
    without error. Checking for the GTFS-Realtime version header is what
    distinguishes "decoded successfully" from "is actually our feed".
    """
    payload = feed_builder(gtfs_version="", vehicles=[{}])
    with pytest.raises(FeedParseError, match="no GTFS-Realtime"):
        parse_feed(payload, "TEST", "http://example", fetched_at)


# ---------------------------------------------------------------------------
# Decoding a real captured payload
# ---------------------------------------------------------------------------


def test_parses_real_captured_feed(sample_feed_bytes, fetched_at):
    """
    Runs against real bytes captured from the live MTA endpoint, so the test
    fails if the parser drifts away from the format as actually served rather
    than as documented.
    """
    snapshot = parse_feed(sample_feed_bytes, "1234567S", "http://mta", fetched_at)

    assert snapshot.feed_key == "1234567S"
    assert snapshot.fetched_at == fetched_at
    assert snapshot.header_timestamp is not None
    assert snapshot.entity_count == 7

    assert len(snapshot.vehicle_positions) == 3
    assert len(snapshot.trip_stop_updates) > 0
    assert len(snapshot.alerts) == 1

    vehicle = snapshot.vehicle_positions[0]
    assert vehicle["route_id"] == "1"
    assert vehicle["direction"] in ("N", "S")
    assert vehicle["current_status"] in (
        "STOPPED_AT",
        "IN_TRANSIT_TO",
        "INCOMING_AT",
    )
    assert vehicle["trip_id"].startswith("1")


def test_real_feed_alert_with_unparseable_trip_id_is_still_captured(
    sample_feed_bytes, fetched_at
):
    """
    The captured alert names trip 065600_7..MAIN ST34, whose id has no
    direction letter. The alert must still be recorded: dropping it would lose
    the only signal MTA gives that a train is delayed.
    """
    snapshot = parse_feed(sample_feed_bytes, "1234567S", "http://mta", fetched_at)
    alert = snapshot.alerts[0]

    assert alert["trip_id"] == "065600_7..MAIN ST34"
    assert alert["direction"] is None
    assert alert["origin_departure_seconds"] == 39360
    assert "delay" in (alert["alert_header"] or "").lower()


def test_vehicle_and_trip_update_have_different_entity_ids(
    sample_feed_bytes, fetched_at
):
    """
    REGRESSION: a train appears in the feed as TWO entities with different
    ids. Joining positions to predictions on entity_id therefore matches
    nothing, which is a bug that produced a table of all nulls while every
    structural test still passed.

    This test pins the fact down at the parsing layer so the reason the dbt
    models join on trip_id plus stop_id is recorded in code and not only in a
    commit message.
    """
    snapshot = parse_feed(sample_feed_bytes, "1234567S", "http://mta", fetched_at)

    vehicle_ids = {row["entity_id"] for row in snapshot.vehicle_positions}
    trip_update_ids = {row["entity_id"] for row in snapshot.trip_stop_updates}

    assert vehicle_ids
    assert trip_update_ids
    assert not (vehicle_ids & trip_update_ids), (
        "vehicle and trip_update entity ids overlapped, which would make the "
        "entity_id join look like it works"
    )

    # The same trip is present on both sides, which is why trip_id is the
    # correct join key.
    vehicle_trips = {row["trip_id"] for row in snapshot.vehicle_positions}
    update_trips = {row["trip_id"] for row in snapshot.trip_stop_updates}
    assert vehicle_trips & update_trips


# ---------------------------------------------------------------------------
# Synthetic edge cases
# ---------------------------------------------------------------------------


def test_stop_time_update_without_arrival_yields_null_not_epoch_zero(
    feed_builder, fetched_at
):
    payload = feed_builder(
        trip_updates=[
            {
                "trip_id": "098150_1..N15R",
                "stops": [
                    {"stop_id": "101N", "arrival": None, "departure": 1789075200},
                    {"stop_id": "103N", "arrival": 1789075500},
                ],
            }
        ]
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert len(snapshot.trip_stop_updates) == 2
    assert snapshot.trip_stop_updates[0]["arrival_time"] is None
    assert snapshot.trip_stop_updates[0]["departure_time"] is not None
    assert snapshot.trip_stop_updates[1]["arrival_time"] is not None


def test_stop_index_records_order_because_nyct_omits_stop_sequence(
    feed_builder, fetched_at
):
    """
    NYCT does not populate stop_sequence, so position in the list is the only
    ordering available. Index 0 must be the train's next stop.
    """
    payload = feed_builder(
        trip_updates=[
            {
                "stops": [
                    {"stop_id": "A", "arrival": 100},
                    {"stop_id": "B", "arrival": 200},
                    {"stop_id": "C", "arrival": 300},
                ]
            }
        ]
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert [row["stop_index"] for row in snapshot.trip_stop_updates] == [0, 1, 2]
    assert [row["stop_id"] for row in snapshot.trip_stop_updates] == ["A", "B", "C"]


def test_alert_with_multiple_informed_entities_becomes_multiple_rows(
    feed_builder, fetched_at
):
    payload = feed_builder(
        alerts=[
            {
                "header_text": "Train delayed",
                "informed": [
                    {"trip_id": "098150_1..N15R"},
                    {"trip_id": "099150_1..N15R"},
                ],
            }
        ]
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert len(snapshot.alerts) == 2
    assert {row["trip_id"] for row in snapshot.alerts} == {
        "098150_1..N15R",
        "099150_1..N15R",
    }
    assert all(row["alert_header"] == "Train delayed" for row in snapshot.alerts)


def test_alert_with_no_informed_entity_still_produces_a_row(
    feed_builder, fetched_at
):
    """A system wide alert names no trip, and must not vanish."""
    payload = feed_builder(alerts=[{"header_text": "Service change", "informed": []}])
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert len(snapshot.alerts) == 1
    assert snapshot.alerts[0]["trip_id"] is None
    assert snapshot.alerts[0]["alert_header"] == "Service change"


def test_alert_naming_only_a_route_keeps_the_route(feed_builder, fetched_at):
    payload = feed_builder(
        alerts=[{"header_text": "Delays on the 6", "informed": [{"route_id": "6"}]}]
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert snapshot.alerts[0]["route_id"] == "6"
    assert snapshot.alerts[0]["trip_id"] is None


def test_vehicle_status_codes_are_decoded_to_labels(feed_builder, fetched_at):
    payload = feed_builder(
        vehicles=[
            {"current_status": 0, "trip_id": "098150_1..N"},
            {"current_status": 1, "trip_id": "098250_1..N"},
            {"current_status": 2, "trip_id": "098350_1..N"},
        ]
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert [row["current_status"] for row in snapshot.vehicle_positions] == [
        "INCOMING_AT",
        "STOPPED_AT",
        "IN_TRANSIT_TO",
    ]


def test_row_count_matches_the_sum_of_the_three_record_types(
    feed_builder, fetched_at
):
    payload = feed_builder(
        vehicles=[{"trip_id": "098150_1..N"}],
        trip_updates=[{"stops": [{"stop_id": "A", "arrival": 1}, {"stop_id": "B", "arrival": 2}]}],
        alerts=[{"header_text": "Train delayed", "informed": [{"trip_id": "098150_1..N"}]}],
    )
    snapshot = parse_feed(payload, "TEST", "http://x", fetched_at)

    assert snapshot.row_count == 1 + 2 + 1
    assert snapshot.entity_count == 3


def test_empty_but_valid_feed_parses_to_zero_rows(feed_builder, fetched_at):
    """
    A feed with a valid header and no entities is not an error. It happens at
    4am on some lines, and treating it as a failure would fill the error log
    with noise every night.
    """
    snapshot = parse_feed(feed_builder(), "TEST", "http://x", fetched_at)

    assert snapshot.entity_count == 0
    assert snapshot.row_count == 0
    assert snapshot.header_timestamp is not None
