"""
Decode MTA GTFS-Realtime protobuf payloads into plain Python dictionaries.

This module deliberately does no network and no database work. Everything here
is a pure function: bytes go in, dictionaries come out. That is what makes the
ingestion logic unit testable without a live connection, which is what
tests/test_parse.py relies on.

A note on what the MTA feed actually contains, because it drives the schema:

  * A FeedMessage has a header with a timestamp. That timestamp only advances
    when MTA publishes a new snapshot, roughly every 30 seconds. We use it as
    the natural deduplication key so polling faster than the publisher does not
    create duplicate rows.
  * Each entity carries exactly one of trip_update, vehicle, or alert.
  * A trip_update holds a list of stop_time_update records, one per upcoming
    stop, each with a predicted arrival and departure epoch.
  * A vehicle record is the train's current position: which stop it is at or
    heading to, and whether it is stopped or in transit.
  * NYCT does NOT populate the standard GTFS delay field. It does publish
    alerts whose header text says things like "Train delayed", and the trip_id
    itself encodes the scheduled origin departure time. Both are parsed below.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2


class FeedParseError(Exception):
    """Raised when a payload is not a decodable GTFS-Realtime FeedMessage."""


# GTFS VehiclePosition.VehicleStopStatus is an integer enum on the wire.
# Translating it here keeps the meaning in the data rather than in a lookup
# somebody has to remember.
VEHICLE_STATUS_LABELS = {
    0: "INCOMING_AT",
    1: "STOPPED_AT",
    2: "IN_TRANSIT_TO",
}

# GTFS StopTimeUpdate.ScheduleRelationship.
SCHEDULE_RELATIONSHIP_LABELS = {
    0: "SCHEDULED",
    1: "SKIPPED",
    2: "NO_DATA",
    3: "UNSCHEDULED",
}

# NYCT trip_id format. The leading six digits are the scheduled origin
# departure expressed in hundredths of a minute after midnight, so 098150 means
# 981.50 minutes, which is 16:21:30. After the underscore comes the route, one
# or two dots, then a path code that usually begins with the direction letter.
#
# Three real variants show up in the live feed, which is why the direction is
# matched separately rather than being baked into this pattern:
#
#   098150_1..N15R        the common case, two dots, direction N
#   104150_GS.N04R        the 42nd Street shuttle, only one dot
#   065600_7..MAIN ST34   a terminal name in place of a direction letter
#
# Roughly six percent of trip ids in a given snapshot are one of the last two
# forms, so treating only the first as valid would quietly drop them.
NYCT_TRIP_ID_PATTERN = re.compile(
    r"^(?P<origin>\d{6})_(?P<route>[^.]+)\.{1,2}(?P<path>.*)$"
)


@dataclass
class ParsedSnapshot:
    """
    One decoded fetch of one feed.

    The three record lists map one to one onto the three raw tables in DuckDB.
    Keeping them in a single object means the storage layer can write all of
    them inside one transaction, so a snapshot is never half persisted.
    """

    feed_key: str
    feed_url: str
    fetched_at: datetime
    header_timestamp: datetime | None
    payload_bytes: int
    entity_count: int
    vehicle_positions: list[dict] = field(default_factory=list)
    trip_stop_updates: list[dict] = field(default_factory=list)
    alerts: list[dict] = field(default_factory=list)

    @property
    def row_count(self) -> int:
        return (
            len(self.vehicle_positions)
            + len(self.trip_stop_updates)
            + len(self.alerts)
        )


def epoch_to_utc(value: int | None) -> datetime | None:
    """
    Convert a POSIX epoch second to a timezone aware UTC datetime.

    Protobuf scalar fields default to 0 when absent rather than being null, so
    a 0 here means "not provided" and not "1 January 1970". Returning None for
    that case keeps the nulls honest once the data reaches DuckDB.
    """
    if not value:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc)


def parse_nyct_trip_id(trip_id: str) -> dict:
    """
    Pull the extra information NYCT hides inside the trip_id string.

    Returns a dictionary with:
        direction                  "N", "S", or None if the id does not match
        origin_departure_seconds   scheduled departure from the first stop,
                                   as seconds after midnight, or None
        trip_path                  the remaining path code, useful for grouping

    Any trip_id that does not match the NYCT convention yields all None values
    instead of raising. Feeds change, and a single odd trip id should never
    take down an ingestion run.
    """
    match = NYCT_TRIP_ID_PATTERN.match(trip_id or "")
    if not match:
        return {
            "direction": None,
            "origin_departure_seconds": None,
            "trip_path": None,
        }

    # Six digits are hundredths of a minute, so divide by 100 to get minutes
    # and multiply by 60 to get seconds.
    hundredths_of_minute = int(match.group("origin"))
    origin_seconds = round(hundredths_of_minute / 100.0 * 60.0)

    path = match.group("path")
    # The path normally starts with the direction letter, but not always, for
    # example "MAIN ST34". Only treat a leading N or S as a direction when the
    # rest of the path does not look like a station name.
    direction = None
    if path[:1] in ("N", "S") and not path[1:2].isalpha():
        direction = path[:1]

    return {
        "direction": direction,
        "origin_departure_seconds": origin_seconds,
        "trip_path": path or None,
    }


def _trip_fields(trip) -> dict:
    """Flatten a TripDescriptor plus the NYCT extras decoded from its id."""
    trip_id = trip.trip_id or None
    fields = {
        "trip_id": trip_id,
        "route_id": trip.route_id or None,
        "start_date": trip.start_date or None,
    }
    fields.update(parse_nyct_trip_id(trip_id))
    return fields


def parse_feed(
    payload: bytes,
    feed_key: str,
    feed_url: str,
    fetched_at: datetime,
) -> ParsedSnapshot:
    """
    Decode raw protobuf bytes into a ParsedSnapshot.

    Args:
        payload:    the raw response body from the MTA endpoint
        feed_key:   short label for which feed this is, for example "1234567S"
        feed_url:   the endpoint it came from, stored for provenance
        fetched_at: when we made the request, timezone aware UTC

    Raises:
        FeedParseError if the bytes are not a valid FeedMessage. MTA
        occasionally returns an HTML error page with a 200 status, and that is
        exactly the "unexpected response format" case worth guarding.
    """
    if not payload:
        raise FeedParseError(f"Empty payload from feed '{feed_key}'")

    message = gtfs_realtime_pb2.FeedMessage()
    try:
        message.ParseFromString(payload)
    except (DecodeError, UnicodeDecodeError) as exc:
        preview = payload[:80]
        raise FeedParseError(
            f"Could not decode protobuf from feed '{feed_key}': {exc}. "
            f"First bytes were {preview!r}"
        ) from exc

    # A valid but unrelated payload can technically parse into an empty
    # FeedMessage, so check that the header actually looks like GTFS-RT.
    if not message.header.gtfs_realtime_version:
        raise FeedParseError(
            f"Payload from feed '{feed_key}' decoded but has no GTFS-Realtime "
            f"header. It is probably not a GTFS-Realtime feed."
        )

    snapshot = ParsedSnapshot(
        feed_key=feed_key,
        feed_url=feed_url,
        fetched_at=fetched_at,
        header_timestamp=epoch_to_utc(message.header.timestamp),
        payload_bytes=len(payload),
        entity_count=len(message.entity),
    )

    for entity in message.entity:
        entity_id = entity.id or None

        if entity.HasField("vehicle"):
            vehicle = entity.vehicle
            row = {"entity_id": entity_id}
            row.update(_trip_fields(vehicle.trip))
            row.update(
                {
                    "current_stop_sequence": vehicle.current_stop_sequence or None,
                    "current_status": VEHICLE_STATUS_LABELS.get(
                        vehicle.current_status, str(vehicle.current_status)
                    ),
                    "stop_id": vehicle.stop_id or None,
                    "vehicle_timestamp": epoch_to_utc(vehicle.timestamp),
                }
            )
            snapshot.vehicle_positions.append(row)

        if entity.HasField("trip_update"):
            trip_update = entity.trip_update
            trip = _trip_fields(trip_update.trip)
            # stop_sequence is not populated by NYCT, so we record the position
            # of the update within the list. Index 0 is the train's next stop,
            # which is the row the dashboard cares about most.
            for index, stop_update in enumerate(trip_update.stop_time_update):
                row = {"entity_id": entity_id}
                row.update(trip)
                row.update(
                    {
                        "stop_index": index,
                        "stop_id": stop_update.stop_id or None,
                        "arrival_time": epoch_to_utc(
                            stop_update.arrival.time
                            if stop_update.HasField("arrival")
                            else None
                        ),
                        "departure_time": epoch_to_utc(
                            stop_update.departure.time
                            if stop_update.HasField("departure")
                            else None
                        ),
                        "schedule_relationship": SCHEDULE_RELATIONSHIP_LABELS.get(
                            stop_update.schedule_relationship,
                            str(stop_update.schedule_relationship),
                        ),
                    }
                )
                snapshot.trip_stop_updates.append(row)

        if entity.HasField("alert"):
            alert = entity.alert
            header_text = None
            if alert.header_text.translation:
                header_text = alert.header_text.translation[0].text or None

            # One alert can name several affected trips or routes, so we write
            # one row per informed entity. That keeps joins simple downstream.
            if not alert.informed_entity:
                snapshot.alerts.append(
                    {
                        "entity_id": entity_id,
                        "alert_header": header_text,
                        "trip_id": None,
                        "route_id": None,
                        "direction": None,
                        "origin_departure_seconds": None,
                        "trip_path": None,
                        "start_date": None,
                    }
                )
            for informed in alert.informed_entity:
                row = {"entity_id": entity_id, "alert_header": header_text}
                row.update(_trip_fields(informed.trip))
                # An alert can name a route without naming a trip.
                if not row["route_id"]:
                    row["route_id"] = informed.route_id or None
                snapshot.alerts.append(row)

    return snapshot
