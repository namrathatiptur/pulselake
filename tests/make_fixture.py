"""
Regenerate the captured feed fixture used by the test suite.

    python -m tests.make_fixture

This is the ONLY thing in tests/ that touches the network, and it is not run
by pytest. The fixture it writes is committed to the repo so the suite stays
offline and deterministic.

Why keep a real captured payload at all, rather than only synthetic ones:
every interesting bug in this project came from the difference between the
feed as documented and the feed as actually served. Synthetic fixtures encode
what I believed the format was, which is exactly the belief that was wrong.
Real bytes do not have that problem.

Keep the fixture small. Three trip updates, three vehicles, one alert, and the
stop lists trimmed to four entries is enough to exercise every code path.
"""

from __future__ import annotations

import sys

import requests
from google.transit import gtfs_realtime_pb2

from ingest.config import FEEDS
from tests.conftest import SAMPLE_FEED_PATH

LIMITS = {"trip_update": 3, "vehicle": 3, "alert": 1}
MAX_STOPS_PER_TRIP = 4


def main() -> int:
    url = FEEDS["1234567S"]
    print(f"Fetching {url}")
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    source = gtfs_realtime_pb2.FeedMessage()
    source.ParseFromString(response.content)

    trimmed = gtfs_realtime_pb2.FeedMessage()
    trimmed.header.CopyFrom(source.header)

    kept = {kind: 0 for kind in LIMITS}
    for entity in source.entity:
        for kind in LIMITS:
            if entity.HasField(kind) and kept[kind] < LIMITS[kind]:
                trimmed.entity.add().CopyFrom(entity)
                kept[kind] += 1
                break
        if all(kept[kind] >= LIMITS[kind] for kind in LIMITS):
            break

    for entity in trimmed.entity:
        if entity.HasField("trip_update"):
            del entity.trip_update.stop_time_update[MAX_STOPS_PER_TRIP:]

    payload = trimmed.SerializeToString()
    SAMPLE_FEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    SAMPLE_FEED_PATH.write_bytes(payload)

    print(f"Wrote {SAMPLE_FEED_PATH} ({len(payload)} bytes): {kept}")

    if kept["alert"] == 0:
        print(
            "WARNING: no alert was present in this snapshot. Some tests expect "
            "one. Try again in a few minutes.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
