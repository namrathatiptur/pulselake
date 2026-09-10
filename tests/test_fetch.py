"""
Unit tests for the HTTP layer, with no network access.

Every test here drives fetch_feed_bytes with a fake session object. That is
possible only because fetch_feed_bytes accepts a session rather than creating
one internally, which is the whole reason it takes that argument.
"""

from __future__ import annotations

import pytest
import requests

from ingest.fetch import (
    FeedFetchError,
    PermanentFeedError,
    fetch_and_parse,
    fetch_feed_bytes,
)


class FakeResponse:
    def __init__(self, status_code: int = 200, content: bytes = b"data"):
        self.status_code = status_code
        self.content = content


class FakeSession:
    """
    Stands in for requests.Session. Replays a scripted list of outcomes, one
    per call, and records how many times it was asked.
    """

    def __init__(self, outcomes: list):
        self.outcomes = list(outcomes)
        self.calls = 0

    def get(self, url, timeout=None):
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else self.outcomes
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@pytest.fixture(autouse=True)
def no_real_sleeping(monkeypatch):
    """
    Neutralise the retry backoff so the suite stays fast. Without this, the
    retry tests would spend six real seconds sleeping.
    """
    monkeypatch.setattr("ingest.fetch.time.sleep", lambda seconds: None)


def test_successful_fetch_returns_bytes_and_a_timestamp():
    session = FakeSession([FakeResponse(200, b"payload")])
    payload, requested_at = fetch_feed_bytes("TEST", "http://x", session=session)

    assert payload == b"payload"
    assert requested_at.tzinfo is not None
    assert session.calls == 1


def test_retries_a_transient_server_error_then_succeeds():
    session = FakeSession(
        [FakeResponse(503), FakeResponse(503), FakeResponse(200, b"ok")]
    )
    payload, _ = fetch_feed_bytes("TEST", "http://x", session=session)

    assert payload == b"ok"
    assert session.calls == 3


def test_gives_up_after_max_retries():
    session = FakeSession([FakeResponse(503)] * 3)
    with pytest.raises(FeedFetchError):
        fetch_feed_bytes("TEST", "http://x", session=session, max_retries=3)

    assert session.calls == 3


def test_network_error_is_retried():
    session = FakeSession(
        [requests.ConnectionError("no route to host"), FakeResponse(200, b"ok")]
    )
    payload, _ = fetch_feed_bytes("TEST", "http://x", session=session)

    assert payload == b"ok"
    assert session.calls == 2


def test_timeout_is_retried():
    session = FakeSession([requests.Timeout("timed out"), FakeResponse(200, b"ok")])
    payload, _ = fetch_feed_bytes("TEST", "http://x", session=session)

    assert payload == b"ok"


@pytest.mark.parametrize("status", [400, 401, 403, 404, 410])
def test_permanent_errors_fail_immediately_without_retrying(status):
    """
    REGRESSION: requests.HTTPError subclasses requests.RequestException, so
    the first version of this code raised HTTPError for a non-retryable
    status inside the try block and its own retry handler caught it. A 403
    was retried three times with backoff before failing.

    Asserting on session.calls is the point of this test. Asserting only that
    it raises would pass against the buggy version too.
    """
    session = FakeSession([FakeResponse(status)] * 5)

    with pytest.raises(PermanentFeedError):
        fetch_feed_bytes("TEST", "http://x", session=session, max_retries=5)

    assert session.calls == 1, f"HTTP {status} should not be retried"


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
def test_retryable_statuses_are_actually_retried(status):
    """The other half of the pair above: these ones should be retried."""
    session = FakeSession([FakeResponse(status)] * 3)

    with pytest.raises(FeedFetchError):
        fetch_feed_bytes("TEST", "http://x", session=session, max_retries=3)

    assert session.calls == 3


def test_permanent_error_is_still_catchable_as_feed_fetch_error():
    """
    PermanentFeedError subclasses FeedFetchError on purpose, so callers that
    only care that the fetch failed do not need to know about the split.
    """
    session = FakeSession([FakeResponse(404)])

    with pytest.raises(FeedFetchError):
        fetch_feed_bytes("TEST", "http://x", session=session)


def test_empty_body_with_a_200_is_treated_as_a_failure():
    session = FakeSession([FakeResponse(200, b"")] * 3)

    with pytest.raises(FeedFetchError):
        fetch_feed_bytes("TEST", "http://x", session=session, max_retries=3)

    assert session.calls == 3


def test_fetch_and_parse_wires_the_two_layers_together(feed_builder):
    payload = feed_builder(vehicles=[{"trip_id": "098150_1..N15R", "route_id": "1"}])
    session = FakeSession([FakeResponse(200, payload)])

    snapshot = fetch_and_parse("1234567S", "http://x", session=session)

    assert snapshot.feed_key == "1234567S"
    assert snapshot.feed_url == "http://x"
    assert len(snapshot.vehicle_positions) == 1
    assert snapshot.vehicle_positions[0]["route_id"] == "1"
