"""
Network layer: pull raw protobuf bytes from the MTA GTFS-Realtime endpoints.

Separated from parse.py so that the parsing logic stays testable offline, and
so all the retry and timeout behaviour lives in exactly one place.

The MTA subway feeds need no API key and no account. They have been fully open
since 2023, so there is no credential handling anywhere in this project.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

import requests

from ingest.config import (
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECONDS,
    RETRY_BACKOFF_SECONDS,
)
from ingest.parse import ParsedSnapshot, parse_feed

logger = logging.getLogger(__name__)

# Status codes worth trying again. Everything else, for example a 404 on a
# feed key that no longer exists, is a real failure and retrying just wastes
# time and hides the problem.
RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

USER_AGENT = "PulseLake/0.1 (portfolio data pipeline; local MVP)"


class FeedFetchError(Exception):
    """Raised when a feed could not be retrieved after all retries."""


class PermanentFeedError(FeedFetchError):
    """
    Raised when retrying would be pointless, for example a 404 on a feed key
    that no longer exists or a 403 on a URL we are not allowed to read.

    Kept as a subclass of FeedFetchError so callers that only care about
    "the fetch failed" can still catch the parent.
    """


def build_session() -> requests.Session:
    """
    Create a reusable HTTP session.

    Reusing one session across a polling loop keeps the TCP and TLS connection
    alive between cycles, which matters when you are hitting the same host
    every 45 seconds for hours.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_feed_bytes(
    feed_key: str,
    feed_url: str,
    session: requests.Session | None = None,
    max_retries: int = MAX_RETRIES,
) -> tuple[bytes, datetime]:
    """
    Download one feed and return its raw bytes plus the time of the request.

    Retries transient failures with a linear backoff. Returns the fetch time
    from before the request rather than after, so the timestamp reflects when
    we asked for the data.

    Raises:
        FeedFetchError if every attempt fails.
    """
    session = session or build_session()
    last_error: Exception | None = None

    for attempt in range(1, max_retries + 1):
        requested_at = datetime.now(timezone.utc)
        try:
            response = session.get(feed_url, timeout=REQUEST_TIMEOUT_SECONDS)

            # Order matters here. A non-retryable error status has to escape
            # the retry loop immediately, and because requests.HTTPError is a
            # subclass of requests.RequestException, raising it inside the try
            # block would get it swallowed by the retry handler below.
            if (
                response.status_code >= 400
                and response.status_code not in RETRYABLE_STATUS_CODES
            ):
                raise PermanentFeedError(
                    f"HTTP {response.status_code} from {feed_url}. "
                    f"This will not succeed on retry."
                )

            if response.status_code in RETRYABLE_STATUS_CODES:
                raise requests.HTTPError(
                    f"HTTP {response.status_code} from {feed_url}",
                    response=response,
                )

            if not response.content:
                raise requests.HTTPError(f"Empty body from {feed_url}")

            return response.content, requested_at

        except requests.RequestException as exc:
            last_error = exc
            if attempt < max_retries:
                delay = RETRY_BACKOFF_SECONDS * attempt
                logger.warning(
                    "Feed %s attempt %d/%d failed (%s). Retrying in %.1fs.",
                    feed_key,
                    attempt,
                    max_retries,
                    exc,
                    delay,
                )
                time.sleep(delay)
            else:
                logger.error(
                    "Feed %s failed after %d attempts: %s",
                    feed_key,
                    max_retries,
                    exc,
                )

    raise FeedFetchError(
        f"Could not fetch feed '{feed_key}' from {feed_url} after "
        f"{max_retries} attempts: {last_error}"
    ) from last_error


def fetch_and_parse(
    feed_key: str,
    feed_url: str,
    session: requests.Session | None = None,
) -> ParsedSnapshot:
    """
    Fetch one feed and decode it in a single call.

    This is the seam the rest of the pipeline uses. Anything that raises here
    is either a FeedFetchError (network side) or a FeedParseError (payload
    side), and the caller can log the two differently because they mean very
    different things operationally.
    """
    payload, requested_at = fetch_feed_bytes(feed_key, feed_url, session=session)
    return parse_feed(
        payload=payload,
        feed_key=feed_key,
        feed_url=feed_url,
        fetched_at=requested_at,
    )
