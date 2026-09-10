-- Singular test: ingestion should not have long holes in it.
--
-- Configured as a warning rather than an error, because a gap is a completely
-- normal consequence of stopping the poller to go and do something else. On a
-- real schedule you would make this an error and page on it.
--
-- The reason it matters: a dashboard reading stale data looks identical to a
-- dashboard reading a quiet system. Only something checking the clock can
-- tell the difference, and that something has to live in the pipeline rather
-- than in the reader's head.
--
-- Any row this query returns is a failure.

{{ config(severity = 'warn') }}

select
    feed_key,
    snapshot_at,
    seconds_since_previous_snapshot,
    round(seconds_since_previous_snapshot / 60.0, 1) as gap_minutes
from {{ ref('dq_ingestion_health') }}
where is_after_ingestion_gap
