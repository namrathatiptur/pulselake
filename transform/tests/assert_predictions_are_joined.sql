-- Singular test: most trains must actually have an arrival prediction.
--
-- This test exists because of a bug it would have caught immediately.
--
-- fct_current_trains originally joined train positions to arrival predictions
-- on entity_id. That looked right, and every structural test passed: the
-- grain was still unique, no key was null, every accepted_values check held.
-- But a train appears in the feed as two separate entities with different
-- entity_ids, so the join matched zero rows, and the model quietly produced a
-- table where every prediction column was null. The dashboard would have
-- shown an empty column and a chart with no data, and nothing would have
-- said why.
--
-- The general lesson, and the reason this file is worth reading in an
-- interview: unique and not_null verify that a table is well formed. They
-- cannot tell you it is correct. A left join that matches nothing is
-- perfectly well formed. Somebody has to assert that the data is actually
-- there.
--
-- Set to warn rather than error because a genuinely small number of trains
-- do lack predictions at terminals, and the threshold is a judgement call
-- rather than a law.
--
-- Any row this query returns is a failure.

{{ config(severity = 'warn') }}

with coverage as (

    select
        count(*)                                        as total_trains,
        count(next_stop_id)                             as trains_with_prediction,
        round(100.0 * count(next_stop_id) / nullif(count(*), 0), 1)
                                                        as pct_with_prediction
    from {{ ref('fct_current_trains') }}

)

select
    total_trains,
    trains_with_prediction,
    pct_with_prediction,
    'Arrival predictions are not joining to train positions' as failure_reason
from coverage
where pct_with_prediction < 80
