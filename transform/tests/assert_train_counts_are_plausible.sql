-- Singular test: the number of trains per line has to be physically possible.
--
-- Generic tests check structure. This one checks that the numbers mean
-- something. NYC runs on the order of twenty to forty trains per line at once,
-- and never several hundred.
--
-- The failure this actually catches is a fan out join. If a model accidentally
-- joins one train to its twenty upcoming stops, every count multiplies by
-- twenty and the result still looks like a perfectly valid table. Nothing
-- structural is wrong, so unique and not_null both pass. Only a test that
-- knows what a plausible answer looks like will catch it.
--
-- Any row this query returns is a failure.

select
    route_id,
    trains_running,
    'Implausible train count for one line' as failure_reason
from {{ ref('fct_line_activity') }}
where trains_running > 150
   or trains_running < 1
