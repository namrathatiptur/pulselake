-- Service level summary, one row per subway line, from the newest snapshot.
--
-- This is the model that answers "what is the system doing right now" in about
-- ten rows, which is what makes it the right thing to put at the top of a
-- dashboard. Grain is one row per (feed_key, route_id).

with current_trains as (

    select * from {{ ref('fct_current_trains') }}

),

by_route as (

    select
        feed_key,
        route_id,
        max(snapshot_at)                                        as snapshot_at,

        count(*)                                                as trains_running,
        count(*) filter (where direction = 'N')                 as trains_northbound,
        count(*) filter (where direction = 'S')                 as trains_southbound,

        count(*) filter (where current_status = 'STOPPED_AT')   as trains_at_station,
        count(*) filter (where current_status = 'IN_TRANSIT_TO') as trains_in_transit,
        count(*) filter (where current_status = 'INCOMING_AT')  as trains_arriving,

        count(*) filter (where has_delay_alert)                 as trains_delayed,

        -- Median rather than mean. A single train showing a 40 minute
        -- prediction because it is holding at a terminal would drag an
        -- average badly, and there are only twenty or so trains per line.
        round(median(minutes_to_next_stop), 1)                  as median_minutes_to_next_stop,
        round(max(minutes_to_next_stop), 1)                     as max_minutes_to_next_stop,

        -- Feed quality, per line, so a problem can be localized rather than
        -- just noticed.
        count(*) filter (where has_reused_trip_id)              as trains_with_reused_trip_id,
        count(*) filter (where next_stop_id is null)            as trains_missing_prediction

    from current_trains
    where route_id is not null
    group by 1, 2

),

with_rates as (

    select
        *,
        round(100.0 * trains_delayed / nullif(trains_running, 0), 1)
            as pct_delayed,
        round(100.0 * trains_missing_prediction / nullif(trains_running, 0), 1)
            as pct_missing_prediction
    from by_route

)

select * from with_rates
