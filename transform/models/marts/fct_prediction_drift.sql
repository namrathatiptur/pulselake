-- How much MTA's own arrival prediction for a stop moved over time.
--
-- Why this model exists at all:
--
-- The NYCT feed does not populate the standard GTFS `delay` field. It is
-- absent from every record, so there is no published number saying a train is
-- N seconds late, and without the static GTFS schedule there is no per stop
-- timetable to subtract from either. Rather than invent a delay figure, this
-- model measures something the pipeline genuinely can observe.
--
-- For a given train and a given upcoming stop, MTA publishes a predicted
-- arrival time in every snapshot. If the train is running well, that
-- prediction holds steady. If the train is losing time, the prediction for
-- that same stop keeps sliding later. The difference between the earliest and
-- latest prediction for the same stop is drift:
--
--     drift > 0    the train is slipping later, losing time
--     drift ~ 0    the prediction is holding, running to plan
--     drift < 0    the train is making up time
--
-- This is the clearest argument in the project for why the pipeline stores
-- history instead of just showing the live feed. A single snapshot cannot
-- produce this number. It only exists because every fetch was kept.
--
-- Grain: one row per (trip_key, stop_id).
--
-- Note the join key. This model spans snapshots, so it uses trip_key and NOT
-- entity_id, which is only a position within one protobuf message and points
-- at a different train from one snapshot to the next.

with observations as (

    select
        trip_key,
        trip_id,
        route_id,
        direction,
        stop_id,
        header_timestamp,
        arrival_time,
        seconds_to_arrival
    from {{ ref('stg_trip_stop_updates') }}

),

per_stop as (

    select
        trip_key,
        any_value(trip_id)                                      as trip_id,
        any_value(route_id)                                     as route_id,
        any_value(direction)                                    as direction,
        stop_id,

        count(*)                                                as observation_count,
        min(header_timestamp)                                   as first_observed_at,
        max(header_timestamp)                                   as last_observed_at,

        -- arg_min and arg_max pick the value of one column at the row where
        -- another column is smallest or largest, which is exactly "the
        -- prediction we saw first" and "the prediction we saw last" without
        -- needing a window function and a filter.
        arg_min(arrival_time, header_timestamp)                 as first_predicted_arrival,
        arg_max(arrival_time, header_timestamp)                 as last_predicted_arrival,
        arg_min(seconds_to_arrival, header_timestamp)           as first_seconds_to_arrival

    from observations
    group by trip_key, stop_id

),

filtered as (

    select *
    from per_stop
    -- Drift needs at least two observations of the same stop by definition.
    where observation_count >= 2
      -- Only count stops that were genuinely still ahead of the train when we
      -- first saw them. MTA keeps a stop in the list briefly after arrival,
      -- and those trailing rows produce drift that reflects the feed's
      -- bookkeeping rather than the train's progress.
      and first_seconds_to_arrival > 60

),

scored as (

    select
        trip_key,
        trip_id,
        route_id,
        direction,
        stop_id,

        observation_count,
        first_observed_at,
        last_observed_at,
        first_predicted_arrival,
        last_predicted_arrival,

        date_diff('second', first_predicted_arrival, last_predicted_arrival)
            as drift_seconds,

        round(
            date_diff('second', first_predicted_arrival, last_predicted_arrival) / 60.0,
            1
        ) as drift_minutes,

        -- How long we watched this stop for. A large drift observed over
        -- thirty seconds is a very different claim from the same drift
        -- observed over ten minutes, so the reader needs both numbers.
        date_diff('second', first_observed_at, last_observed_at)
            as observation_window_seconds

    from filtered

),

with_trip_rollup as (

    select
        *,
        -- Median drift across all of this train's upcoming stops, repeated on
        -- every row. One stop slipping can be a held signal; the median
        -- slipping means the whole trip is running late.
        round(
            median(drift_seconds) over (partition by trip_key) / 60.0,
            1
        ) as trip_median_drift_minutes,

        count(*) over (partition by trip_key) as trip_stops_observed,

        case
            when drift_seconds >  120 then 'Losing time'
            when drift_seconds < -120 then 'Making up time'
            else 'On plan'
        end as drift_label

    from scored

)

select * from with_trip_rollup
