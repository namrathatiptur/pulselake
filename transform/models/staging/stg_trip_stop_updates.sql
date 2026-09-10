-- Predicted arrivals, one row per upcoming stop per trip per snapshot.
--
-- This is by far the largest table in the project, around 7000 rows per
-- snapshot. It is left as a view rather than a table so the disk cost stays
-- with the raw data and not with a second copy of it.
--
-- stop_index 0 is the train's next stop. That single row per trip is what the
-- current state dashboard needs; the rest of the list is what makes the
-- prediction drift model possible.

with source as (

    select * from {{ source('pulselake_raw', 'raw_trip_stop_updates') }}

),

typed as (

    select
        fetch_id || '|' || entity_id || '|' || stop_index   as snapshot_stop_key,
        coalesce(trip_id, 'unknown')
            || '|' || coalesce(start_date, 'unknown')       as trip_key,

        fetch_id,
        feed_key,
        entity_id,
        trip_id,
        start_date,
        route_id,
        direction,

        stop_index,
        stop_id,
        schedule_relationship,

        header_timestamp,
        fetched_at,
        arrival_time,
        departure_time,

        stop_index = 0                                      as is_next_stop,

        -- How far in the future the predicted arrival was, measured from the
        -- moment MTA published the snapshot. Negative values are normal and
        -- meaningful: MTA keeps a stop in the list briefly after the train has
        -- already arrived, so a small negative number means "just arrived".
        date_diff('second', header_timestamp, arrival_time) as seconds_to_arrival

    from source
    -- A stop update with no predicted arrival carries no information for any
    -- downstream model. This is the only row filtering in the staging layer.
    where arrival_time is not null

)

select * from typed
