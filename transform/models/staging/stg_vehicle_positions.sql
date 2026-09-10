-- Train positions, one row per train per snapshot.
--
-- This layer does three things and nothing else: it builds the keys the rest
-- of the project joins on, it flags the two known data quality problems in the
-- feed, and it converts raw timestamps into the derived measures every
-- downstream model would otherwise recompute.
--
-- On keys, because this is the single most important thing to get right here:
--
--   snapshot_train_key = fetch_id + entity_id
--       Unique within a snapshot. Use it whenever you are looking at one
--       moment in time.
--
--   trip_key = trip_id + start_date
--       The only way to follow one train across snapshots. entity_id cannot
--       do this: it is the entity's position in the protobuf message, and it
--       shifts every time a train ahead of it terminates. The same physical
--       train was observed as entity 000014, then 000012, then 000006 within
--       eight minutes.
--
-- Neither key works for both jobs, which is why both exist.

with source as (

    select * from {{ source('pulselake_raw', 'raw_vehicle_positions') }}

),

flagged as (

    select
        -- Keys
        fetch_id || '|' || entity_id                    as snapshot_train_key,
        coalesce(trip_id, 'unknown')
            || '|' || coalesce(start_date, 'unknown')   as trip_key,

        fetch_id,
        feed_key,
        entity_id,
        trip_id,
        start_date,
        route_id,
        direction,
        trip_path,

        -- Position
        stop_id,
        current_status,
        current_stop_sequence,

        -- Timestamps. header_timestamp is when MTA published the snapshot and
        -- is the correct time axis for anything comparing snapshots.
        -- fetched_at is when we asked, and the gap between them is our own
        -- pipeline latency rather than anything about the trains.
        header_timestamp,
        fetched_at,
        vehicle_timestamp,

        date_diff('second', header_timestamp, fetched_at)   as ingest_lag_seconds,

        -- How old the train's own position report was when the snapshot was
        -- published. A large value means MTA is showing us a stale train.
        date_diff('second', vehicle_timestamp, header_timestamp)
                                                            as position_age_seconds,

        -- Scheduled departure from the trip's first stop, decoded from the
        -- trip_id. Null for the small number of trips whose ids do not follow
        -- the NYCT convention.
        origin_departure_seconds,

        -- Data quality flag 1: NYCT reuses a trip_id for two physically
        -- different trains in roughly one percent of rows. Counting over the
        -- snapshot rather than over all history keeps this measuring the real
        -- problem instead of just counting how long the train has existed.
        count(*) over (partition by fetch_id, trip_id) > 1  as has_reused_trip_id,

        -- Data quality flag 2: trips whose id did not parse, so direction and
        -- scheduled origin are unavailable.
        direction is null                                   as is_unparsed_trip_id

    from source

),

ranked as (

    select
        *,
        -- 1 marks the newest snapshot for each feed. The current state marts
        -- filter on this rather than on max(header_timestamp), which would
        -- need a second pass over the table.
        dense_rank() over (
            partition by feed_key
            order by header_timestamp desc
        ) as snapshot_recency_rank

    from flagged

)

select * from ranked
