-- Every train currently running, one row per train, from the newest snapshot.
--
-- This is the model the dashboard's main table reads. Grain is one row per
-- (feed_key, entity_id) in the latest snapshot, which is guaranteed unique and
-- is asserted by a test in marts.yml.
--
-- A warning about the join between positions and predictions, because the
-- obvious version of it is wrong and fails silently.
--
-- A train appears in the feed as TWO separate entities: one vehicle entity
-- and one trip_update entity, with different entity_ids that merely happen to
-- be adjacent (000170 and 000169 for the same train). Joining the two tables
-- on entity_id therefore matches nothing at all, ever. It produced a table
-- where every prediction column was null while every structural test still
-- passed, because a left join that matches nothing does not violate
-- uniqueness or not_null.
--
-- The correct key is trip_id, start_date and stop_id together:
--
--   join on trip_id alone           209 rows out of 207 trains, fanning out
--                                   on the reused trip ids
--   join on trip_id and stop_id     207 rows, exactly one per train
--
-- The stop_id is what disambiguates the two trains sharing a trip_id, since
-- they are at different stations. A handful of trains per snapshot have a
-- vehicle stop_id that does not match their own next prediction, and those
-- are left null on purpose and counted in fct_line_activity rather than being
-- forced into a match.

with latest_positions as (

    select *
    from {{ ref('stg_vehicle_positions') }}
    where snapshot_recency_rank = 1

),

next_stop as (

    select
        fetch_id,
        entity_id           as trip_update_entity_id,
        trip_id,
        start_date,
        stop_id,
        arrival_time        as next_stop_arrival_time,
        seconds_to_arrival  as seconds_to_next_stop
    from {{ ref('stg_trip_stop_updates') }}
    where is_next_stop
    -- Belt and braces against fan out. If the two trains sharing a trip_id
    -- ever sat at the same stop, the stop_id would stop disambiguating them
    -- and this join would double every affected row. Keeping one is the
    -- lesser evil, and the unique test on snapshot_train_key still guards
    -- the outcome.
    qualify row_number() over (
        partition by fetch_id, trip_id, start_date, stop_id
        order by entity_id
    ) = 1

),

-- The stop after the next one, reached through the trip_update entity that
-- next_stop already resolved. Within the stop updates table entity_id IS
-- unique per snapshot, so this join is safe.
following_stop as (

    select
        fetch_id,
        entity_id           as trip_update_entity_id,
        stop_id             as following_stop_id,
        arrival_time        as following_stop_arrival_time
    from {{ ref('stg_trip_stop_updates') }}
    where stop_index = 1

),

-- One train can attract more than one alert, so this is aggregated to one row
-- per trip before the join. Without that the join would multiply rows and
-- quietly inflate every count on the dashboard.
trip_alerts as (

    select
        fetch_id,
        trip_key,
        max(is_delay_alert)                                   as has_delay_alert,
        count(*)                                              as alert_count,
        string_agg(distinct alert_header, '; ')               as alert_text
    from {{ ref('stg_alerts') }}
    where is_trip_level
    group by 1, 2

),

joined as (

    select
        p.snapshot_train_key,
        p.trip_key,
        p.feed_key,
        p.fetch_id,
        p.entity_id,
        p.trip_id,
        p.route_id,
        p.direction,
        p.start_date,

        p.current_status,
        p.stop_id                       as current_stop_id,
        p.current_stop_sequence,

        n.stop_id                       as next_stop_id,
        n.next_stop_arrival_time,
        n.seconds_to_next_stop,
        round(n.seconds_to_next_stop / 60.0, 1)  as minutes_to_next_stop,

        f.following_stop_id,
        f.following_stop_arrival_time,

        coalesce(a.has_delay_alert, false)       as has_delay_alert,
        a.alert_text,

        -- The two feed quality flags, carried through so the dashboard can
        -- show how much of what it is displaying is affected.
        p.has_reused_trip_id,
        p.is_unparsed_trip_id,

        p.position_age_seconds,
        p.ingest_lag_seconds,
        p.header_timestamp                       as snapshot_at,
        p.fetched_at,

        -- A readable status for the dashboard table. Ordering matters: an
        -- explicit MTA delay alert outranks anything inferred from timings.
        case
            when coalesce(a.has_delay_alert, false)       then 'Delayed (MTA alert)'
            when p.current_status = 'STOPPED_AT'          then 'At station'
            when p.current_status = 'INCOMING_AT'         then 'Arriving'
            when p.current_status = 'IN_TRANSIT_TO'       then 'In transit'
            else 'Unknown'
        end                                      as status_label

    from latest_positions p
    left join next_stop n
        on  p.fetch_id = n.fetch_id
        and p.trip_id  = n.trip_id
        -- start_date is nullable, and null never equals null in SQL, so a
        -- plain equality here would drop those rows from the join.
        and coalesce(p.start_date, '') = coalesce(n.start_date, '')
        and p.stop_id  = n.stop_id
    left join following_stop f
        on  n.fetch_id              = f.fetch_id
        and n.trip_update_entity_id = f.trip_update_entity_id
    left join trip_alerts a
        on  p.fetch_id = a.fetch_id
        and p.trip_key = a.trip_key

)

select * from joined
