-- Pipeline health, one row per snapshot.
--
-- This model is about the pipeline, not about the trains. It is what you look
-- at when a number on the dashboard seems wrong and you need to work out
-- whether the trains changed or the data did.
--
-- The gap column is the important one. A dashboard reading from stale data
-- looks exactly like a dashboard reading from a quiet system, and the only
-- difference is whether anything is measuring the clock.

with fetches as (

    select * from {{ source('pulselake_raw', 'raw_feed_fetches') }}

),

positions as (

    select
        fetch_id,
        count(*)                                            as vehicle_rows,
        count(distinct trip_id)                             as distinct_trip_ids,
        count(*) filter (where has_reused_trip_id)          as rows_with_reused_trip_id,
        count(*) filter (where is_unparsed_trip_id)         as rows_with_unparsed_trip_id,
        count(*) filter (where route_id is null)            as rows_missing_route,
        count(*) filter (where stop_id is null)             as rows_missing_stop,
        round(avg(position_age_seconds), 1)                 as avg_position_age_seconds,
        round(avg(ingest_lag_seconds), 1)                   as avg_ingest_lag_seconds
    from {{ ref('stg_vehicle_positions') }}
    group by 1

),

alerts as (

    select
        fetch_id,
        count(*)                                    as alert_rows,
        count(*) filter (where is_delay_alert)      as delay_alert_rows
    from {{ ref('stg_alerts') }}
    group by 1

),

joined as (

    select
        f.fetch_id,
        f.feed_key,
        f.header_timestamp                                  as snapshot_at,
        f.fetched_at,
        f.payload_bytes,
        f.entity_count,
        f.poll_count,
        f.last_polled_at,

        coalesce(p.vehicle_rows, 0)                         as vehicle_rows,
        f.trip_stop_rows,
        coalesce(a.alert_rows, 0)                           as alert_rows,
        coalesce(a.delay_alert_rows, 0)                     as delay_alert_rows,

        coalesce(p.distinct_trip_ids, 0)                    as distinct_trip_ids,
        coalesce(p.rows_with_reused_trip_id, 0)             as rows_with_reused_trip_id,
        coalesce(p.rows_with_unparsed_trip_id, 0)           as rows_with_unparsed_trip_id,
        coalesce(p.rows_missing_route, 0)                   as rows_missing_route,
        coalesce(p.rows_missing_stop, 0)                    as rows_missing_stop,

        p.avg_position_age_seconds,
        p.avg_ingest_lag_seconds,

        -- Seconds since the previous snapshot for the same feed. This is how
        -- you spot a poller that died at 3am: the gap jumps from 20 seconds
        -- to 6 hours and nothing else in the data would show it.
        date_diff(
            'second',
            lag(f.header_timestamp) over (
                partition by f.feed_key order by f.header_timestamp
            ),
            f.header_timestamp
        ) as seconds_since_previous_snapshot

    from fetches f
    left join positions p on f.fetch_id = p.fetch_id
    left join alerts    a on f.fetch_id = a.fetch_id

),

scored as (

    select
        *,
        round(
            100.0 * rows_with_reused_trip_id / nullif(vehicle_rows, 0), 2
        ) as pct_rows_with_reused_trip_id,

        round(
            100.0 * rows_with_unparsed_trip_id / nullif(vehicle_rows, 0), 2
        ) as pct_rows_with_unparsed_trip_id,

        -- A gap of more than five minutes means ingestion stopped, whatever
        -- the reason. Flagged rather than filtered, because the gaps are the
        -- interesting rows.
        coalesce(seconds_since_previous_snapshot, 0) > 300  as is_after_ingestion_gap

    from joined

)

select * from scored
