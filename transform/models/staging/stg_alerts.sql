-- Service alerts, one row per affected trip per snapshot.
--
-- These matter more than they look. The NYCT feed does not populate the
-- standard GTFS delay field at all, so alerts whose header text reads
-- "Train delayed" are the only place MTA states outright that a train is
-- late. Everything else in this project infers delay; this observes it.

with source as (

    select * from {{ source('pulselake_raw', 'raw_alerts') }}

),

classified as (

    select
        fetch_id,
        feed_key,
        entity_id,
        alert_header,

        trip_id,
        start_date,
        route_id,
        direction,

        coalesce(trip_id, 'unknown')
            || '|' || coalesce(start_date, 'unknown')   as trip_key,

        header_timestamp,
        fetched_at,

        -- Matching on text is fragile and it is worth being honest about that
        -- in the model rather than hiding it. If MTA changes the wording this
        -- silently stops matching, which is exactly why the count of delay
        -- alerts is tracked as a data quality metric in dq_ingestion_health
        -- instead of being assumed correct.
        lower(coalesce(alert_header, '')) like '%delay%'  as is_delay_alert,

        trip_id is not null                              as is_trip_level

    from source

)

select * from classified
