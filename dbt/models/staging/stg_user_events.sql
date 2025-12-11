{{
  config(
    materialized='view',
    tags=['staging', 'events']
  )
}}

with source as (
    select * from {{ source('raw', 'user_events') }}
),

renamed as (
    select
        event_id,
        user_id,
        event_type,
        event_timestamp,
        event_data,
        created_at,

        -- Extract common fields from JSONB
        event_data->>'page' as page_url,
        event_data->>'session_id' as session_id,
        event_data->'metadata'->>'browser' as browser,
        event_data->'metadata'->>'device' as device,

        -- Time-based fields
        date_trunc('hour', event_timestamp) as event_hour,
        date_trunc('day', event_timestamp) as event_day,
        extract(hour from event_timestamp) as hour_of_day,
        extract(dow from event_timestamp) as day_of_week

    from source
)

select * from renamed
