{{
  config(
    materialized='view',
    tags=['staging', 'sales']
  )
}}

with source as (
    select * from {{ source('raw', 'sales_transactions') }}
),

renamed as (
    select
        transaction_id,
        user_id,
        product_id,
        amount,
        currency,
        transaction_date,
        status,
        created_at,

        -- Add calculated fields
        date_trunc('day', transaction_date) as transaction_day,
        date_trunc('month', transaction_date) as transaction_month,
        date_trunc('year', transaction_date) as transaction_year,

        -- Business logic
        case
            when status = 'completed' then true
            else false
        end as is_completed,

        case
            when amount >= 1000 then 'high'
            when amount >= 100 then 'medium'
            else 'low'
        end as transaction_tier

    from source
)

select * from renamed
