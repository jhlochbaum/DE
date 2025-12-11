{{
  config(
    materialized='table',
    tags=['core', 'daily', 'sales']
  )
}}

with daily_transactions as (
    select
        transaction_day::date as date,
        count(distinct transaction_id) as total_transactions,
        count(distinct user_id) as unique_users,
        count(distinct product_id) as unique_products,
        sum(amount) as total_revenue,
        avg(amount) as avg_transaction_value,
        max(amount) as max_transaction_value,
        min(amount) as min_transaction_value,

        -- Completed vs failed
        sum(case when is_completed then 1 else 0 end) as completed_transactions,
        sum(case when not is_completed then 1 else 0 end) as failed_transactions,

        -- Transaction tiers
        sum(case when transaction_tier = 'high' then 1 else 0 end) as high_value_transactions,
        sum(case when transaction_tier = 'medium' then 1 else 0 end) as medium_value_transactions,
        sum(case when transaction_tier = 'low' then 1 else 0 end) as low_value_transactions,

        -- Revenue by tier
        sum(case when transaction_tier = 'high' and is_completed then amount else 0 end) as high_tier_revenue,
        sum(case when transaction_tier = 'medium' and is_completed then amount else 0 end) as medium_tier_revenue,
        sum(case when transaction_tier = 'low' and is_completed then amount else 0 end) as low_tier_revenue

    from {{ ref('stg_sales_transactions') }}
    where transaction_date >= '{{ var("start_date") }}'
    group by 1
)

select
    *,
    -- Calculated fields
    case
        when total_transactions > 0
        then round(100.0 * completed_transactions / total_transactions, 2)
        else 0
    end as completion_rate,

    current_timestamp as dbt_updated_at

from daily_transactions
order by date desc
