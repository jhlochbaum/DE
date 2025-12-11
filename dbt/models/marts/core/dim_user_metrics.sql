{{
  config(
    materialized='table',
    tags=['core', 'daily', 'users']
  )
}}

with user_transactions as (
    select
        user_id,
        count(*) as total_transactions,
        count(distinct transaction_day) as active_days,
        sum(case when is_completed then 1 else 0 end) as completed_transactions,
        sum(case when is_completed then amount else 0 end) as lifetime_value,
        avg(case when is_completed then amount else null end) as avg_transaction_value,
        max(case when is_completed then amount else null end) as max_transaction_value,
        min(transaction_date) as first_transaction_date,
        max(transaction_date) as last_transaction_date,
        max(transaction_day) as last_transaction_day

    from {{ ref('stg_sales_transactions') }}
    group by 1
),

user_events as (
    select
        user_id,
        count(*) as total_events,
        count(distinct event_day) as active_event_days,
        count(distinct session_id) as total_sessions,
        min(event_timestamp) as first_event_date,
        max(event_timestamp) as last_event_date

    from {{ ref('stg_user_events') }}
    group by 1
),

combined as (
    select
        coalesce(t.user_id, e.user_id) as user_id,

        -- Transaction metrics
        coalesce(t.total_transactions, 0) as total_transactions,
        coalesce(t.completed_transactions, 0) as completed_transactions,
        coalesce(t.lifetime_value, 0) as lifetime_value,
        t.avg_transaction_value,
        t.max_transaction_value,
        t.first_transaction_date,
        t.last_transaction_date,
        t.active_days as transaction_active_days,

        -- Event metrics
        coalesce(e.total_events, 0) as total_events,
        coalesce(e.total_sessions, 0) as total_sessions,
        e.active_event_days,
        e.first_event_date,
        e.last_event_date,

        -- Engagement score (simple calculation)
        coalesce(t.total_transactions, 0) * 10 + coalesce(e.total_sessions, 0) as engagement_score,

        -- Recency (days since last activity)
        current_date - coalesce(t.last_transaction_day::date, e.last_event_date::date) as days_since_last_activity,

        -- User segment
        case
            when coalesce(t.lifetime_value, 0) >= 1000 then 'VIP'
            when coalesce(t.lifetime_value, 0) >= 500 then 'High Value'
            when coalesce(t.lifetime_value, 0) >= 100 then 'Medium Value'
            when coalesce(t.lifetime_value, 0) > 0 then 'Low Value'
            else 'No Purchase'
        end as user_segment

    from user_transactions t
    full outer join user_events e using (user_id)
)

select
    *,
    -- Activity status
    case
        when days_since_last_activity <= 7 then 'Active'
        when days_since_last_activity <= 30 then 'At Risk'
        when days_since_last_activity <= 90 then 'Dormant'
        else 'Churned'
    end as activity_status,

    current_timestamp as dbt_updated_at

from combined
