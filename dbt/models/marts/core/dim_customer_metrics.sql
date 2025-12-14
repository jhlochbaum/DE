{{
  config(
    materialized='table',
    tags=['core', 'dimension', 'customers']
  )
}}

with customer_orders as (
    select
        c.customer_id,
        c.company_name,
        c.contact_name,
        c.country,
        c.city,
        c.region_group,
        c.contact_type,

        o.order_id,
        o.order_date,
        o.shipped_date,
        o.freight,
        o.shipping_performance,
        o.is_shipped

    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
),

customer_order_details as (
    select
        co.*,
        od.extended_price,
        od.quantity,
        od.discount_amount

    from customer_orders co
    left join {{ ref('stg_order_details') }} od on co.order_id = od.order_id
),

customer_aggregates as (
    select
        customer_id,
        company_name,
        contact_name,
        country,
        city,
        region_group,
        contact_type,

        -- Order metrics
        count(distinct order_id) as total_orders,
        count(distinct case when is_shipped then order_id end) as shipped_orders,
        count(distinct case when shipping_performance = 'On Time' then order_id end) as on_time_orders,

        -- Revenue metrics
        sum(extended_price) as lifetime_value,
        avg(extended_price) as avg_line_item_value,
        sum(quantity) as total_units_purchased,
        sum(discount_amount) as total_discounts_received,

        -- Freight metrics
        sum(freight) as total_freight_paid,
        avg(freight) as avg_freight_per_order,

        -- Temporal metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        max(order_date)::date as last_order_day,
        count(distinct date_trunc('month', order_date)) as active_months,
        count(distinct date_trunc('day', order_date)) as active_days

    from customer_order_details
    group by 1, 2, 3, 4, 5, 6, 7
),

enriched as (
    select
        *,

        -- Calculate derived metrics
        case
            when total_orders > 0
            then round(lifetime_value / total_orders, 2)
            else 0
        end as avg_order_value,

        case
            when total_orders > 0
            then round(100.0 * shipped_orders / total_orders, 2)
            else 0
        end as fulfillment_rate_pct,

        case
            when shipped_orders > 0
            then round(100.0 * on_time_orders / shipped_orders, 2)
            else 0
        end as on_time_delivery_rate_pct,

        -- Recency (days since last order)
        current_date - last_order_day as days_since_last_order,

        -- Customer lifespan (days from first to last order)
        last_order_day - first_order_date::date as customer_lifespan_days,

        -- RFM-style segmentation
        case
            when lifetime_value >= 10000 then 'VIP'
            when lifetime_value >= 5000 then 'High Value'
            when lifetime_value >= 1000 then 'Medium Value'
            when lifetime_value > 0 then 'Low Value'
            else 'No Orders'
        end as value_segment,

        case
            when total_orders >= 20 then 'Frequent'
            when total_orders >= 10 then 'Regular'
            when total_orders >= 5 then 'Occasional'
            when total_orders > 0 then 'One-Time'
            else 'Never Ordered'
        end as frequency_segment

    from customer_aggregates
)

select
    *,

    -- Activity status based on recency
    case
        when days_since_last_order is null then 'Never Ordered'
        when days_since_last_order <= 30 then 'Active'
        when days_since_last_order <= 90 then 'At Risk'
        when days_since_last_order <= 180 then 'Dormant'
        else 'Churned'
    end as activity_status,

    -- Combined RFM score (simplified)
    case
        when value_segment = 'VIP' and frequency_segment = 'Frequent'
            and (days_since_last_order is null or days_since_last_order <= 30) then 'Champion'
        when value_segment in ('VIP', 'High Value') and frequency_segment in ('Frequent', 'Regular')
            then 'Loyal'
        when (days_since_last_order is not null and days_since_last_order > 90)
            and value_segment in ('VIP', 'High Value') then 'At Risk VIP'
        when total_orders <= 2 and (days_since_last_order is null or days_since_last_order <= 90)
            then 'New Customer'
        when days_since_last_order is not null and days_since_last_order > 180 then 'Lost'
        else 'Standard'
    end as customer_tier,

    current_timestamp as dbt_updated_at

from enriched
order by lifetime_value desc nulls last
