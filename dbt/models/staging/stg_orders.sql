{{
  config(
    materialized='view',
    tags=['staging', 'northwind', 'orders']
  )
}}

with source as (
    select * from orders
),

enriched as (
    select
        o.order_id,
        o.customer_id,
        o.employee_id,
        o.order_date,
        o.required_date,
        o.shipped_date,
        o.ship_via as shipper_id,
        o.freight,
        o.ship_name,
        o.ship_address,
        o.ship_city,
        o.ship_region,
        o.ship_postal_code,
        o.ship_country,

        -- Date transformations
        date_trunc('day', o.order_date) as order_day,
        date_trunc('week', o.order_date) as order_week,
        date_trunc('month', o.order_date) as order_month,
        date_trunc('quarter', o.order_date) as order_quarter,
        date_trunc('year', o.order_date) as order_year,

        extract(year from o.order_date) as order_year_num,
        extract(month from o.order_date) as order_month_num,
        extract(dow from o.order_date) as order_day_of_week,

        -- Calculated fields
        case
            when o.shipped_date is not null then true
            else false
        end as is_shipped,

        case
            when o.shipped_date is not null
            then o.shipped_date - o.order_date
            else null
        end as days_to_ship,

        case
            when o.required_date is not null and o.shipped_date is not null
            then o.shipped_date - o.required_date
            else null
        end as shipping_delay_days,

        -- Categorize shipping performance
        case
            when o.shipped_date is null then 'Not Shipped'
            when o.shipped_date <= o.required_date then 'On Time'
            when o.shipped_date <= o.required_date + interval '3 days' then 'Slightly Late'
            else 'Late'
        end as shipping_performance,

        -- Categorize freight cost
        case
            when o.freight >= 100 then 'High Freight'
            when o.freight >= 20 then 'Medium Freight'
            else 'Low Freight'
        end as freight_tier

    from source o
)

select * from enriched
