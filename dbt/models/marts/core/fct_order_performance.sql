{{
  config(
    materialized='table',
    tags=['core', 'daily', 'orders', 'performance']
  )
}}

with daily_orders as (
    select
        o.order_day::date as date,
        o.order_month,
        o.order_year_num as year,
        o.order_month_num as month,

        -- Order counts
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as unique_customers,
        count(distinct o.employee_id) as unique_employees,

        -- Shipping metrics
        sum(case when o.is_shipped then 1 else 0 end) as shipped_orders,
        sum(case when not o.is_shipped then 1 else 0 end) as unshipped_orders,
        sum(case when o.shipping_performance = 'On Time' then 1 else 0 end) as on_time_orders,
        sum(case when o.shipping_performance = 'Late' then 1 else 0 end) as late_orders,

        -- Shipping timing
        avg(case when o.is_shipped then o.days_to_ship else null end) as avg_days_to_ship,
        max(case when o.is_shipped then o.days_to_ship else null end) as max_days_to_ship,
        min(case when o.is_shipped then o.days_to_ship else null end) as min_days_to_ship,

        -- Freight analysis
        sum(o.freight) as total_freight,
        avg(o.freight) as avg_freight_per_order,
        sum(case when o.freight_tier = 'High Freight' then 1 else 0 end) as high_freight_orders

    from {{ ref('stg_orders') }} o
    group by 1, 2, 3, 4
),

daily_revenue as (
    select
        o.order_day::date as date,

        -- Revenue metrics
        count(distinct od.product_id) as unique_products_sold,
        sum(od.quantity) as total_units_sold,
        sum(od.extended_price) as total_revenue,
        sum(od.line_subtotal) as gross_revenue,
        sum(od.discount_amount) as total_discounts,

        avg(od.extended_price) as avg_line_item_value,
        sum(case when od.has_discount then 1 else 0 end) as discounted_line_items,
        sum(case when od.is_high_value_line then 1 else 0 end) as high_value_line_items

    from {{ ref('stg_orders') }} o
    inner join {{ ref('stg_order_details') }} od on o.order_id = od.order_id
    group by 1
),

combined as (
    select
        do.date,
        do.order_month,
        do.year,
        do.month,

        -- Order metrics
        do.total_orders,
        do.unique_customers,
        do.unique_employees,
        do.shipped_orders,
        do.unshipped_orders,
        do.on_time_orders,
        do.late_orders,
        do.avg_days_to_ship,
        do.max_days_to_ship,
        do.min_days_to_ship,

        -- Freight metrics
        do.total_freight,
        do.avg_freight_per_order,
        do.high_freight_orders,

        -- Revenue metrics
        dr.unique_products_sold,
        dr.total_units_sold,
        dr.total_revenue,
        dr.gross_revenue,
        dr.total_discounts,
        dr.avg_line_item_value,
        dr.discounted_line_items,
        dr.high_value_line_items

    from daily_orders do
    left join daily_revenue dr on do.date = dr.date
)

select
    *,

    -- Calculated KPIs
    case
        when total_orders > 0
        then round(100.0 * shipped_orders / total_orders, 2)
        else 0
    end as shipping_rate_pct,

    case
        when shipped_orders > 0
        then round(100.0 * on_time_orders / shipped_orders, 2)
        else 0
    end as on_time_rate_pct,

    case
        when total_orders > 0
        then round(total_revenue / total_orders, 2)
        else 0
    end as avg_order_value,

    case
        when gross_revenue > 0
        then round(100.0 * total_discounts / gross_revenue, 2)
        else 0
    end as discount_rate_pct,

    total_revenue + total_freight as grand_total_with_freight,

    current_timestamp as dbt_updated_at

from combined
order by date desc
