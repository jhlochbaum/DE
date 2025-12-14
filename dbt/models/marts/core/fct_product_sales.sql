{{
  config(
    materialized='table',
    tags=['core', 'fact', 'products', 'sales']
  )
}}

with product_order_details as (
    select
        p.product_id,
        p.product_name,
        p.category_id,
        p.category_name,
        p.supplier_id,
        p.supplier_name,
        p.supplier_country,
        p.unit_price as current_unit_price,
        p.units_in_stock,
        p.units_on_order,
        p.reorder_level,
        p.stock_status,
        p.price_tier,
        p.product_status,
        p.discontinued,

        od.order_id,
        od.quantity,
        od.unit_price as sold_unit_price,
        od.discount,
        od.extended_price,
        od.discount_amount,
        od.quantity_tier,
        od.discount_tier,

        o.order_date,
        o.customer_id,
        o.customer_country,
        o.order_month

    from {{ ref('stg_products') }} p
    left join {{ ref('stg_order_details') }} od on p.product_id = od.product_id
    left join {{ ref('stg_orders') }} o on od.order_id = o.order_id
),

product_metrics as (
    select
        product_id,
        product_name,
        category_id,
        category_name,
        supplier_id,
        supplier_name,
        supplier_country,
        current_unit_price,
        units_in_stock,
        units_on_order,
        reorder_level,
        stock_status,
        price_tier,
        product_status,
        discontinued,

        -- Sales metrics
        count(distinct order_id) as times_ordered,
        count(distinct customer_id) as unique_customers,
        count(distinct customer_country) as countries_sold_to,
        sum(quantity) as total_units_sold,
        sum(extended_price) as total_revenue,
        sum(discount_amount) as total_discounts_given,
        avg(sold_unit_price) as avg_selling_price,
        max(sold_unit_price) as max_selling_price,
        min(sold_unit_price) as min_selling_price,
        avg(discount) as avg_discount_rate,

        -- Temporal metrics
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        max(order_date)::date as last_sale_day,
        count(distinct order_month) as months_with_sales,

        -- Quantity analysis
        sum(case when quantity_tier = 'Bulk Order' then quantity else 0 end) as bulk_order_units,
        sum(case when quantity_tier = 'Large Order' then quantity else 0 end) as large_order_units,
        sum(case when quantity_tier = 'Standard Order' then quantity else 0 end) as standard_order_units,
        sum(case when quantity_tier = 'Small Order' then quantity else 0 end) as small_order_units

    from product_order_details
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
),

category_totals as (
    select
        category_name,
        sum(total_revenue) as category_total_revenue
    from product_metrics
    group by 1
),

enriched as (
    select
        pm.*,
        ct.category_total_revenue,

        -- Calculated metrics
        case
            when total_units_sold > 0
            then round(total_revenue / total_units_sold, 2)
            else 0
        end as revenue_per_unit,

        case
            when times_ordered > 0
            then round(total_units_sold::numeric / times_ordered, 2)
            else 0
        end as avg_quantity_per_order,

        case
            when total_revenue > 0
            then round(100.0 * total_discounts_given / (total_revenue + total_discounts_given), 2)
            else 0
        end as discount_rate_pct,

        -- Days since last sale
        current_date - last_sale_day as days_since_last_sale,

        -- Product share of category revenue
        case
            when ct.category_total_revenue > 0
            then round(100.0 * pm.total_revenue / ct.category_total_revenue, 2)
            else 0
        end as pct_of_category_revenue,

        -- Stock coverage (days of inventory at current sales rate)
        case
            when total_units_sold > 0 and months_with_sales > 0
            then round(units_in_stock::numeric / (total_units_sold::numeric / months_with_sales / 30), 1)
            else null
        end as days_of_stock_remaining

    from product_metrics pm
    left join category_totals ct on pm.category_name = ct.category_name
)

select
    *,

    -- Performance segmentation
    case
        when total_revenue >= 10000 then 'Star Product'
        when total_revenue >= 5000 then 'High Performer'
        when total_revenue >= 1000 then 'Solid Performer'
        when total_revenue > 0 then 'Low Performer'
        else 'Never Sold'
    end as performance_tier,

    -- Inventory risk assessment
    case
        when discontinued then 'Discontinued'
        when stock_status = 'Out of Stock' and total_revenue > 1000 then 'URGENT: Restock High Seller'
        when stock_status = 'Low Stock' and total_revenue > 5000 then 'High Priority Restock'
        when stock_status = 'Low Stock' then 'Monitor Stock'
        when stock_status = 'Overstocked' and days_since_last_sale > 90 then 'Liquidate Excess'
        when stock_status = 'Overstocked' then 'Reduce Inventory'
        else 'Normal'
    end as inventory_action,

    -- Sales velocity (units per month)
    case
        when months_with_sales > 0
        then round(total_units_sold::numeric / months_with_sales, 1)
        else 0
    end as units_per_month,

    current_timestamp as dbt_updated_at

from enriched
order by total_revenue desc nulls last
