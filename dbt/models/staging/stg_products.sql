{{
  config(
    materialized='view',
    tags=['staging', 'northwind', 'products']
  )
}}

with source as (
    select
        p.*,
        c.category_name,
        s.company_name as supplier_name,
        s.country as supplier_country
    from products p
    left join categories c on p.category_id = c.category_id
    left join suppliers s on p.supplier_id = s.supplier_id
),

enriched as (
    select
        product_id,
        product_name,
        supplier_id,
        supplier_name,
        supplier_country,
        category_id,
        category_name,
        quantity_per_unit,
        unit_price,
        units_in_stock,
        units_on_order,
        reorder_level,
        discontinued,

        -- Calculated fields
        units_in_stock + units_on_order as total_available_units,

        case
            when units_in_stock <= reorder_level and not discontinued then true
            else false
        end as needs_reorder,

        case
            when units_in_stock = 0 and not discontinued then 'Out of Stock'
            when units_in_stock <= reorder_level and not discontinued then 'Low Stock'
            when units_in_stock > reorder_level * 3 then 'Overstocked'
            else 'Normal'
        end as stock_status,

        -- Price categorization
        case
            when unit_price >= 50 then 'Premium'
            when unit_price >= 20 then 'Mid-Range'
            else 'Economy'
        end as price_tier,

        -- Product status
        case
            when discontinued then 'Discontinued'
            when units_in_stock = 0 then 'Out of Stock'
            when units_in_stock <= reorder_level then 'Low Stock'
            else 'Available'
        end as product_status

    from source
)

select * from enriched
