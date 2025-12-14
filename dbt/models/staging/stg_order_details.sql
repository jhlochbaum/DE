{{
  config(
    materialized='view',
    tags=['staging', 'northwind', 'order_details']
  )
}}

with source as (
    select * from order_details
),

calculated as (
    select
        od.order_id,
        od.product_id,
        od.unit_price,
        od.quantity,
        od.discount,

        -- Calculate extended price (total line item value)
        (od.unit_price * od.quantity) as line_subtotal,
        (od.unit_price * od.quantity * (1 - od.discount)) as extended_price,
        (od.unit_price * od.quantity * od.discount) as discount_amount,

        -- Categorize line items
        case
            when od.quantity >= 50 then 'Bulk Order'
            when od.quantity >= 20 then 'Large Order'
            when od.quantity >= 5 then 'Standard Order'
            else 'Small Order'
        end as quantity_tier,

        case
            when od.discount > 0 then true
            else false
        end as has_discount,

        case
            when od.discount >= 0.2 then 'High Discount'
            when od.discount >= 0.1 then 'Medium Discount'
            when od.discount > 0 then 'Low Discount'
            else 'No Discount'
        end as discount_tier,

        -- Flag high-value line items
        case
            when (od.unit_price * od.quantity * (1 - od.discount)) >= 1000 then true
            else false
        end as is_high_value_line

    from source od
)

select * from calculated
