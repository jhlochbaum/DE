{{
  config(
    materialized='view',
    tags=['staging', 'northwind', 'customers']
  )
}}

with source as (
    select * from customers
),

cleaned as (
    select
        customer_id,
        company_name,
        contact_name,
        contact_title,
        address,
        city,
        region,
        postal_code,
        country,
        phone,
        fax,

        -- Standardized fields
        upper(trim(country)) as country_code,
        lower(trim(city)) as city_normalized,

        -- Categorize by region
        case
            when country in ('USA', 'Canada', 'Mexico') then 'North America'
            when country in ('UK', 'Germany', 'France', 'Spain', 'Italy', 'Belgium',
                           'Switzerland', 'Austria', 'Poland', 'Sweden', 'Norway',
                           'Denmark', 'Finland', 'Ireland', 'Portugal') then 'Europe'
            when country in ('Brazil', 'Argentina', 'Venezuela') then 'South America'
            else 'Other'
        end as region_group,

        -- Extract contact type
        case
            when contact_title ilike '%owner%' then 'Owner'
            when contact_title ilike '%manager%' then 'Manager'
            when contact_title ilike '%representative%' then 'Sales Rep'
            when contact_title ilike '%agent%' then 'Agent'
            else 'Other'
        end as contact_type

    from source
)

select * from cleaned
