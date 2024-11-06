with source as (
    select * from {{ source('ecom', 'raw_customers') }}
),

renamed as (
    select
        ----------  ids
        id as customer_id,

        ---------- text
        name as customer_name,

        ---------- location
        country

    from source
),

customers_in_england as (
    select *
    from renamed
    where country = 'England'
)

select * from customers_in_england