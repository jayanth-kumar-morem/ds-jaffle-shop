with

source as (
    select * from {{ source('ecom', 'raw_products') }}
),

renamed as (
    select
        ----------  ids
        sku as product_id,

        ---------- text
        name as product_name,
        type as product_type,
        description as product_description,

        ---------- numerics
        {{ cents_to_dollars('price') }} as product_price,

        ---------- booleans
        coalesce(type = 'jaffle', false) as is_food_item,
        coalesce(type = 'beverage', false) as is_drink_item

    from source
),

england_products as (
    select r.*
    from renamed r
    join {{ ref('stg_order_items_england') }} oi on r.product_id = oi.product_id
)

select distinct * from england_products