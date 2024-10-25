with source as (
    select * from {{ source('ecom', 'raw_products') }}
),

renamed as (
    select
        sku as product_id,
        name as product_name,
        type as product_type,
        description as product_description,
        {{ cents_to_dollars('price') }} as product_price,
        type = 'jaffle' as is_food_item,
        type = 'beverage' as is_drink_item
    from source
)

select * from renamed