with

source as (
    select * from {{ source('ecom', 'raw_items') }}
),

renamed as (
    select
        ----------  ids
        id as order_item_id,
        order_id,
        sku as product_id
    from source
),

england_order_items as (
    select r.*
    from renamed r
    join {{ ref('stg_orders') }} o on r.order_id = o.order_id
    join {{ ref('stg_customers_england') }} c on o.customer_id = c.customer_id
)

select * from england_order_items