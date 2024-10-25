with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

order_supplies_summary as (
    select
        product_id,
        sum(supply_cost) as supply_cost
    from {{ ref('stg_supplies') }}
    group by 1
),

final as (
    select
        oi.*,
        o.ordered_at,
        p.product_name,
        p.product_price,
        p.is_food_item,
        p.is_drink_item,
        coalesce(oss.supply_cost, 0) as supply_cost
    from order_items oi
    left join orders o on oi.order_id = o.order_id
    left join products p on oi.product_id = p.product_id
    left join order_supplies_summary oss on oi.product_id = oss.product_id
)

select * from final