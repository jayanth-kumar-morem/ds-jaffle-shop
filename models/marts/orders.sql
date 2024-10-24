with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('order_items') }}
),

order_items_summary as (
    select
        order_id,
        sum(supply_cost) as order_cost,
        sum(product_price) as order_items_subtotal,
        count(order_item_id) as count_order_items,
        sum(case when is_food_item then 1 else 0 end) as count_food_items,
        sum(case when is_drink_item then 1 else 0 end) as count_drink_items
    from order_items
    group by 1
),

orders_with_summaries as (
    select
        o.*,
        ois.order_cost,
        ois.order_items_subtotal,
        ois.count_food_items,
        ois.count_drink_items,
        ois.count_order_items,
        ois.count_food_items > 0 as is_food_order,
        ois.count_drink_items > 0 as is_drink_order
    from orders o
    left join order_items_summary ois on o.order_id = ois.order_id
),

final as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by ordered_at asc
        ) as customer_order_number
    from orders_with_summaries
)

select * from final