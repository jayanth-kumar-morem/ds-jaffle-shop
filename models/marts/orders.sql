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

compute_booleans as (
    select
        orders.*,
        coalesce(order_items_summary.order_cost, 0) as order_cost,
        coalesce(order_items_summary.order_items_subtotal, 0) as order_items_subtotal,
        coalesce(order_items_summary.count_food_items, 0) as count_food_items,
        coalesce(order_items_summary.count_drink_items, 0) as count_drink_items,
        coalesce(order_items_summary.count_order_items, 0) as count_order_items,
        coalesce(order_items_summary.count_food_items, 0) > 0 as is_food_order,
        coalesce(order_items_summary.count_drink_items, 0) > 0 as is_drink_order
    from orders
    left join order_items_summary
        on orders.order_id = order_items_summary.order_id
),

final as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by ordered_at asc
        ) as customer_order_number
    from compute_booleans
)

select * from final