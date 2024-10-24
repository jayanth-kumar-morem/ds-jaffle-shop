with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items_summary as (
    select
        order_id,
        sum(supply_cost) as order_cost,
        sum(product_price) as order_items_subtotal,
        count(*) as count_order_items,
        sum(case when is_food_item then 1 else 0 end) as count_food_items,
        sum(case when is_drink_item then 1 else 0 end) as count_drink_items
    from {{ ref('order_items') }}
    group by 1
),

final as (
    select
        orders.*,
        coalesce(ois.order_cost, 0) as order_cost,
        coalesce(ois.order_items_subtotal, 0) as order_items_subtotal,
        coalesce(ois.count_food_items, 0) as count_food_items,
        coalesce(ois.count_drink_items, 0) as count_drink_items,
        coalesce(ois.count_order_items, 0) as count_order_items,
        coalesce(ois.count_food_items, 0) > 0 as is_food_order,
        coalesce(ois.count_drink_items, 0) > 0 as is_drink_order,
        row_number() over (
            partition by orders.customer_id
            order by orders.ordered_at asc
        ) as customer_order_number
    from orders
    left join order_items_summary ois
        on orders.order_id = ois.order_id
)

select * from final