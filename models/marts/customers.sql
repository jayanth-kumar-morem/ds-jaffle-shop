with

blessed_users as (

    select * from {{ ref('stg_blessed_users') }}

),

orders as (

    select * from {{ ref('orders') }}

),

blessed_user_orders_summary as (

    select
        orders.blessed_user_id,

        count(distinct orders.order_id) as count_lifetime_orders,
        count(distinct orders.order_id) > 1 as is_repeat_buyer,
        min(orders.ordered_at) as first_ordered_at,
        max(orders.ordered_at) as last_ordered_at,
        sum(orders.subtotal) as lifetime_spend_pretax,
        sum(orders.tax_paid) as lifetime_tax_paid,
        sum(orders.order_total) as lifetime_spend

    from orders

    group by 1

),

joined as (

    select
        blessed_users.*,

        blessed_user_orders_summary.count_lifetime_orders,
        blessed_user_orders_summary.first_ordered_at,
        blessed_user_orders_summary.last_ordered_at,
        blessed_user_orders_summary.lifetime_spend_pretax,
        blessed_user_orders_summary.lifetime_tax_paid,
        blessed_user_orders_summary.lifetime_spend,

        case
            when blessed_user_orders_summary.is_repeat_buyer then 'returning'
            else 'new'
        end as blessed_user_type

    from blessed_users

    left join blessed_user_orders_summary
        on blessed_users.blessed_user_id = blessed_user_orders_summary.blessed_user_id

)

select * from joined