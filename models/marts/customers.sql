with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

customer_orders_summary as (
    select
        customer_id,
        count(distinct order_id) as count_lifetime_orders,
        count(distinct order_id) > 1 as is_repeat_buyer,
        min(ordered_at) as first_ordered_at,
        max(ordered_at) as last_ordered_at,
        sum(subtotal) as lifetime_spend_pretax,
        sum(tax_paid) as lifetime_tax_paid,
        sum(order_total) as lifetime_spend
    from orders
    group by 1
),

final as (
    select
        c.*,
        coalesce(cos.count_lifetime_orders, 0) as count_lifetime_orders,
        cos.first_ordered_at,
        cos.last_ordered_at,
        coalesce(cos.lifetime_spend_pretax, 0) as lifetime_spend_pretax,
        coalesce(cos.lifetime_tax_paid, 0) as lifetime_tax_paid,
        coalesce(cos.lifetime_spend, 0) as lifetime_spend,
        case
            when cos.is_repeat_buyer then 'returning'
            else 'new'
        end as customer_type
    from customers c
    left join customer_orders_summary cos
        on c.customer_id = cos.customer_id
)

select * from final