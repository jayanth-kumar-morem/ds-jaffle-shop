with

customers as (

    select * from {{ ref('stgCustomers') }}

),

orders as (

    select * from {{ ref('orders') }}

),

customerOrdersSummary as (

    select
        orders.customerId,

        count(distinct orders.orderId) as countLifetimeOrders,
        count(distinct orders.orderId) > 1 as isRepeatBuyer,
        min(orders.orderedAt) as firstOrderedAt,
        max(orders.orderedAt) as lastOrderedAt,
        sum(orders.subtotal) as lifetimeSpendPretax,
        sum(orders.taxPaid) as lifetimeTaxPaid,
        sum(orders.orderTotal) as lifetimeSpend

    from orders

    group by 1

),

joined as (

    select
        customers.*,

        customerOrdersSummary.countLifetimeOrders,
        customerOrdersSummary.firstOrderedAt,
        customerOrdersSummary.lastOrderedAt,
        customerOrdersSummary.lifetimeSpendPretax,
        customerOrdersSummary.lifetimeTaxPaid,
        customerOrdersSummary.lifetimeSpend,

        case
            when customerOrdersSummary.isRepeatBuyer then 'returning'
            else 'new'
        end as customerType

    from customers

    left join customerOrdersSummary
        on customers.customerId = customerOrdersSummary.customerId

)

select * from joined