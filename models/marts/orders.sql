with

orders as (

    select * from {{ ref('stgOrders') }}

),

orderItems as (

    select * from {{ ref('orderItems') }}

),

orderItemsSummary as (

    select
        orderId,

        sum(supplyCost) as orderCost,
        sum(productPrice) as orderItemsSubtotal,
        count(orderItemId) as countOrderItems,
        sum(
            case
                when isFoodItem then 1
                else 0
            end
        ) as countFoodItems,
        sum(
            case
                when isDrinkItem then 1
                else 0
            end
        ) as countDrinkItems

    from orderItems

    group by 1

),

computeBooleans as (

    select
        orders.*,

        orderItemsSummary.orderCost,
        orderItemsSummary.orderItemsSubtotal,
        orderItemsSummary.countFoodItems,
        orderItemsSummary.countDrinkItems,
        orderItemsSummary.countOrderItems,
        orderItemsSummary.countFoodItems > 0 as isFoodOrder,
        orderItemsSummary.countDrinkItems > 0 as isDrinkOrder

    from orders

    left join
        orderItemsSummary
        on orders.orderId = orderItemsSummary.orderId

),

customerOrderCount as (

    select
        *,

        row_number() over (
            partition by customerId
            order by orderedAt asc
        ) as customerOrderNumber

    from computeBooleans

)

select * from customerOrderCount