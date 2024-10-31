with

orderItems as (

    select * from {{ ref('stgOrderItems') }}

),


orders as (

    select * from {{ ref('stgOrders') }}

),

products as (

    select * from {{ ref('stgProducts') }}

),

supplies as (

    select * from {{ ref('stgSupplies') }}

),

orderSuppliesSummary as (

    select
        productId,

        sum(supplyCost) as supplyCost

    from supplies

    group by 1

),

joined as (

    select
        orderItems.*,

        orders.orderedAt,

        products.productName,
        products.productPrice,
        products.isFoodItem,
        products.isDrinkItem,

        orderSuppliesSummary.supplyCost

    from orderItems

    left join orders on orderItems.orderId = orders.orderId

    left join products on orderItems.productId = products.productId

    left join orderSuppliesSummary
        on orderItems.productId = orderSuppliesSummary.productId

)

select * from joined