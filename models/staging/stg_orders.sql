with

source as (

    select * from {{ source('ecom', 'rawOrders') }}

),

renamed as (

    select

        ----------  ids
        id as orderId,
        storeId as locationId,
        customer as customerId,

        ---------- numerics
        subtotal as subtotalCents,
        taxPaid as taxPaidCents,
        orderTotal as orderTotalCents,
        {{ centsToToDollars('subtotal') }} as subtotal,
        {{ centsToToDollars('taxPaid') }} as taxPaid,
        {{ centsToToDollars('orderTotal') }} as orderTotal,

        ---------- timestamps
        {{ dbt.date_trunc('day','orderedAt') }} as orderedAt

    from source

)

select * from renamed