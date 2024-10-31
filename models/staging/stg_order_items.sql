with

source as (

    select * from {{ source('ecom', 'rawItems') }}

),

renamed as (

    select

        ----------  ids
        id as orderItemId,
        orderId,
        sku as productId

    from source

)

select * from renamed