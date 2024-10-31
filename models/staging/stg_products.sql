with

source as (

    select * from {{ source('ecom', 'rawProducts') }}

),

renamed as (

    select

        ----------  ids
        sku as productId,

        ---------- text
        name as productName,
        type as productType,
        description as productDescription,


        ---------- numerics
        {{ centsToToDollars('price') }} as productPrice,

        ---------- booleans
        coalesce(type = 'jaffle', false) as isFoodItem,

        coalesce(type = 'beverage', false) as isDrinkItem

    from source

)

select * from renamed