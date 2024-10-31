with

source as (

    select * from {{ source('ecom', 'rawSupplies') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supplyUuid,
        id as supplyId,
        sku as productId,

        ---------- text
        name as supplyName,

        ---------- numerics
        {{ centsToToDollars('cost') }} as supplyCost,

        ---------- booleans
        perishable as isPerishableSupply

    from source

)

select * from renamed