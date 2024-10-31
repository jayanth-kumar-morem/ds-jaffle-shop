with

source as (

    select * from {{ source('ecom', 'rawCustomers') }}

),

renamed as (

    select

        ----------  ids
        id as customerId,

        ---------- text
        name as customerName

    from source

)

select * from renamed