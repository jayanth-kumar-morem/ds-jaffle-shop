with

source as (

    select * from {{ source('ecom', 'rawStores') }}

),

renamed as (

    select

        ----------  ids
        id as locationId,

        ---------- text
        name as locationName,

        ---------- numerics
        taxRate,

        ---------- timestamps
        {{ dbt.date_trunc('day', 'openedAt') }} as openedDate

    from source

)

select * from renamed