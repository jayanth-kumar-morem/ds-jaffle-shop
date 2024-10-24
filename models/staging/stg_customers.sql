with

source as (

    select * from {{ source('ecom', 'raw_blessed_users') }}

),

renamed as (

    select

        ----------  ids
        id as blessed_user_id,

        ---------- text
        name as blessed_user_name

    from source

)

select * from renamed