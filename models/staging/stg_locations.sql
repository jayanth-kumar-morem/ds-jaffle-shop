with

source as (
    select * from {{ source('ecom', 'raw_stores') }}
),

renamed as (
    select
        ----------  ids
        id as location_id,

        ---------- text
        name as location_name,
        country,  -- Add country field

        ---------- numerics
        tax_rate,

        ---------- timestamps
        {{ dbt.date_trunc('day', 'opened_at') }} as opened_date

    from source
),

england_locations as (
    select *
    from renamed
    where country = 'England'
)

select * from england_locations