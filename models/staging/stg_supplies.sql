with

source as (
    select * from {{ source('ecom', 'raw_supplies') }}
),

renamed as (
    select
        ----------  ids
        {{ dbt_utils.generate_surrogate_key(['id', 'sku']) }} as supply_uuid,
        id as supply_id,
        sku as product_id,

        ---------- text
        name as supply_name,

        ---------- numerics
        {{ cents_to_dollars('cost') }} as supply_cost,

        ---------- booleans
        perishable as is_perishable_supply

    from source
),

england_supplies as (
    select r.*
    from renamed r
    join {{ ref('stg_locations_england') }} l on r.supply_id = l.location_id
)

select * from england_supplies