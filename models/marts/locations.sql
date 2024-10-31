with

locations as (

    select * from {{ ref('stgLocations') }}

)

select * from locations