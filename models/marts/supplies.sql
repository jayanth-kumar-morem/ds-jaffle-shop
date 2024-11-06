with

supplies as (
    select * from {{ ref('stg_supplies') }}
),

england_supplies as (
    select s.*
    from supplies s
    join {{ ref('stg_locations') }} l on s.location_id = l.location_id
    where l.country = 'England'
)

select * from england_supplies