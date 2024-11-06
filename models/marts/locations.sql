with

locations as (
    select * from {{ ref('stg_locations') }}
),

england_locations as (
    select *
    from locations
    where country = 'England'
)

select * from england_locations