with

products as (

    select * from {{ ref('stgProducts') }}

)

select * from products