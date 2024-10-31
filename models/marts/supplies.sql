with

supplies as (

    select * from {{ ref('stgSupplies') }}

)

select * from supplies