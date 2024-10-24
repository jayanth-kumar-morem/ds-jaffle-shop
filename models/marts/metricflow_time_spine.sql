-- metricflow_time_spine.sql

{{ config(materialized='table') }}

with date_spine as (
    {{ dbt_date.get_date_spine(
        start_date="cast('2000-01-01' as date)",
        end_date="dateadd(year, 10, current_date)",
        datepart="day"
    ) }}
)

select 
    cast(date_day as date) as date_day
from 
    date_spine