{{ config(materialized='table', schema='gold') }}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
)

select
    date_day as date_key,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    dayname(date_day) as day_name,
    quarter(date_day) as quarter
from date_spine