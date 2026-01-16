{{ config(materialized='view') }}

with trips as (
    select pickup_ts
    from {{ ref('stg_yellow_trips') }}
    where pickup_ts is not null
),

dates as (
    select distinct
        date(pickup_ts) as date_day
    from trips
)

select
    date_day,
    year(date_day) as year,
    month(date_day) as month,
    day(date_day) as day,
    day_of_week(date_day) as day_of_week,
    date_format(date_day, '%Y-%m') as year_month
from dates
