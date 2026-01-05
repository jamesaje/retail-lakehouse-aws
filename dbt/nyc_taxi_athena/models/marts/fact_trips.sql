{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['pickup_year', 'pickup_month']
) }}

with trips as (

    select *
    from {{ ref('stg_yellow_trips') }}

    {% if is_incremental() %}
      -- On incremental runs, only process months >= the latest month already in the target table
      where (pickup_year, pickup_month) >= (
        select
          max(pickup_year) as pickup_year,
          max(pickup_month) as pickup_month
        from {{ this }}
      )
    {% endif %}

),

final as (

    select
        -- Natural key for now (we'll improve later with hashing when you ingest more months)
        row_number() over (order by pickup_ts, dropoff_ts, pu_location_id, do_location_id) as trip_id,

        date(pickup_ts) as pickup_date,

        vendor_id,
        passenger_count,
        rate_code_id,
        pu_location_id,
        do_location_id,
        payment_type,

        pickup_ts,
        dropoff_ts,

        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,

        -- Derived metric: trip duration in minutes
        date_diff('second', pickup_ts, dropoff_ts) / 60.0 as trip_duration_minutes,

        pickup_year,
        pickup_month

    from trips
    where pickup_ts is not null
      and dropoff_ts is not null
      and dropoff_ts >= pickup_ts
      and date_diff('second', pickup_ts, dropoff_ts) between 0 and 86400
)

select * from final
