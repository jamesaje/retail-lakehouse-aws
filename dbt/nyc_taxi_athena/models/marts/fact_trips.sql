{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partitioned_by=['pickup_year', 'pickup_month'],
    unique_key='trip_id'
) }}

with trips as (

    select distinct
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
        pickup_year,
        pickup_month
    from {{ ref('stg_yellow_trips') }}
    where pickup_ts is not null
      and dropoff_ts is not null
      and dropoff_ts >= pickup_ts
      and date_diff('second', pickup_ts, dropoff_ts) between 0 and 86400

    {% if is_incremental() %}
      and pickup_year = {{ var('year') }}
      and pickup_month = {{ var('month') }}
    {% endif %}

),

final as (

    select
        -- Deterministic trip_id: stable across reruns/months
        lower(to_hex(md5(
          to_utf8(
            concat_ws('|',
              cast(pickup_ts as varchar),
              cast(dropoff_ts as varchar),
              cast(vendor_id as varchar),
              cast(rate_code_id as varchar),
              cast(payment_type as varchar),
              cast(pu_location_id as varchar),
              cast(do_location_id as varchar),
              cast(coalesce(passenger_count, 0) as varchar),
              cast(coalesce(trip_distance, 0.0) as varchar),
              cast(coalesce(fare_amount, 0.0) as varchar),
              cast(coalesce(tip_amount, 0.0) as varchar),
              cast(coalesce(tolls_amount, 0.0) as varchar),
              cast(coalesce(total_amount, 0.0) as varchar)
            )
          )
        ))) as trip_id,


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

)

select * from final
