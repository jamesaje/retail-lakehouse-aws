{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('nyc_taxi', 'cleaned_yellow') }}

),

renamed as (

    select
        cast(vendorid as integer) as vendor_id,
        cast(passenger_count as integer) as passenger_count,
        cast(ratecodeid as integer) as rate_code_id,
        cast(pulocationid as integer) as pu_location_id,
        cast(dolocationid as integer) as do_location_id,
        cast(payment_type as integer) as payment_type,

        cast(tpep_pickup_datetime as timestamp) as pickup_ts,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,

        cast(trip_distance as double) as trip_distance,
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee,
        cast(total_amount as double) as total_amount,

        cast(pickup_year as integer) as pickup_year,
        cast(pickup_month as integer) as pickup_month

    from source

)

select * from renamed
