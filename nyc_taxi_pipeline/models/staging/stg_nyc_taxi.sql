with source as (
    select * from {{ source('raw', 'nyc_taxi') }}
),

renamed as (
    select
        -- identifiers
        "VendorID"              as vendor_id,
        "PULocationID"          as pickup_location_id,
        "DOLocationID"          as dropoff_location_id,
        payment_type,

        -- timestamps
        tpep_pickup_datetime    as pickup_at,
        tpep_dropoff_datetime   as dropoff_at,

        -- trip details
        passenger_count,
        trip_distance,
        trip_duration_min,
        pickup_hour,
        pickup_day_of_week,

        -- financials
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        fare_per_km

    from source
    where trip_distance > 0
      and fare_amount   > 0
      and passenger_count > 0
)

select * from renamed