{{ config(
    materialized='incremental',
    unique_key='trip_id'
) }}

with source as (
    select
        row_number() over (order by pickup_at) as trip_id,
        pickup_at,
        pickup_location_id,
        dropoff_location_id,
        trip_distance,
        total_amount,
        pickup_hour
    from {{ ref('stg_nyc_taxi') }}
)

select * from source

{% if is_incremental() %}
    where pickup_at > (select max(pickup_at) from {{ this }})
{% endif %}