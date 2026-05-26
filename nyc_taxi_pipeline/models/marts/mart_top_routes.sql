with trips as (
    select * from {{ ref('stg_nyc_taxi') }}
)

select
    pickup_location_id,
    dropoff_location_id,
    count(*)                              as trip_count,
    round(avg(fare_amount)::numeric, 2)   as avg_fare,
    round(avg(trip_distance)::numeric, 2) as avg_distance_km
from trips
group by pickup_location_id, dropoff_location_id
order by trip_count desc
limit 50