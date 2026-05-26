with trips as (
    select * from {{ ref('stg_nyc_taxi') }}
)

select
    pickup_hour,
    pickup_day_of_week,
    count(*)                     as trip_count,
    round(avg(trip_duration_min)::numeric, 2) as avg_duration_min,
    round(avg(fare_amount)::numeric, 2)       as avg_fare,
    round(sum(total_amount)::numeric, 2)      as total_revenue
from trips
group by pickup_hour, pickup_day_of_week
order by pickup_hour