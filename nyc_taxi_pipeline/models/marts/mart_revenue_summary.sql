with trips as (
    select * from {{ ref('stg_nyc_taxi') }}
)

select
    date(pickup_at)                           as trip_date,
    count(*)                                  as total_trips,
    round(sum(total_amount)::numeric, 2)      as total_revenue,
    round(avg(total_amount)::numeric, 2)      as avg_revenue_per_trip,
    round(sum(tip_amount)::numeric, 2)        as total_tips,
    round(avg(trip_distance)::numeric, 2)     as avg_distance
from trips
group by date(pickup_at)
order by trip_date