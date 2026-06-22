-- Daily trip volume and revenue trends across Jan–Oct 2024
with daily as (
    select * from {{ ref('stg_daily_summary') }}
)

select
    pickup_date,
    total_trips,
    total_revenue,
    avg_fare,
    avg_trip_distance,
    round(total_trips - lag(total_trips) over (order by pickup_date), 0) as trips_vs_prev_day,
    round(total_revenue - lag(total_revenue) over (order by pickup_date), 2) as revenue_vs_prev_day
from daily
order by pickup_date