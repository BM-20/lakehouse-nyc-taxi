-- Identifies the busiest hours of the day for NYC taxi demand
with trips as (
    select * from {{ ref('stg_trips_by_hour') }}
)

select
    pickup_hour,
    total_trips,
    avg_fare,
    avg_trip_distance,
    round(total_trips * 100.0 / sum(total_trips) over (), 2) as pct_of_daily_trips,
    case
        when pickup_hour between 7 and 9 then 'Morning Rush'
        when pickup_hour between 11 and 14 then 'Lunch'
        when pickup_hour between 17 and 19 then 'Evening Rush'
        when pickup_hour between 20 and 23 then 'Late Night'
        else 'Off Peak'
    end as time_period
from trips
order by total_trips desc