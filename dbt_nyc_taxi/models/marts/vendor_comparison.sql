-- Side by side comparison of the two NYC taxi vendors
with vendors as (
    select * from {{ ref('stg_vendor_summary') }}
)

select
    vendor_name,
    total_trips,
    avg_fare,
    avg_distance,
    avg_duration_minutes,
    round(avg_fare / nullif(avg_distance, 0), 2) as fare_per_mile,
    round(total_trips * 100.0 / sum(total_trips) over (), 2) as market_share_pct
from vendors
order by total_trips desc