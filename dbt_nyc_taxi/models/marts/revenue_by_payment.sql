-- Revenue and tip analysis broken down by payment method
with payments as (
    select * from {{ ref('stg_trips_by_payment') }}
)

select
    payment_type,
    total_trips,
    total_revenue,
    avg_tip,
    round(total_revenue * 100.0 / sum(total_revenue) over (), 2) as pct_of_total_revenue,
    round(total_trips * 100.0 / sum(total_trips) over (), 2) as pct_of_total_trips
from payments
order by total_revenue desc