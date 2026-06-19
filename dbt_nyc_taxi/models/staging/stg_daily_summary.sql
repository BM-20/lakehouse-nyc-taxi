with source as (
    select * from {{ source('nyc_taxi', 'daily_summary') }}
),

staged as (
    select
        pickup_date,
        total_trips,
        round(total_revenue, 2)     as total_revenue,
        round(avg_fare, 2)          as avg_fare,
        round(avg_trip_distance, 2) as avg_trip_distance,
        _created_at
    from source
    -- Filter out anomalous dates outside January 2024
    where pickup_date between '2024-01-01' and '2024-01-31'
)

select * from staged