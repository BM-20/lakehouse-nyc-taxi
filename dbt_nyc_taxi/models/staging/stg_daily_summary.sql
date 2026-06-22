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
    -- Keep only the loaded window (Jan–Oct 2024); this also drops the handful
    -- of source rows with corrupt meter timestamps (2002/2009/2026, etc.)
    where pickup_date between '2024-01-01' and '2024-10-31'
)

select * from staged