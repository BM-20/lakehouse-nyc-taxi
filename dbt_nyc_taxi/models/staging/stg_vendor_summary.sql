with source as (
    select * from {{ source('nyc_taxi', 'vendor_summary') }}
),

staged as (
    select
        vendor_name,
        total_trips,
        round(avg_fare, 2)             as avg_fare,
        round(avg_distance, 2)         as avg_distance,
        round(avg_duration_minutes, 2) as avg_duration_minutes,
        _created_at
    from source
    where vendor_name is not null
)

select * from staged