with source as (
    select * from {{ source('nyc_taxi', 'trips_by_hour') }}
),

staged as (
    select
        pickup_hour,
        total_trips,
        round(avg_fare, 2)          as avg_fare,
        round(avg_trip_distance, 2) as avg_trip_distance,
        _created_at
    from source
    where pickup_hour between 0 and 23
)

select * from staged