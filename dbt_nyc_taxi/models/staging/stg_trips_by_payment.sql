with source as (
    select * from {{ source('nyc_taxi', 'trips_by_payment') }}
),

staged as (
    select
        payment_type,
        total_trips,
        round(total_revenue, 2) as total_revenue,
        round(avg_tip, 2)       as avg_tip,
        _created_at
    from source
    where payment_type is not null
)

select * from staged