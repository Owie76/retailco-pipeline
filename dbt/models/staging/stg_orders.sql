with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        -- primary key
        id                              as order_id,

        -- foreign keys
        customer_id,
        store_id,
        employee_id,

        -- attributes
        status,
        discount_code,

        -- numeric
        total_amount::decimal(10,2)     as total_amount,
        discount_amount::decimal(10,2)  as discount_amount,

        -- timestamps
        ordered_at,
        paid_at,
        shipped_at,
        delivered_at,
        cancelled_at,
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at                 as loaded_at

    from source
    where status != 'cancelled'
)

select * from renamed