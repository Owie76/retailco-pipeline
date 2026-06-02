with source as (
    select * from {{ source('raw', 'payments') }}
),

renamed as (
    select
        -- primary key
        id                              as payment_id,

        -- foreign keys
        order_id,
        customer_id,
        payment_method_id,

        -- attributes
        status,
        payment_type,
        currency,
        reference,

        -- numeric
        amount_paid::decimal(10,2)      as amount_paid,

        -- refund flag
        case
            when amount_paid::decimal(10,2) < 0
            and status = 'refunded'
            then true
            else false
        end                             as is_refund,

        -- anomaly flag
        case
            when amount_paid::decimal(10,2) = 0
            then true
            when amount_paid::decimal(10,2) < 0
            and status != 'refunded'
            then true
            else false
        end                             as is_anomalous,

        -- timestamps
        paid_at,
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at                 as loaded_at

    from source
)

select * from renamed