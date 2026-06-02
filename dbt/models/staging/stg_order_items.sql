with source as (
    select * from {{ source('raw', 'order_items') }}
),

renamed as (
    select
        -- primary key
        id                              as order_item_id,

        -- foreign keys
        order_id,
        product_id,

        -- numeric
        quantity::int                   as quantity,
        unit_price::decimal(10,2)       as unit_price,
        discount_pct::decimal(5,2)      as discount_pct,
        line_total::decimal(10,2)       as line_total,

        -- timestamps
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at                 as loaded_at

    from source
)

select * from renamed