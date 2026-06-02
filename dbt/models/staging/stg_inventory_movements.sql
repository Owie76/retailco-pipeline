with source as (
    select * from {{ source('raw', 'inventory_movements') }}
),

renamed as (
    select
        -- primary key
        id                          as movement_id,

        -- foreign keys
        product_id,
        store_id,

        -- attributes
        movement_type,
        reference_type,
        reference_id,
        notes,

        -- numeric
        quantity::int               as quantity,

        -- timestamps
        moved_at,
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at             as loaded_at

    from source
)

select * from renamed