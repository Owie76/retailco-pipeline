with source as (
    select * from {{ source('raw', 'products') }}
),

renamed as (
    select
        -- primary key
        id                              as product_id,

        -- attributes
        name                            as product_name,
        sku,
        category,
        sub_category,
        brand,
        supplier,

        -- numeric
        selling_price::decimal(10,2)    as unit_price,
        cost_price::decimal(10,2)       as cost_price,

        -- timestamps
        effective_from,
        created_at,
        updated_at,

        -- soft delete
        is_deleted,

        -- metadata
        _lake_loaded_at                 as loaded_at

    from source
)

select * from renamed