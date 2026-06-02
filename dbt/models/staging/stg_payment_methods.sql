with source as (
    select * from {{ source('raw', 'payment_methods') }}
),

renamed as (
    select
        -- primary key
        id                          as payment_method_id,

        -- attributes
        name                        as method_name,
        provider,
        is_digital,

        -- timestamps
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at             as loaded_at

    from source
)

select * from renamed