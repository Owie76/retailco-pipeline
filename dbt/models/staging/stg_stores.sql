with source as (
    select * from {{ source('raw', 'stores') }}
),

renamed as (
    select
        -- primary key
        id                          as store_id,

        -- attributes
        name                        as store_name,
        city,
        state,
        address,
        phone,
        manager_name,
        opened_date::date           as opened_date,

        -- timestamps
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at             as loaded_at

    from source
)

select * from renamed