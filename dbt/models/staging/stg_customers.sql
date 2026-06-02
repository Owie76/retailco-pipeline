with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        -- primary key
        id                          as customer_id,

        -- attributes
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        phone,
        address,
        city,
        state,
        segment,
        tier,

        -- timestamps
        effective_from,
        registered_at,
        created_at,
        updated_at,

        -- soft delete
        is_deleted,

        -- metadata
        _lake_loaded_at             as loaded_at

    from source
)

select * from renamed