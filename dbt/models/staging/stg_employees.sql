with source as (
    select * from {{ source('raw', 'employees') }}
),

renamed as (
    select
        -- primary key
        id                          as employee_id,

        -- foreign key
        store_id,

        -- attributes
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        role,
        hired_date::date            as hired_date,

        -- soft delete
        is_deleted,

        -- timestamps
        created_at,
        updated_at,

        -- metadata
        _lake_loaded_at             as loaded_at

    from source
)

select * from renamed