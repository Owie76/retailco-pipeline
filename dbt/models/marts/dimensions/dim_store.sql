with source as (
    select * from {{ ref('stg_stores') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['store_id']) }} as store_key,

        -- natural key
        store_id,

        -- attributes
        store_name,
        city,
        state,
        address,
        phone,
        manager_name,
        opened_date,

        -- timestamps
        created_at,
        updated_at

    from source
)

select * from final