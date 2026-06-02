with source as (
    select * from {{ ref('stg_payment_methods') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['payment_method_id']) }} as payment_method_key,

        -- natural key
        payment_method_id,

        -- attributes
        method_name,
        provider,
        is_digital,

        -- timestamps
        created_at,
        updated_at

    from source
)

select * from final