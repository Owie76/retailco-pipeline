with snapshot as (
    select * from {{ ref('snap_products') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['product_id', 'dbt_valid_from']) }} as product_key,

        -- natural key
        product_id,

        -- attributes
        product_name,
        sku,
        category,
        sub_category,
        brand,
        supplier,
        unit_price,
        cost_price,

        -- calculated
        unit_price - cost_price as gross_margin,

        -- timestamps
        effective_from,
        created_at,
        updated_at,

        -- SCD2 columns
        dbt_valid_from          as valid_from,
        dbt_valid_to            as valid_to,
        case
            when dbt_valid_to is null then true
            else false
        end                     as is_current,

        -- soft delete
        is_deleted

    from snapshot
)

select * from final