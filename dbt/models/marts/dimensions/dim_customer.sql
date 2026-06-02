with snapshot as (
    select * from {{ ref('snap_customers') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'dbt_valid_from']) }} as customer_key,

        -- natural key
        customer_id,

        -- attributes
        first_name,
        last_name,
        full_name,
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