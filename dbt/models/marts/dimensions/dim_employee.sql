with employees as (
    select * from {{ ref('stg_employees') }}
),

stores as (
    select store_key, store_id from {{ ref('dim_store') }}
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['e.employee_id']) }} as employee_key,

        -- natural key
        e.employee_id,

        -- foreign key
        s.store_key,

        -- attributes
        e.first_name,
        e.last_name,
        e.full_name,
        e.email,
        e.role,
        e.hired_date,

        -- soft delete
        e.is_deleted,

        -- timestamps
        e.created_at,
        e.updated_at

    from employees e
    left join stores s on e.store_id = s.store_id
)

select * from final