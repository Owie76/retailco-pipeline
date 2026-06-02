with orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_store as (
    select store_key, store_id
    from {{ ref('dim_store') }}
),

dim_employee as (
    select employee_key, employee_id
    from {{ ref('dim_employee') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

joined as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'o.order_id'
        ]) }}                               as lifecycle_key,

        -- natural key
        o.order_id,

        -- dimension foreign keys
        dc.customer_key,
        ds.store_key,
        de.employee_key,

        -- multiple date foreign keys
        dd_ordered.date_key                 as ordered_date_key,
        dd_paid.date_key                    as paid_date_key,
        dd_shipped.date_key                 as shipped_date_key,
        dd_delivered.date_key               as delivered_date_key,

        -- current status
        o.status                            as current_status,

        -- measures
        o.total_amount,
        o.discount_amount,

        -- calculated days between milestones
        case
            when o.paid_at is not null and o.ordered_at is not null
            then extract(day from o.paid_at - o.ordered_at)::int
        end                                 as days_to_pay,

        case
            when o.shipped_at is not null and o.paid_at is not null
            then extract(day from o.shipped_at - o.paid_at)::int
        end                                 as days_to_ship,

        case
            when o.delivered_at is not null and o.shipped_at is not null
            then extract(day from o.delivered_at - o.shipped_at)::int
        end                                 as days_to_deliver,

        -- timestamps
        o.ordered_at,
        o.paid_at,
        o.shipped_at,
        o.delivered_at

    from orders o
    left join dim_customer dc
        on o.customer_id = dc.customer_id
    left join dim_store ds
        on o.store_id = ds.store_id
    left join dim_employee de
        on o.employee_id = de.employee_id
    left join dim_date dd_ordered
        on o.ordered_at::date = dd_ordered.full_date
    left join dim_date dd_paid
        on o.paid_at::date = dd_paid.full_date
    left join dim_date dd_shipped
        on o.shipped_at::date = dd_shipped.full_date
    left join dim_date dd_delivered
        on o.delivered_at::date = dd_delivered.full_date
)

select * from joined