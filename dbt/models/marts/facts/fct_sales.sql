with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

dim_customer as (
    select customer_key, customer_id
    from {{ ref('dim_customer') }}
    where is_current = true
),

dim_product as (
    select product_key, product_id
    from {{ ref('dim_product') }}
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
            'oi.order_item_id'
        ]) }}                               as sales_key,

        -- natural keys for traceability
        oi.order_item_id,
        oi.order_id,

        -- dimension foreign keys
        dd.date_key,
        dc.customer_key,
        dp.product_key,
        ds.store_key,
        de.employee_key,

        -- measures
        oi.quantity,
        oi.unit_price,
        oi.discount_pct,
        oi.line_total                       as gross_amount,
        oi.line_total * (1 - coalesce(oi.discount_pct, 0) / 100)
                                            as net_amount,
        oi.line_total - (oi.unit_price * oi.quantity)
                                            as discount_amount

    from order_items oi
    inner join orders o
        on oi.order_id = o.order_id
    left join dim_customer dc
        on o.customer_id = dc.customer_id
    left join dim_product dp
        on oi.product_id = dp.product_id
    left join dim_store ds
        on o.store_id = ds.store_id
    left join dim_employee de
        on o.employee_id = de.employee_id
    left join dim_date dd
        on o.ordered_at::date = dd.full_date
)

select * from joined