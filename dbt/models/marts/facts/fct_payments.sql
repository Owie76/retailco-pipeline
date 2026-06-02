with payments as (
    select * from {{ ref('stg_payments') }}
    -- exclude anomalous payments
    where is_anomalous = false
),

orders as (
    select order_id, store_id
    from {{ ref('stg_orders') }}
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

dim_payment_method as (
    select payment_method_key, payment_method_id
    from {{ ref('dim_payment_method') }}
),

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

joined as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'p.payment_id'
        ]) }}                               as payment_key,

        -- natural keys
        p.payment_id,
        p.order_id,

        -- dimension foreign keys
        dd.date_key,
        dc.customer_key,
        ds.store_key,
        dpm.payment_method_key,

        -- measures
        p.amount_paid,
        p.is_refund,

        -- attributes
        p.status,
        p.payment_type,
        p.currency,
        p.reference

    from payments p
    left join orders o
        on p.order_id = o.order_id
    left join dim_customer dc
        on p.customer_id = dc.customer_id
    left join dim_store ds
        on o.store_id = ds.store_id
    left join dim_payment_method dpm
        on p.payment_method_id = dpm.payment_method_id
    left join dim_date dd
        on p.paid_at::date = dd.full_date
)

select * from joined