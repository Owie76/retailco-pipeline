{% snapshot snap_customers %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    customer_id,
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
    effective_from,
    registered_at,
    created_at,
    updated_at,
    is_deleted

from {{ ref('stg_customers') }}

{% endsnapshot %}