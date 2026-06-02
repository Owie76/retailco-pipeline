{% snapshot snap_products %}

{{
    config(
        target_schema='snapshots',
        unique_key='product_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    product_id,
    product_name,
    sku,
    category,
    sub_category,
    brand,
    supplier,
    unit_price,
    cost_price,
    effective_from,
    created_at,
    updated_at,
    is_deleted

from {{ ref('stg_products') }}

{% endsnapshot %}