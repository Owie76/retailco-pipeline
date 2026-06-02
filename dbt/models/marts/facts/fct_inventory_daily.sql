with movements as (
    select * from {{ ref('stg_inventory_movements') }}
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

dim_date as (
    select date_key, full_date
    from {{ ref('dim_date') }}
),

-- aggregate movements to daily level
daily_movements as (
    select
        m.product_id,
        m.store_id,
        m.moved_at::date                    as movement_date,

        sum(case
            when m.movement_type = 'in'
            then m.quantity else 0
        end)                                as stock_in,

        sum(case
            when m.movement_type = 'out'
            then m.quantity else 0
        end)                                as stock_out

    from movements m
    group by
        m.product_id,
        m.store_id,
        m.moved_at::date
),

joined as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'dm.product_id',
            'dm.store_id',
            'dm.movement_date'
        ]) }}                               as inventory_key,

        -- dimension foreign keys
        dd.date_key,
        dp.product_key,
        ds.store_key,

        -- measures
        dm.stock_in,
        dm.stock_out,
        dm.stock_in - dm.stock_out          as net_movement

    from daily_movements dm
    left join dim_product dp
        on dm.product_id = dp.product_id
    left join dim_store ds
        on dm.store_id = ds.store_id
    left join dim_date dd
        on dm.movement_date = dd.full_date
)

select * from joined