with payments as (
    select * from {{ ref('stg_payments') }}
    where is_anomalous = true
),

final as (
    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key([
            'payment_id'
        ]) }}                               as flagged_payment_key,

        -- natural keys
        payment_id,
        order_id,
        customer_id,
        payment_method_id,

        -- measures
        amount_paid,

        -- flag reason
        case
            when amount_paid = 0
            then 'Zero amount payment'
            when amount_paid < 0 and status != 'refunded'
            then 'Unexplained negative amount'
            else 'Unknown anomaly'
        end                                 as flag_reason,

        -- attributes
        status,
        payment_type,
        currency,

        -- timestamps
        paid_at,
        created_at,
        updated_at,

        -- metadata
        current_timestamp                   as flagged_at

    from payments
)

select * from final