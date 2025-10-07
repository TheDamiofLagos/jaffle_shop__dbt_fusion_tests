{{
    config(
        tags=['stripe', 'staging']
    )
}}

select
    amount/100 as amount,
    created,
    id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    _batched_at
from 
    {{ source('stripe', 'payment') }}