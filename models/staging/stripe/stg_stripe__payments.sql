{{
    config(
        tags=['stripe', 'staging']
    )
}}

select
    {{ cents_to_dollars("amount")}} as amount,
    created,
    id,
    orderid as order_id,
    paymentmethod as payment_method,
    status,
    _batched_at
from 
    {{ source('stripe', 'payment') }}