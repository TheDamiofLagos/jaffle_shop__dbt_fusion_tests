with
    payment as (
        select
            order_id,
            amount,
            status as payment_status
        from
            {{ ref('stg_stripe__payments') }}
        where status = 'success'
    ),

    orders as (
        select
            order_id,
            customer_id,
            order_date,
            status
        from
            {{ ref('stg_jaffle_shop__orders') }}
        {% if is_dev() %}
        limit 100
        {% endif %}
    )

select 
    orders.order_id,
    orders.customer_id,
    SUM(payment.amount) as amount
from 
    orders
left join payment using (order_id)
group by 1,2