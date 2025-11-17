{{
    config(
        materialized='incremental'
    )
}}

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
    ),

    final as (
        select 
            orders.order_date,
            orders.order_id,
            orders.customer_id,
            SUM(payment.amount) as amount
        from 
            orders
        left join payment using (order_id)
        group by 1,2,3
    )

select 
    *
from final
{% if is_incremental() %}
where order_date > (select max(order_date) from {{ this }})
{% endif %}
order by order_date desc

