{{
    config(
        static_analysis='off',
        materialized='incremental',
        incremental_strategy='merge',
        merge_update_columns=['order_date', 'amount'],
        unique_key='order_id'
    )
}}

with
    {% if is_incremental() %}
    max_date as (
        select coalesce(max(order_date), '1900-01-01'::date) as max_order_date 
        from {{ this }}
    ),
    {% endif %}

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
        {% if is_incremental() %}
        cross join max_date
        where order_date > max_date.max_order_date
        {% endif %}
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
order by order_date desc
