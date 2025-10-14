{{
    config(
        materialized = 'table',
        static_analysis = 'unsafe'
    )
}}

with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2018-01-01' as date)",
        end_date="cast('2018-04-01' as date)"
        )
    }}
),

customer_orders as (
    select
        customer_id,
        order_date,
        count(order_id) as daily_order_count
    from {{ ref('stg_jaffle_shop__orders') }}
    group by all
)

select
    {{ dbt_utils.generate_surrogate_key(
        ['customer_orders.customer_id', 'date_spine.date_day']
    )}} AS pkey,
    date_spine.date_day as order_date,
    customer_orders.customer_id,
    coalesce(customer_orders.daily_order_count, 0) as daily_order_count
from date_spine
left join customer_orders
on date_spine.date_day = customer_orders.order_date
