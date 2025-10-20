{{
    config(
        tags=['jaffle_shop', 'marts', 'dimensional'],
        post_hook=[
            "{{ log_model_stats(this, row_threshold=50) }}",
            "{{ log_data_quality_checks(this, {
                'null_check_columns': ['customer_id', 'first_name', 'last_name'],
                'duplicate_check': 'customer_id'
            }) }}"
        ]
    )
}}

with customers as (

    select
        customer_id,
        first_name,
        last_name
    from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select
        order_id,
        customer_id,
        order_date,
        status,
        amount

    from {{ ref('stg_jaffle_shop__orders') }}
    left join {{ ref('stg_stripe__payments') }} using (order_id)

),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value

    from orders

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        coalesce(customer_orders.lifetime_value, 0) as lifetime_value

    from customers

    left join customer_orders using (customer_id)



)

select * from final
