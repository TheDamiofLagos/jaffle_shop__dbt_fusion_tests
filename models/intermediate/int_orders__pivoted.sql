{% set payment_method = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with 
    payments as (
        select 
            order_id,
            amount,
            payment_method
        from {{ ref('stg_stripe__payments') }}
        where status = 'success'
    ),

    pivoted as (
        select
            order_id,
            {% for method in payment_method %}
                sum(case when payment_method = '{{ method }}' then amount else 0 end) as payment_method__{{ method }} {% if not loop.last%},{% endif %}
            {% endfor %}
        from payments
        group by order_id
    )

select * from pivoted