{% docs customers %}
Customer-level aggregation combining customer information with their order history. Provides a comprehensive view of each customer including their first and most recent order dates, as well as total order count.
{% enddocs %}

{% docs stg_jaffle_shop__customers %}
Cleaned and standardized customer data with renamed columns for consistency.
{% enddocs %}

{% docs stg_jaffle_shop__orders %}
Cleaned and standardized order data with renamed columns for consistency.
{% enddocs %}

{% docs customer_id %}
Unique identifier for each customer
{% enddocs %}

{% docs first_name %}
Customer's first name
{% enddocs %}

{% docs last_name %}
Customer's last name
{% enddocs %}

{% docs order_id %}
Unique identifier for each order
{% enddocs %}

{% docs order_date %}
Date when the order was placed
{% enddocs %}

{% docs status %}
Current status of the order (e.g., completed, shipped, returned)
{% enddocs %}

{% docs first_order_date %}
Date of the customer's first order
{% enddocs %}

{% docs most_recent_order_date %}
Date of the customer's most recent order
{% enddocs %}

{% docs number_of_orders %}
Total count of orders placed by the customer (defaults to 0 for customers with no orders)
{% enddocs %}

{% docs stg_stripe__payments %}
Cleaned and standardized payment data with renamed columns and amount converted from cents to dollars.
{% enddocs %}

{% docs payment_id %}
Unique identifier for each payment
{% enddocs %}

{% docs amount %}
Payment amount in dollars (converted from cents)
{% enddocs %}

{% docs created %}
Timestamp when the payment was created
{% enddocs %}

{% docs payment_method %}
Method used for the payment (e.g., credit_card, bank_transfer)
{% enddocs %}

{% docs payment_status %}
Current status of the payment (e.g., success, pending, failed)
{% enddocs %}

{% docs batched_at %}
Timestamp when the record was batched into the data warehouse
{% enddocs %}

{% docs lifetime_value %}
Total dollar amount spent by the customer across all orders (defaults to 0 for customers with no orders)
{% enddocs %}

{% docs dim_customers %}
Dimensional model providing a comprehensive view of each customer including their first and most recent order dates, total order count, and lifetime value.
{% enddocs %}

{% docs fct_orders %}
Fact table containing order-level transactions with associated payment amounts.
{% enddocs %}
