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
