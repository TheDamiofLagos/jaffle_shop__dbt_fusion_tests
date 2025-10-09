{% macro cents_to_dollars(amount_col, decimals = 2) %}
    ROUND({{ amount_col }}/100, {{ decimals }})
{% endmacro %}