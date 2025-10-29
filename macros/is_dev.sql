{%  macro is_dev() %}
    {%- if target.name not in var('dbt_full_refresh_targets') -%}
        {{ return(true) }}
    {%- else -%}
        {{ return(false) }}
    {%- endif -%}
{% endmacro % %}