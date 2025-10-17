{% macro grant_select(database=target.database, schema=target.schema, role=target.role) %}
    {% set sql %}
        USE DATABASE {{ target.database }};
        GRANT USAGE ON SCHEMA {{ schema }} TO {{ role }};
        GRANT SELECT ON ALL TABLES IN SCHEMA {{ schema }} TO {{ role }};
        GRANT SELECT ON ALL VIEWS IN SCHEMA {{ schema }} TO {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and view in schema ' ~ target.schema  ~ 'to role ' ~ target.role, info=true) }}
    {% do run_query(sql) %}
    {{ log('Grant complete', info=true) }}
{% endmacro %}