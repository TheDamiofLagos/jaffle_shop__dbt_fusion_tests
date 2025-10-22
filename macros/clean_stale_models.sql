{% macro clean_stale_models(database=target.database, schema=target.schema) %}
    {{ log("Starting Operation: Clean Stale Models", info=True)}}
    {% set query %}
        select
            table_schema,
            table_name,
            last_altered
        from {{database}}.information_schema.tables
        where table_schema = upper('{{schema}}')
    {% endset %}
{% endmacro %}