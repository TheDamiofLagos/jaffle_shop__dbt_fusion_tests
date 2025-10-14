{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    
    {%- if target.name == 'prod' and custom_schema_name is not none -%}
        {# In prod, use only the custom schema name (stg, int, mrt) #}
        {{ custom_schema_name | trim }}
        
    {%- else -%}
        {# In dev/ci, use the target schema (everything in one schema) #}
        {{ default_schema }}
        
    {%- endif -%}

{%- endmacro %}
