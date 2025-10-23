{% macro clean_stale_models(database=target.database, schema=target.schema, days=7, dry_run=True) %}
    {{ log("Starting Operation: Clean Stale Models", info=True)}}
    {% set get_drop_commands_query %}
        select
            case 
                when table_type = 'VIEW'
                    then table_type
                else 
                    'TABLE'
            end as drop_type, 
            'DROP ' || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';'
        from {{ database }}.information_schema.tables 
        where table_schema = upper('{{ schema }}')
        and last_altered <= current_date - {{ days }} 
    {% endset %}

    {{ log('\nGeneratig cleanup Queries...\n', info=True) }}
    {% set drop_queries = run_query(get_drop_commands_query).columns[1].values() %}

    {{ log('\nDrop Queries Generated \n') }}
    {% for query in drop_queries %}
        {% if dry_run %}
            {{ log('[DRY RUN] ' ~ query, info=True) }}
        {% else %}
            {{ log('Droppinng object with command: ' ~ query, info=True) }}
            {% do run_query(query) %}
        {% enndif %}
    {% endfor %}
{% endmacro %}