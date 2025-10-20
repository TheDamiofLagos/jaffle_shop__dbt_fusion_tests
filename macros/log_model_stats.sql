{% macro log_model_stats(relation, row_threshold=1000) %}
    {# 
    Semi-advanced macro that logs model execution statistics
    
    Features:
    - Logs row count and size information
    - Warns if row count is below threshold
    - Logs execution context (target, schema)
    - Uses different log levels appropriately
    #}
    
    {% set start_time = modules.datetime.datetime.now() %}
    
    {# Log execution context #}
    {{ log("=" * 60, info=True) }}
    {{ log("MODEL STATISTICS LOGGER", info=True) }}
    {{ log("Model: " ~ relation, info=True) }}
    {{ log("Target: " ~ target.name, info=True) }}
    {{ log("Schema: " ~ relation.schema, info=True) }}
    {{ log("=" * 60, info=True) }}
    
    {# Get row count #}
    {% set row_count_query %}
        select count(*) as row_count
        from {{ relation }}
    {% endset %}
    
    {% set results = run_query(row_count_query) %}
    
    {% if execute %}
        {% set row_count = results.columns[0].values()[0] %}
        
        {# Log row count with appropriate level #}
        {% if row_count == 0 %}
            {{ log("⚠️  WARNING: Model has 0 rows!", info=True) }}
            {{ exceptions.warn("Model " ~ relation ~ " is empty") }}
        {% elif row_count < row_threshold %}
            {{ log("⚠️  WARNING: Low row count detected!", info=True) }}
            {{ log("   Rows: " ~ row_count ~ " (threshold: " ~ row_threshold ~ ")", info=True) }}
        {% else %}
            {{ log("✓ Row count: " ~ row_count | string | replace(',', '_'), info=True) }}
        {% endif %}
        
        {# Get column count #}
        {% set column_count_query %}
            select count(*) as col_count
            from {{ adapter.get_columns_in_relation(relation) | length }}
        {% endset %}
        
        {% set col_count = adapter.get_columns_in_relation(relation) | length %}
        {{ log("✓ Column count: " ~ col_count, info=True) }}
        
        {# Calculate estimated size (rough estimate) #}
        {% set estimated_size_mb = (row_count * col_count * 100) / 1024 / 1024 %}
        {{ log("✓ Estimated size: ~" ~ "%.2f"|format(estimated_size_mb) ~ " MB", info=True) }}
        
        {# Log column names #}
        {{ log("", info=True) }}
        {{ log("Columns in model:", info=True) }}
        {% for column in adapter.get_columns_in_relation(relation) %}
            {{ log("  - " ~ column.name ~ " (" ~ column.dtype ~ ")", info=True) }}
        {% endfor %}
        
        {# Calculate execution time #}
        {% set end_time = modules.datetime.datetime.now() %}
        {% set duration = (end_time - start_time).total_seconds() %}
        
        {{ log("", info=True) }}
        {{ log("Analysis completed in " ~ "%.3f"|format(duration) ~ " seconds", info=True) }}
        {{ log("=" * 60, info=True) }}
        
    {% endif %}
    
{% endmacro %}
