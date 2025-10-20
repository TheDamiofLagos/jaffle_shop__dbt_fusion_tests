{% macro log_data_quality_checks(relation, checks_config={}) %}
    {# 
    Advanced logging macro for data quality monitoring
    
    Features:
    - Multiple log levels (info, warn, error)
    - Custom data quality checks with logging
    - Performance metrics
    - Conditional logging based on environment
    - Summary statistics with formatted output
    
    Usage:
        {{ log_data_quality_checks(this, {
            'null_check_columns': ['customer_id', 'email'],
            'duplicate_check': 'customer_id',
            'date_range_check': 'created_at'
        }) }}
    #}
    
    {% set start_time = modules.datetime.datetime.now() %}
    {% set model_name = relation.identifier %}
    
    {# Only log detailed info in dev and ci, summary in prod #}
    {% set verbose_logging = target.name in ['dev', 'ci'] %}
    
    {# Header #}
    {{ log("", info=True) }}
    {{ log("╔" ~ "═" * 58 ~ "╗", info=True) }}
    {{ log("║" ~ " " * 15 ~ "DATA QUALITY CHECKS" ~ " " * 24 ~ "║", info=True) }}
    {{ log("╠" ~ "═" * 58 ~ "╣", info=True) }}
    {{ log("║ Model: " ~ model_name ~ " " * (50 - model_name|length) ~ "║", info=True) }}
    {{ log("║ Environment: " ~ target.name ~ " " * (44 - target.name|length) ~ "║", info=True) }}
    {{ log("╚" ~ "═" * 58 ~ "╝", info=True) }}
    {{ log("", info=True) }}
    
    {% set total_checks = 0 %}
    {% set passed_checks = 0 %}
    {% set warnings = [] %}
    {% set errors = [] %}
    
    {# Check 1: Total Row Count #}
    {% set row_count_query %}
        select count(*) as total_rows from {{ relation }}
    {% endset %}
    
    {% if execute %}
        {% set total_rows = run_query(row_count_query).columns[0].values()[0] %}
        {% set total_checks = total_checks + 1 %}
        
        {% if total_rows > 0 %}
            {% set passed_checks = passed_checks + 1 %}
            {{ log("✓ Row Count Check: " ~ total_rows ~ " rows", info=verbose_logging) }}
        {% else %}
            {{ log("✗ Row Count Check: FAILED - 0 rows found", info=True) }}
            {% set errors = errors + ["Empty table - 0 rows"] %}
        {% endif %}
        
        {# Check 2: Null Value Checks #}
        {% if checks_config.get('null_check_columns') %}
            {{ log("", info=verbose_logging) }}
            {{ log("Checking for NULL values in key columns...", info=verbose_logging) }}
            
            {% for column in checks_config['null_check_columns'] %}
                {% set null_check_query %}
                    select 
                        count(*) as null_count,
                        count(*) * 100.0 / nullif({{ total_rows }}, 0) as null_percentage
                    from {{ relation }}
                    where {{ column }} is null
                {% endset %}
                
                {% set null_results = run_query(null_check_query) %}
                {% set null_count = null_results.columns[0].values()[0] %}
                {% set null_pct = null_results.columns[1].values()[0] %}
                {% set total_checks = total_checks + 1 %}
                
                {% if null_count == 0 %}
                    {% set passed_checks = passed_checks + 1 %}
                    {{ log("  ✓ " ~ column ~ ": No NULLs found", info=verbose_logging) }}
                {% elif null_pct < 5 %}
                    {% set passed_checks = passed_checks + 1 %}
                    {{ log("  ⚠ " ~ column ~ ": " ~ null_count ~ " NULLs (" ~ "%.2f"|format(null_pct) ~ "%)", info=True) }}
                    {% set warnings = warnings + [column ~ " has " ~ null_count ~ " NULL values"] %}
                {% else %}
                    {{ log("  ✗ " ~ column ~ ": " ~ null_count ~ " NULLs (" ~ "%.2f"|format(null_pct) ~ "%) - CRITICAL", info=True) }}
                    {% set errors = errors + [column ~ " has excessive NULL values: " ~ "%.2f"|format(null_pct) ~ "%"] %}
                {% endif %}
            {% endfor %}
        {% endif %}
        
        {# Check 3: Duplicate Check #}
        {% if checks_config.get('duplicate_check') %}
            {% set dup_column = checks_config['duplicate_check'] %}
            {{ log("", info=verbose_logging) }}
            {{ log("Checking for duplicates on: " ~ dup_column, info=verbose_logging) }}
            
            {% set duplicate_query %}
                select count(*) as dup_count
                from (
                    select {{ dup_column }}, count(*) as cnt
                    from {{ relation }}
                    group by {{ dup_column }}
                    having count(*) > 1
                ) duplicates
            {% endset %}
            
            {% set dup_count = run_query(duplicate_query).columns[0].values()[0] %}
            {% set total_checks = total_checks + 1 %}
            
            {% if dup_count == 0 %}
                {% set passed_checks = passed_checks + 1 %}
                {{ log("  ✓ No duplicates found on " ~ dup_column, info=verbose_logging) }}
            {% else %}
                {{ log("  ✗ Found " ~ dup_count ~ " duplicate values in " ~ dup_column, info=True) }}
                {% set warnings = warnings + ["Duplicates found in " ~ dup_column] %}
            {% endif %}
        {% endif %}
        
        {# Check 4: Date Range Check #}
        {% if checks_config.get('date_range_check') %}
            {% set date_column = checks_config['date_range_check'] %}
            {{ log("", info=verbose_logging) }}
            {{ log("Checking date range for: " ~ date_column, info=verbose_logging) }}
            
            {% set date_range_query %}
                select 
                    min({{ date_column }}) as min_date,
                    max({{ date_column }}) as max_date,
                    datediff(day, min({{ date_column }}), max({{ date_column }})) as date_span_days
                from {{ relation }}
                where {{ date_column }} is not null
            {% endset %}
            
            {% set date_results = run_query(date_range_query) %}
            {% set min_date = date_results.columns[0].values()[0] %}
            {% set max_date = date_results.columns[1].values()[0] %}
            {% set date_span = date_results.columns[2].values()[0] %}
            {% set total_checks = total_checks + 1 %}
            {% set passed_checks = passed_checks + 1 %}
            
            {{ log("  ✓ Date Range: " ~ min_date ~ " to " ~ max_date, info=verbose_logging) }}
            {{ log("    Span: " ~ date_span ~ " days", info=verbose_logging) }}
            
            {# Check for future dates #}
            {% set future_date_query %}
                select count(*) as future_count
                from {{ relation }}
                where {{ date_column }} > current_date
            {% endset %}
            
            {% set future_count = run_query(future_date_query).columns[0].values()[0] %}
            
            {% if future_count > 0 %}
                {{ log("  ⚠ Warning: " ~ future_count ~ " records have future dates", info=True) }}
                {% set warnings = warnings + [future_count ~ " records with future dates"] %}
            {% endif %}
        {% endif %}
        
        {# Calculate metrics #}
        {% set end_time = modules.datetime.datetime.now() %}
        {% set duration = (end_time - start_time).total_seconds() %}
        {% set success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0 %}
        
        {# Summary Section #}
        {{ log("", info=True) }}
        {{ log("┌" ~ "─" * 58 ~ "┐", info=True) }}
        {{ log("│" ~ " " * 20 ~ "SUMMARY" ~ " " * 31 ~ "│", info=True) }}
        {{ log("├" ~ "─" * 58 ~ "┤", info=True) }}
        {{ log("│ Total Checks: " ~ total_checks ~ " " * (43 - (total_checks|string|length)) ~ "│", info=True) }}
        {{ log("│ Passed: " ~ passed_checks ~ " " * (49 - (passed_checks|string|length)) ~ "│", info=True) }}
        {{ log("│ Success Rate: " ~ "%.1f"|format(success_rate) ~ "%" ~ " " * (38) ~ "│", info=True) }}
        {{ log("│ Duration: " ~ "%.3f"|format(duration) ~ "s" ~ " " * (43 - ("%.3f"|format(duration)|length)) ~ "│", info=True) }}
        {{ log("└" ~ "─" * 58 ~ "┘", info=True) }}
        
        {# Log warnings and errors #}
        {% if warnings|length > 0 %}
            {{ log("", info=True) }}
            {{ log("⚠️  WARNINGS:", info=True) }}
            {% for warning in warnings %}
                {{ log("   • " ~ warning, info=True) }}
            {% endfor %}
        {% endif %}
        
        {% if errors|length > 0 %}
            {{ log("", info=True) }}
            {{ log("❌ ERRORS:", info=True) }}
            {% for error in errors %}
                {{ log("   • " ~ error, info=True) }}
                {{ exceptions.warn(error) }}
            {% endfor %}
        {% endif %}
        
        {# Final status message #}
        {{ log("", info=True) }}
        {% if errors|length > 0 %}
            {{ log("🔴 Data quality checks completed with ERRORS", info=True) }}
        {% elif warnings|length > 0 %}
            {{ log("🟡 Data quality checks completed with WARNINGS", info=True) }}
        {% else %}
            {{ log("🟢 All data quality checks PASSED", info=True) }}
        {% endif %}
        {{ log("", info=True) }}
        
    {% endif %}
    
{% endmacro %}
