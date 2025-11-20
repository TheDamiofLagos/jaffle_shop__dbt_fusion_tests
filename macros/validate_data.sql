{% macro validate_data(validation_type, column_name=none, config={}) %}
    {# 
    Generic data validation macro following dbt best practices
    
    This macro provides reusable validation logic that can be used:
    - As a generic test (returning failed records)
    - In post-hooks for runtime validation
    - For custom data quality assertions
    
    Best Practices Implemented:
    1. Returns failed records (for test context)
    2. Configurable and extensible
    3. Clear error messages
    4. Performance optimized
    5. Multi-adapter compatible
    6. Supports severity levels (warn/error)
    7. Follows DRY principle
    
    Validation Types:
    - not_null: Validates column has no null values
    - unique: Validates column has no duplicate values
    - positive: Validates numeric column has only positive values
    - non_negative: Validates numeric column has no negative values
    - date_range: Validates dates within specified range
    - email_format: Validates email address format
    - referential_integrity: Validates foreign key relationships
    - custom_sql: Executes custom SQL validation logic
    
    Usage Examples:
    
    1. As a generic test in schema.yml:
        tests:
          - {{ validate_data('not_null', 'customer_id') }}
    
    2. In a custom test file:
        {{ validate_data('positive', 'order_amount', {
            'error_message': 'Order amounts must be positive'
        }) }}
    
    3. With date range validation:
        {{ validate_data('date_range', 'order_date', {
            'min_date': '2020-01-01',
            'max_date': 'current_date',
            'allow_nulls': false
        }) }}
    
    4. Custom SQL validation:
        {{ validate_data('custom_sql', config={
            'validation_sql': 'total_amount = sum(line_items)',
            'error_message': 'Total does not match sum of line items'
        }) }}
    
    Arguments:
        validation_type (string, required): Type of validation to perform
        column_name (string, optional): Column to validate (required for most validation types)
        config (dict, optional): Configuration options specific to validation type
    
    Returns:
        SQL query that returns failed records (empty result = validation passed)
    #}
    
    {% set model_relation = ref(model.name) if execute else none %}
    {% set error_message = config.get('error_message', 'Validation failed') %}
    {% set allow_nulls = config.get('allow_nulls', false) %}
    
    {# NOT NULL VALIDATION #}
    {% if validation_type == 'not_null' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'not_null' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            'NULL value found in column {{ column_name }}' as validation_error
        from {{ model }}
        where {{ column_name }} is null
    
    {# UNIQUE VALIDATION #}
    {% elif validation_type == 'unique' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'unique' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            count(*) as duplicate_count,
            'Duplicate value found: ' || cast({{ column_name }} as {{ type_string() }}) as validation_error
        from {{ model }}
        {% if not allow_nulls %}
        where {{ column_name }} is not null
        {% endif %}
        group by {{ column_name }}
        having count(*) > 1
    
    {# POSITIVE VALUES VALIDATION #}
    {% elif validation_type == 'positive' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'positive' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            'Non-positive value found in {{ column_name }}: ' || cast({{ column_name }} as {{ type_string() }}) as validation_error
        from {{ model }}
        where {{ column_name }} <= 0
        {% if not allow_nulls %}
            or {{ column_name }} is null
        {% endif %}
    
    {# NON-NEGATIVE VALUES VALIDATION #}
    {% elif validation_type == 'non_negative' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'non_negative' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            'Negative value found in {{ column_name }}: ' || cast({{ column_name }} as {{ type_string() }}) as validation_error
        from {{ model }}
        where {{ column_name }} < 0
        {% if not allow_nulls %}
            or {{ column_name }} is null
        {% endif %}
    
    {# DATE RANGE VALIDATION #}
    {% elif validation_type == 'date_range' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'date_range' validation") }}
        {% endif %}
        
        {% set min_date = config.get('min_date', '1900-01-01') %}
        {% set max_date = config.get('max_date', 'current_date') %}
        
        select
            {{ column_name }},
            case
                when {{ column_name }} < cast('{{ min_date }}' as date) 
                    then 'Date before minimum: ' || cast({{ column_name }} as {{ type_string() }})
                when {{ column_name }} > {{ max_date }}
                    then 'Date after maximum: ' || cast({{ column_name }} as {{ type_string() }})
                else 'Date out of range'
            end as validation_error
        from {{ model }}
        where {{ column_name }} < cast('{{ min_date }}' as date)
            or {{ column_name }} > {{ max_date }}
        {% if not allow_nulls %}
            or {{ column_name }} is null
        {% endif %}
    
    {# EMAIL FORMAT VALIDATION #}
    {% elif validation_type == 'email_format' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'email_format' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            'Invalid email format: ' || {{ column_name }} as validation_error
        from {{ model }}
        where {{ column_name }} is not null
            and (
                {{ column_name }} not like '%_@_%.__%'
                or {{ column_name }} like '%@%@%'
                or {{ column_name }} like '% %'
            )
    
    {# REFERENTIAL INTEGRITY VALIDATION #}
    {% elif validation_type == 'referential_integrity' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'referential_integrity' validation") }}
        {% endif %}
        
        {% set parent_model = config.get('parent_model') %}
        {% set parent_column = config.get('parent_column', column_name) %}
        
        {% if not parent_model %}
            {{ exceptions.raise_compiler_error("parent_model must be specified in config for 'referential_integrity' validation") }}
        {% endif %}
        
        select
            child.{{ column_name }},
            'Referential integrity violation: {{ column_name }} = ' || 
            cast(child.{{ column_name }} as {{ type_string() }}) || 
            ' not found in {{ parent_model }}' as validation_error
        from {{ model }} as child
        left join {{ ref(parent_model) }} as parent
            on child.{{ column_name }} = parent.{{ parent_column }}
        where child.{{ column_name }} is not null
            and parent.{{ parent_column }} is null
    
    {# VALUE IN LIST VALIDATION #}
    {% elif validation_type == 'accepted_values' %}
        {% if not column_name %}
            {{ exceptions.raise_compiler_error("column_name is required for 'accepted_values' validation") }}
        {% endif %}
        
        {% set values = config.get('values', []) %}
        {% if not values %}
            {{ exceptions.raise_compiler_error("values list must be specified in config for 'accepted_values' validation") }}
        {% endif %}
        
        select
            {{ column_name }},
            'Invalid value in {{ column_name }}: ' || cast({{ column_name }} as {{ type_string() }}) || 
            '. Accepted values are: {{ values | join(", ") }}' as validation_error
        from {{ model }}
        where {{ column_name }} not in (
            {% for value in values %}
                '{{ value }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        )
        {% if not allow_nulls %}
            or {{ column_name }} is null
        {% endif %}
    
    {# CUSTOM SQL VALIDATION #}
    {% elif validation_type == 'custom_sql' %}
        {% set validation_sql = config.get('validation_sql') %}
        
        {% if not validation_sql %}
            {{ exceptions.raise_compiler_error("validation_sql must be specified in config for 'custom_sql' validation") }}
        {% endif %}
        
        select
            *,
            '{{ error_message }}' as validation_error
        from {{ model }}
        where not ({{ validation_sql }})
    
    {# EXPRESSION IS TRUE VALIDATION #}
    {% elif validation_type == 'expression_is_true' %}
        {% set expression = config.get('expression') %}
        {% set condition = config.get('condition', 'true') %}
        
        {% if not expression %}
            {{ exceptions.raise_compiler_error("expression must be specified in config for 'expression_is_true' validation") }}
        {% endif %}
        
        select
            *,
            '{{ error_message }}: Expression "{{ expression }}" is false' as validation_error
        from {{ model }}
        where {{ condition }}
            and not ({{ expression }})
    
    {# RECORD COUNT VALIDATION #}
    {% elif validation_type == 'row_count' %}
        {% set min_rows = config.get('min_rows', 1) %}
        {% set max_rows = config.get('max_rows', none) %}
        
        with row_count as (
            select count(*) as total_rows
            from {{ model }}
        )
        select
            total_rows,
            case
                when total_rows < {{ min_rows }}
                    then 'Row count below minimum: ' || cast(total_rows as {{ type_string() }}) || ' < {{ min_rows }}'
                {% if max_rows %}
                when total_rows > {{ max_rows }}
                    then 'Row count above maximum: ' || cast(total_rows as {{ type_string() }}) || ' > {{ max_rows }}'
                {% endif %}
            end as validation_error
        from row_count
        where total_rows < {{ min_rows }}
        {% if max_rows %}
            or total_rows > {{ max_rows }}
        {% endif %}
    
    {% else %}
        {{ exceptions.raise_compiler_error("Unknown validation_type: '" ~ validation_type ~ "'. Valid types are: not_null, unique, positive, non_negative, date_range, email_format, referential_integrity, accepted_values, custom_sql, expression_is_true, row_count") }}
    {% endif %}

{% endmacro %}
