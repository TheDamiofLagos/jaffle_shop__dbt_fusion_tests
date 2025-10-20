# Advanced Logging Macros Documentation

This document explains the semi-advanced logging macros available in this project and how to use them effectively.

## Overview

We have created two powerful logging macros that demonstrate advanced dbt logging capabilities:

1. **log_model_stats** - Logs model execution statistics
2. **log_data_quality_checks** - Performs and logs data quality checks

---

## 1. log_model_stats

### Description
Analyzes and logs comprehensive statistics about a dbt model, including row counts, column information, estimated size, and execution time.

### Features
- ✅ Row count with threshold warnings
- ✅ Column count and detailed column information
- ✅ Estimated data size calculation
- ✅ Performance timing
- ✅ Environment-aware logging
- ✅ Warning system for empty tables

### Syntax
```sql
{{ log_model_stats(relation, row_threshold=1000) }}
```

### Parameters
- `relation` (required): The dbt relation/model to analyze
- `row_threshold` (optional): Minimum expected row count. Default: 1000

### Usage Examples

#### Example 1: Basic Usage in a Post-Hook
```sql
-- models/marts/dim_customers.sql
{{ config(
    materialized='table',
    post_hook=[
        "{{ log_model_stats(this) }}"
    ]
) }}

select
    customer_id,
    first_name,
    last_name,
    email
from {{ ref('stg_jaffle_shop__customers') }}
```

#### Example 2: With Custom Threshold
```sql
{{ config(
    post_hook=[
        "{{ log_model_stats(this, row_threshold=5000) }}"
    ]
) }}
```

#### Example 3: In an Operation
```sql
-- macros/operations/check_model_stats.sql
{% macro check_model_stats() %}
    {% set relation = ref('dim_customers') %}
    {% set stats = log_model_stats(relation, row_threshold=100) %}
    
    {% if stats.row_count < 100 %}
        {{ exceptions.raise_compiler_error("Model has too few rows!") }}
    {% endif %}
{% endmacro %}

-- Run with: dbt run-operation check_model_stats
```

### Sample Output
```
============================================================
MODEL STATISTICS LOGGER
Model: analytics.dev.dim_customers
Target: dev
Schema: dev
============================================================
✓ Row count: 2_145
✓ Column count: 8
✓ Estimated size: ~0.16 MB

Columns in model:
  - customer_id (NUMBER)
  - first_name (VARCHAR)
  - last_name (VARCHAR)
  - email (VARCHAR)
  - first_order_date (DATE)
  - most_recent_order_date (DATE)
  - number_of_orders (NUMBER)
  - total_order_amount (NUMBER)

Analysis completed in 0.234 seconds
============================================================
```

---

## 2. log_data_quality_checks

### Description
Performs comprehensive data quality checks with detailed logging, warnings, and error reporting. Automatically adapts logging verbosity based on environment.

### Features
- ✅ NULL value detection with percentage thresholds
- ✅ Duplicate detection
- ✅ Date range validation
- ✅ Future date detection
- ✅ Success rate calculation
- ✅ Environment-specific logging (verbose in dev/ci, summary in prod)
- ✅ Formatted output with emojis and box drawing
- ✅ Warning and error aggregation

### Syntax
```sql
{{ log_data_quality_checks(relation, checks_config) }}
```

### Parameters
- `relation` (required): The dbt relation object (use `this` in post-hooks or `ref('model_name')` in operations)
- `checks_config` (optional): Dictionary of checks to perform

### Configuration Options
```python
{
    'null_check_columns': ['col1', 'col2'],  # Check for NULLs in these columns
    'duplicate_check': 'id_column',           # Check for duplicates on this column
    'date_range_check': 'date_column'         # Validate date ranges
}
```

### Usage Examples

#### Example 1: Full Data Quality Check
```sql
-- models/marts/dim_customers.sql
{{ config(
    materialized='table',
    post_hook=[
        "{{ log_data_quality_checks('dim_customers', {
            'null_check_columns': ['customer_id', 'email'],
            'duplicate_check': 'customer_id'
        }) }}"
    ]
) }}

select * from {{ ref('stg_jaffle_shop__customers') }}
```

#### Example 2: With Date Range Check
```sql
{{ config(
    post_hook=[
        "{{ log_data_quality_checks('fct_orders', {
            'null_check_columns': ['order_id', 'customer_id'],
            'duplicate_check': 'order_id',
            'date_range_check': 'order_date'
        }) }}"
    ]
) }}
```

#### Example 3: In an Operation for Multiple Models
```sql
-- macros/operations/validate_all_marts.sql
{% macro validate_all_marts() %}
    {% set models = ['dim_customers', 'fct_orders'] %}
    
    {% for model in models %}
        {{ log("Validating " ~ model, info=True) }}
        {% set result = log_data_quality_checks(model, {
            'null_check_columns': ['id'],
            'duplicate_check': 'id'
        }) %}
    {% endfor %}
{% endmacro %}

-- Run with: dbt run-operation validate_all_marts
```

### Sample Output

#### Success Case
```
╔══════════════════════════════════════════════════════════╗
║               DATA QUALITY CHECKS                        ║
╠══════════════════════════════════════════════════════════╣
║ Model: dim_customers                                     ║
║ Environment: dev                                         ║
╚══════════════════════════════════════════════════════════╝

✓ Row Count Check: 2145 rows

Checking for NULL values in key columns...
  ✓ customer_id: No NULLs found
  ✓ email: No NULLs found

Checking for duplicates on: customer_id
  ✓ No duplicates found on customer_id

┌──────────────────────────────────────────────────────────┐
│                    SUMMARY                               │
├──────────────────────────────────────────────────────────┤
│ Total Checks: 4                                          │
│ Passed: 4                                                │
│ Success Rate: 100.0%                                     │
│ Duration: 0.456s                                         │
└──────────────────────────────────────────────────────────┘

🟢 All data quality checks PASSED
```

#### Warning Case
```
╔══════════════════════════════════════════════════════════╗
║               DATA QUALITY CHECKS                        ║
╠══════════════════════════════════════════════════════════╣
║ Model: fct_orders                                        ║
║ Environment: prod                                        ║
╚══════════════════════════════════════════════════════════╝

✓ Row Count Check: 15234 rows
  ⚠ customer_id: 23 NULLs (0.15%)

┌──────────────────────────────────────────────────────────┐
│                    SUMMARY                               │
├──────────────────────────────────────────────────────────┤
│ Total Checks: 2                                          │
│ Passed: 2                                                │
│ Success Rate: 100.0%                                     │
│ Duration: 0.312s                                         │
└──────────────────────────────────────────────────────────┘

⚠️  WARNINGS:
   • customer_id has 23 NULL values

🟡 Data quality checks completed with WARNINGS
```

---

## Advanced Use Cases

### 1. Conditional Quality Checks Based on Environment
```sql
{% macro smart_quality_check(model_name) %}
    {% if target.name == 'prod' %}
        {# Stricter checks in production #}
        {{ log_data_quality_checks(model_name, {
            'null_check_columns': ['id', 'created_at', 'updated_at'],
            'duplicate_check': 'id',
            'date_range_check': 'created_at'
        }) }}
    {% else %}
        {# Basic checks in dev #}
        {{ log_model_stats(ref(model_name)) }}
    {% endif %}
{% endmacro %}
```

### 2. Custom Validation with Logging
```sql
{% macro validate_and_log_model(model_name, min_rows=1000, max_nulls_pct=5) %}
    {% set relation = ref(model_name) %}
    
    {{ log("Starting validation for " ~ model_name, info=True) }}
    
    {# Get stats #}
    {% set stats = log_model_stats(relation, row_threshold=min_rows) %}
    
    {# Perform quality checks #}
    {% set quality = log_data_quality_checks(model_name, {
        'null_check_columns': ['id'],
        'duplicate_check': 'id'
    }) %}
    
    {# Make decisions based on results #}
    {% if stats.row_count < min_rows %}
        {{ exceptions.warn("Model " ~ model_name ~ " below minimum rows") }}
    {% endif %}
    
    {% if quality.errors > 0 %}
        {{ exceptions.raise_compiler_error("Model " ~ model_name ~ " failed quality checks") }}
    {% endif %}
    
    {{ log("Validation complete for " ~ model_name, info=True) }}
{% endmacro %}
```

### 3. Logging to Custom Table (Audit Trail)
```sql
{% macro log_to_audit_table(model_name, stats) %}
    {% set insert_sql %}
        insert into audit.model_stats (
            model_name,
            run_timestamp,
            row_count,
            column_count,
            estimated_size_mb,
            target_name
        ) values (
            '{{ model_name }}',
            current_timestamp,
            {{ stats.row_count }},
            {{ stats.column_count }},
            {{ stats.estimated_size_mb }},
            '{{ target.name }}'
        )
    {% endset %}
    
    {% do run_query(insert_sql) %}
    {{ log("Logged stats to audit table", info=True) }}
{% endmacro %}
```

---

## Best Practices

### 1. Use Post-Hooks for Automatic Checks
Add quality checks as post-hooks so they run automatically after model builds:

```sql
{{ config(
    post_hook=[
        "{{ log_data_quality_checks(this.name, {...}) }}"
    ]
) }}
```

### 2. Environment-Specific Logging
Leverage the `target.name` variable to adjust logging verbosity:

```sql
{% if target.name in ['dev', 'ci'] %}
    {{ log("Detailed dev info here", info=True) }}
{% endif %}
```

### 3. Combine Multiple Checks
Use both macros together for comprehensive monitoring:

```sql
{% set stats = log_model_stats(this) %}
{% set quality = log_data_quality_checks(this.name, {...}) %}

{% if stats.row_count > 0 and quality.success_rate == 100 %}
    {{ log("✅ Model passed all validations!", info=True) }}
{% endif %}
```

### 4. Create Project-Specific Wrappers
Wrap these macros with your own defaults:

```sql
{% macro my_standard_check(model_name) %}
    {{ log_data_quality_checks(model_name, {
        'null_check_columns': ['id', 'created_at'],
        'duplicate_check': 'id'
    }) }}
{% endmacro %}
```

---

## Logging Levels in dbt

Understanding dbt's logging system:

- `{{ log(message, info=True) }}` - Info level (default, always shown)
- `{{ log(message, info=False) }}` - Debug level (only with --debug flag)
- `{{ exceptions.warn(message) }}` - Warning (shows in yellow)
- `{{ exceptions.raise_compiler_error(message) }}` - Error (stops execution)

---

## Performance Considerations

1. **Query Execution**: These macros execute SQL queries, which take time
2. **Large Tables**: Consider sampling for very large tables
3. **Production**: Use summary logging in production to reduce overhead
4. **Post-Hooks**: Post-hooks run after model build, adding to total time

---

## Troubleshooting

### Issue: Macro not found
**Solution**: Ensure the macro files are in the `macros/` directory and run `dbt compile`

### Issue: Permission errors on queries
**Solution**: Ensure your dbt user has SELECT permissions on the models

### Issue: Verbose logging in production
**Solution**: The macros already check `target.name` - ensure you're using the correct target

---

## Examples of Complete Model Implementation

### dim_customers.sql with Full Monitoring
```sql
{{ config(
    materialized='table',
    post_hook=[
        "{{ log_model_stats(this, row_threshold=100) }}",
        "{{ log_data_quality_checks(this.name, {
            'null_check_columns': ['customer_id', 'email'],
            'duplicate_check': 'customer_id'
        }) }}"
    ]
) }}

with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orders
    group by 1
)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    coalesce(customer_orders.number_of_orders, 0) as number_of_orders
from customers
left join customer_orders using (customer_id)
```

This will automatically log statistics and perform quality checks every time the model runs!
