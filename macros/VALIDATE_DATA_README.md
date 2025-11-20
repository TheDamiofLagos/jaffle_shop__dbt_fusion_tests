# validate_data Macro - Data Validation for dbt

## Overview

The `validate_data` macro is a comprehensive, reusable data validation solution for dbt projects that follows best practices for testing and data quality.

## Key Features

✅ **11 validation types** covering common data quality scenarios  
✅ **dbt best practices** - Returns failed records for test compatibility  
✅ **Highly configurable** - Flexible config options for each validation type  
✅ **Clear error messages** - Descriptive failure messages  
✅ **Multi-adapter compatible** - Works across different database engines  
✅ **Severity support** - Use with warn or error severity levels  
✅ **DRY principle** - Write once, use everywhere  

## Installation

The macro is already available in your `dbt_fundamentals/macros/` directory. No additional installation needed.

## Validation Types

### 1. not_null
Validates that a column contains no NULL values.

```sql
{{ validate_data('not_null', 'customer_id') }}
```

### 2. unique
Validates that a column has no duplicate values.

```sql
{{ validate_data('unique', 'order_id') }}
```

### 3. positive
Validates that numeric values are greater than zero.

```sql
{{ validate_data('positive', 'order_amount') }}
```

### 4. non_negative
Validates that numeric values are zero or greater.

```sql
{{ validate_data('non_negative', 'quantity') }}
```

### 5. date_range
Validates dates fall within a specified range.

```sql
{{ validate_data('date_range', 'order_date', {
    'min_date': '2020-01-01',
    'max_date': 'current_date'
}) }}
```

### 6. email_format
Validates email addresses have proper format.

```sql
{{ validate_data('email_format', 'email') }}
```

### 7. referential_integrity
Validates foreign key relationships.

```sql
{{ validate_data('referential_integrity', 'customer_id', {
    'parent_model': 'dim_customers',
    'parent_column': 'customer_id'
}) }}
```

### 8. accepted_values
Validates values are in an approved list.

```sql
{{ validate_data('accepted_values', 'status', {
    'values': ['pending', 'processing', 'completed', 'cancelled']
}) }}
```

### 9. custom_sql
Executes custom SQL validation logic.

```sql
{{ validate_data('custom_sql', config={
    'validation_sql': 'total_amount = (subtotal + tax - discount)',
    'error_message': 'Order total calculation is incorrect'
}) }}
```

### 10. expression_is_true
Validates conditional business rules.

```sql
{{ validate_data('expression_is_true', config={
    'expression': 'payment_id is not null',
    'condition': 'status = ''completed''',
    'error_message': 'Completed orders must have a payment'
}) }}
```

### 11. row_count
Validates table has acceptable number of records.

```sql
{{ validate_data('row_count', config={
    'min_rows': 1,
    'max_rows': 1000000
}) }}
```

## Usage Patterns

### As a Test File

Create a test file in `tests/` directory:

```sql
-- tests/validate_customer_email.sql
{{ config(severity='error') }}

{{ validate_data('email_format', 'email') }}
```

### In Schema.yml

```yaml
models:
  - name: fct_orders
    columns:
      - name: order_id
        tests:
          - unique
          - not_null
```

### In Post-Hooks

```sql
{{
    config(
        materialized='table',
        post_hook=[
            "{% set validation_query %}{{ validate_data('row_count', config={'min_rows': 1}) }}{% endset %}",
            "{% if execute %}{{ exceptions.raise_compiler_error('Validation failed!') if run_query(validation_query).rows|length > 0 }}{% endif %}"
        ]
    )
}}
```

## Configuration Options

### Common Options (All Types)

- `error_message` (string): Custom error message
- `allow_nulls` (boolean): Whether to allow NULL values (default: false)

### Type-Specific Options

**date_range:**
- `min_date` (string): Minimum acceptable date
- `max_date` (string): Maximum acceptable date

**referential_integrity:**
- `parent_model` (string): Name of parent model
- `parent_column` (string): Column name in parent model

**accepted_values:**
- `values` (list): List of acceptable values

**custom_sql:**
- `validation_sql` (string): SQL condition that should be true

**expression_is_true:**
- `expression` (string): Expression that should be true
- `condition` (string): When to check the expression

**row_count:**
- `min_rows` (integer): Minimum number of rows
- `max_rows` (integer): Maximum number of rows

## Best Practices

1. **Start Simple** - Begin with basic validations and add complexity as needed
2. **Use Severity Wisely** - Set 'error' for critical rules, 'warn' for monitoring
3. **Clear Messages** - Always provide context in error messages
4. **Combine Tests** - Use alongside dbt's built-in tests and dbt_utils
5. **Document Why** - Comment why each validation exists
6. **Test Your Tests** - Intentionally break data to verify tests work
7. **Consider Performance** - Be mindful of complex validations on large datasets
8. **Create Wrappers** - Build custom macros for repeated validation patterns

## Examples

See `tests/example_validate_data_usage.sql` for comprehensive examples including:
- Basic validation patterns
- Advanced configurations
- Real-world use cases
- Integration with schema.yml
- Post-hook implementations
- Combining multiple validations

## Integration with Existing Tools

The macro works seamlessly with:
- ✅ dbt's built-in tests (unique, not_null, relationships, accepted_values)
- ✅ dbt_utils package tests
- ✅ Great Expectations for dbt
- ✅ Elementary data monitoring
- ✅ Custom test frameworks

## Troubleshooting

### Test Not Running
- Ensure the test file is in the `tests/` directory
- Check that the file has `.sql` extension
- Verify the model being tested exists

### Validation Failing Unexpectedly
- Check the validation logic matches your data type
- Review the error message for specific failures
- Test with a small dataset first

### Performance Issues
- Consider adding WHERE clauses to limit scope
- Use sampling for large datasets in development
- Optimize complex validations

## Contributing

To add new validation types:

1. Add a new `elif` block in the macro
2. Follow the existing pattern for parameter validation
3. Return failed records with clear error messages
4. Update documentation and examples
5. Test thoroughly across different scenarios

## Support

For issues or questions:
- Review the examples file
- Check dbt documentation: https://docs.getdbt.com
-eR vwmcrosoecodfoimplmntiodtails
## License
## Licene

This macro is part of the dbt_fundamentals project and follows the same license.
