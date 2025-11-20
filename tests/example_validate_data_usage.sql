{# 
===================================================================================
VALIDATE_DATA MACRO - USAGE EXAMPLES
===================================================================================

This file demonstrates various ways to use the validate_data macro for data validation.
These are example tests that show the macro's capabilities. To use them:

1. Copy the relevant test to your tests/ directory as a separate .sql file
2. Or reference the validation in your model's schema.yml file
3. Or use in post-hooks for runtime validation

The macro follows dbt best practices by:
- Returning failed records (dbt test paradigm)
- Being reusable and DRY
- Providing clear error messages
- Supporting multiple validation types
- Being configurable and extensible

===================================================================================
#}


{# ============================================================================
   EXAMPLE 1: NOT NULL VALIDATION
   Use Case: Ensure critical columns don't contain null values
   ============================================================================ #}

-- Test file: tests/validate_customer_id_not_null.sql
-- {{ validate_data('not_null', 'customer_id') }}


{# ============================================================================
   EXAMPLE 2: UNIQUE VALIDATION
   Use Case: Ensure primary keys are unique
   ============================================================================ #}

-- Test file: tests/validate_order_id_unique.sql
-- {{ validate_data('unique', 'order_id') }}

-- With custom error message:
-- {{ validate_data('unique', 'order_id', {
--     'error_message': 'Order IDs must be unique across all records'
-- }) }}


{# ============================================================================
   EXAMPLE 3: POSITIVE VALUES VALIDATION
   Use Case: Validate that amounts/quantities are positive
   ============================================================================ #}

-- Test file: tests/validate_positive_amount.sql
-- {{ validate_data('positive', 'order_amount') }}

-- Allowing nulls (for optional fields):
-- {{ validate_data('positive', 'discount_amount', {
--     'allow_nulls': true,
--     'error_message': 'Discount amounts must be positive when present'
-- }) }}


{# ============================================================================
   EXAMPLE 4: NON-NEGATIVE VALUES VALIDATION
   Use Case: Validate values that can be zero but not negative
   ============================================================================ #}

-- Test file: tests/validate_non_negative_quantity.sql
-- {{ validate_data('non_negative', 'quantity') }}


{# ============================================================================
   EXAMPLE 5: DATE RANGE VALIDATION
   Use Case: Ensure dates fall within acceptable ranges
   ============================================================================ #}

-- Test file: tests/validate_order_date_range.sql
-- {{ validate_data('date_range', 'order_date', {
--     'min_date': '2020-01-01',
--     'max_date': 'current_date',
--     'error_message': 'Order dates must be between 2020-01-01 and today'
-- }) }}

-- For historical data with no future dates:
-- {{ validate_data('date_range', 'created_at', {
--     'min_date': '1900-01-01',
--     'max_date': 'current_date',
--     'allow_nulls': false
-- }) }}


{# ============================================================================
   EXAMPLE 6: EMAIL FORMAT VALIDATION
   Use Case: Validate email addresses have proper format
   ============================================================================ #}

-- Test file: tests/validate_customer_email.sql
-- {{ validate_data('email_format', 'email') }}


{# ============================================================================
   EXAMPLE 7: REFERENTIAL INTEGRITY VALIDATION
   Use Case: Validate foreign key relationships
   ============================================================================ #}

-- Test file: tests/validate_customer_foreign_key.sql
-- {{ validate_data('referential_integrity', 'customer_id', {
--     'parent_model': 'stg_jaffle_shop__customers',
--     'parent_column': 'customer_id',
--     'error_message': 'All customer_ids must exist in customers table'
-- }) }}

-- With different column names:
-- {{ validate_data('referential_integrity', 'product_id', {
--     'parent_model': 'dim_products',
--     'parent_column': 'id'
-- }) }}


{# ============================================================================
   EXAMPLE 8: ACCEPTED VALUES VALIDATION
   Use Case: Validate enum/categorical columns
   ============================================================================ #}

-- Test file: tests/validate_payment_status.sql
-- {{ validate_data('accepted_values', 'status', {
--     'values': ['pending', 'processing', 'completed', 'cancelled', 'refunded'],
--     'error_message': 'Invalid payment status'
-- }) }}

-- With null handling:
-- {{ validate_data('accepted_values', 'payment_method', {
--     'values': ['credit_card', 'debit_card', 'paypal', 'bank_transfer'],
--     'allow_nulls': true
-- }) }}


{# ============================================================================
   EXAMPLE 9: CUSTOM SQL VALIDATION
   Use Case: Complex business logic validation
   ============================================================================ #}

-- Test file: tests/validate_order_total_matches_items.sql
-- {{ validate_data('custom_sql', config={
--     'validation_sql': 'total_amount = (subtotal + tax - discount)',
--     'error_message': 'Order total must equal subtotal + tax - discount'
-- }) }}

-- Test file: tests/validate_discount_logic.sql
-- {{ validate_data('custom_sql', config={
--     'validation_sql': 'discount_amount <= subtotal',
--     'error_message': 'Discount cannot exceed subtotal'
-- }) }}


{# ============================================================================
   EXAMPLE 10: EXPRESSION IS TRUE VALIDATION
   Use Case: Validate conditional business rules
   ============================================================================ #}

-- Test file: tests/validate_completed_orders_have_payment.sql
-- {{ validate_data('expression_is_true', config={
--     'expression': 'payment_id is not null',
--     'condition': 'status = ''completed''',
--     'error_message': 'Completed orders must have a payment_id'
-- }) }}

-- Test file: tests/validate_delivery_date_after_order.sql
-- {{ validate_data('expression_is_true', config={
--     'expression': 'delivered_at > ordered_at',
--     'condition': 'delivered_at is not null',
--     'error_message': 'Delivery date must be after order date'
-- }) }}


{# ============================================================================
   EXAMPLE 11: ROW COUNT VALIDATION
   Use Case: Ensure models have minimum/maximum number of records
   ============================================================================ #}

-- Test file: tests/validate_minimum_rows.sql
-- {{ validate_data('row_count', config={
--     'min_rows': 1,
--     'error_message': 'Table must contain at least 1 row'
-- }) }}

-- Test file: tests/validate_row_count_bounds.sql
-- {{ validate_data('row_count', config={
--     'min_rows': 100,
--     'max_rows': 1000000,
--     'error_message': 'Table row count outside acceptable range'
-- }) }}


{# ============================================================================
   EXAMPLE 12: USING IN SCHEMA.YML FILES
   ============================================================================ #}

-- In your model's schema.yml file:
--
-- models:
--   - name: fct_orders
--     columns:
--       - name: order_id
--         tests:
--           - unique
--           - not_null
--       
--       - name: customer_id
--         tests:
--           - not_null
--           - relationships:
--               to: ref('dim_customers')
--               field: customer_id
--       
--       - name: order_total
--         description: Total order amount in dollars
--         tests:
--           - dbt_utils.expression_is_true:
--               expression: ">= 0"
--           # Or use our custom macro:
--           # - validate_data('non_negative', 'order_total')
--       
--       - name: order_date
--         tests:
--           - not_null
--           # Custom date range test could be added here


{# ============================================================================
   EXAMPLE 13: USING IN POST-HOOKS
   Use Case: Runtime validation after model build
   ============================================================================ #}

-- In your model's config block:
--
-- {{
--     config(
--         materialized='table',
--         post_hook=[
--             "{{ log('Running data quality validations...', info=true) }}",
--             "{% set validation_query %}{{ validate_data('row_count', config={'min_rows': 1}) }}{% endset %}",
--             "{% if execute %}{{ exceptions.raise_compiler_error('Validation failed!') if run_query(validation_query).rows|length > 0 }}{% endif %}"
--         ]
--     )
-- }}


{# ============================================================================
   EXAMPLE 14: COMBINING WITH SEVERITY LEVELS
   Use Case: Some validations are warnings, others are errors
   ============================================================================ #}

-- In schema.yml with severity levels:
--
-- models:
--   - name: fct_orders
--     tests:
--       - validate_data:
--           validation_type: positive
--           column_name: order_total
--           config:
--             error_message: "Order total must be positive"
--           severity: error  # This will fail the build
--       
--       - validate_data:
--           validation_type: date_range
--           column_name: order_date
--           config:
--             min_date: '2023-01-01'
--             max_date: current_date
--           severity: warn  # This will only warn, not fail


{# ============================================================================
   EXAMPLE 15: REAL-WORLD COMPREHENSIVE EXAMPLE
   Use Case: Complete validation suite for an orders model
   ============================================================================ #}

-- Create file: tests/validate_fct_orders_comprehensive.sql
--
-- This test combines multiple validations for the fct_orders model
--
-- with validation_results as (
--     -- Check 1: Order IDs are unique
--     select 'unique_order_id' as check_name, count(*) as failures
--     from ({{ validate_data('unique', 'order_id') }})
--     
--     union all
--     
--     -- Check 2: Customer IDs exist in dim_customers
--     select 'valid_customer_id' as check_name, count(*) as failures
--     from ({{ validate_data('referential_integrity', 'customer_id', {
--         'parent_model': 'dim_customers',
--         'parent_column': 'customer_id'
--     }) }})
--     
--     union all
--     
--     -- Check 3: Order totals are non-negative
--     select 'non_negative_total' as check_name, count(*) as failures
--     from ({{ validate_data('non_negative', 'order_total') }})
--     
--     union all
--     
--     -- Check 4: Order dates are in valid range
--     select 'valid_order_date' as check_name, count(*) as failures
--     from ({{ validate_data('date_range', 'order_date', {
--         'min_date': '2020-01-01',
--         'max_date': 'current_date'
--     }) }})
--     
--     union all
--     
--     -- Check 5: Status is valid
--     select 'valid_status' as check_name, count(*) as failures
--     from ({{ validate_data('accepted_values', 'status', {
--         'values': ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
--     }) }})
-- )
-- 
-- select 
--     check_name,
--     failures,
--     'Validation failed: ' || check_name as error_message
-- from validation_results
-- where failures > 0


{# ============================================================================
   BEST PRACTICES SUMMARY
   ============================================================================

1. **Start Simple**: Begin with basic validations (not_null, unique) and add complexity

2. **Use Appropriate Severity**: 
   - Use 'error' for critical business rules
   - Use 'warn' for data quality monitoring

3. **Clear Error Messages**: Always provide context in error messages

4. **Combine with Existing Tests**: Use alongside dbt's built-in tests and dbt_utils

5. **Document Your Validations**: Comment why each validation exists

6. **Test Your Tests**: Intentionally break data to verify tests catch issues

7. **Performance**: Be mindful of complex validations on large datasets

8. **Reusability**: Create custom wrappers for common validation patterns

9. **Version Control**: Track validation logic changes like any other code

10. **Monitor Failures**: Set up alerting for test failures in production

