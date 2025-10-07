# Jaffle Shop with Fusion - dbt Fundamentals

A dbt Fusion project demonstrating data transformation best practices using the Jaffle Shop dataset with Snowflake integration.

## 📋 Project Overview

This project implements a modern data warehouse using dbt Fusion to transform raw data from a fictional e-commerce company (Jaffle Shop) into analytics-ready dimensional models. The project includes staging models, dimensional models, and fact tables following best practices for data modeling.

## 🏗️ Project Structure

```
dbt_fundamentals/
├── models/
│   ├── _dictionary.md              # Centralized documentation definitions
│   ├── staging/                    # Staging layer - cleaned source data
│   │   ├── jaffle_shop/
│   │   │   ├── _jaffle_shop__sources.yml
│   │   │   ├── _stg_jaffle_shop__customers.yml
│   │   │   ├── _stg_jaffle_shop__orders.yml
│   │   │   ├── stg_jaffle_shop__customers.sql
│   │   │   └── stg_jaffle_shop__orders.sql
│   │   └── stripe/
│   │       ├── _stripe__sources.yml
│   │       ├── _stg_stripe__payments.yml
│   │       └── stg_stripe__payments.sql
│   └── marts/                      # Marts layer - business-ready models
│       ├── _dim_customers.yml
│       ├── _fct_orders.yml
│       ├── dim_customers.sql       # Customer dimension table
│       └── fct_orders.sql          # Orders fact table
├── tests/
│   └── assert_positive_total_for_payments.sql
├── macros/                         # Reusable SQL functions
├── seeds/                          # CSV files for reference data
├── snapshots/                      # Type 2 slowly changing dimensions
├── analysis/                       # Ad-hoc analysis queries
└── dbt_project.yml                 # Project configuration
```

## 📊 Data Models

### Staging Models
- **stg_jaffle_shop__customers**: Cleaned customer data with standardized column names
- **stg_jaffle_shop__orders**: Cleaned order data with standardized column names
- **stg_stripe__payments**: Cleaned payment data with amounts converted from cents to dollars

### Marts Models
- **dim_customers**: Dimensional model with customer attributes and order history metrics
  - Customer ID, name
  - First order date, most recent order date
  - Total number of orders
  - Lifetime value
  
- **fct_orders**: Fact table containing order-level transactions
  - Order ID, customer ID
  - Payment amount

## 🔧 Setup Instructions

### Prerequisites
- Python 3.8+
- dbt Fusion CLI
- Snowflake account with appropriate permissions
- Git

### Installation

1. **Clone the repository**
   ```bash
   git clone <your-repository-url>
   cd jaffle_shop_with_fusion/dbt_fundamentals
   ```

2. **Install dbt packages**
   ```bash
   dbtf deps
   ```

3. **Configure your profiles.yml**
   
   Create or update `~/.dbt/profiles.yml`:
   ```yaml
   dbt_fundamentals:
     target: dev
     outputs:
       dev:
         type: snowflake
         account: <your-account>
         user: <your-username>
         password: <your-password>
         role: <your-role>
         database: <your-database>
         warehouse: <your-warehouse>
         schema: <your-schema>
         threads: 4
   ```

4. **Test your connection**
   ```bash
   dbtf debug
   ```

## 🚀 Usage

### Build all models
```bash
dbtf build
```

### Run specific model layers
```bash
# Run staging models only
dbtf run --select staging

# Run marts models only
dbtf run --select marts
```

### Test data quality
```bash
dbtf test
```

### Generate documentation
```bash
dbtf docs generate
dbtf docs serve
```

### Compile SQL
```bash
dbtf compile
```

### Run specific models
```bash
# Run a specific model
dbtf run --select dim_customers

# Run a model and its dependencies
dbtf run --select +dim_customers

# Run a model and its downstream dependents
dbtf run --select dim_customers+
```

## 📚 Key Features

- ✅ **Modular design**: Separated staging and marts layers
- ✅ **Comprehensive documentation**: All models and columns documented using doc blocks
- ✅ **Data quality tests**: Unique and not-null constraints on key columns
- ✅ **Source freshness checks**: Configured for orders table
- ✅ **Best practices**: Following dbt and Kimball dimensional modeling standards
- ✅ **dbt Fusion**: Leveraging Fusion for enhanced data transformation capabilities

## 🔍 Data Lineage

```
Sources (Raw Data)
    ├── jaffle_shop.customers → stg_jaffle_shop__customers ┐
    ├── jaffle_shop.orders    → stg_jaffle_shop__orders    ├→ dim_customers
    └── stripe.payments       → stg_stripe__payments       ┘
                                                            ├→ fct_orders
```

## 🧪 Testing

The project includes several types of tests:
- **Schema tests**: Defined in YAML files (unique, not_null, relationships)
- **Data tests**: Custom SQL tests in the `tests/` directory
- **Source freshness**: Monitoring data recency

Run tests with:
```bash
dbtf test
```

Run tests for specific models:
```bash
dbtf test --select dim_customers
```

Check source freshness:
```bash
dbtf source freshness
```

## 📦 Dependencies

This project uses the following dbt packages:
- `dbt-utils`: Utility macros for dbt projects
- `codegen`: Code generation utilities

Defined in `packages.yml` and installed with `dbtf deps`.

## 🛠️ Common Commands

```bash
# Clean compiled files
dbtf clean

# List all models
dbtf list

# Show compiled SQL for a model
dbtf show --inline "select * from {{ ref('dim_customers') }}"

# Run and test in one command
dbtf build --select dim_customers

# Parse project (check for errors)
dbtf parse
```

## 🤝 Contributing

When making changes:
1. Create a new feature branch
2. Make your changes
3. Run `dbtf run` and `dbtf test` to verify
4. Update documentation as needed
5. Run `dbtf docs generate` to update docs
6. Submit a pull request

## 🐛 Troubleshooting

### Common Issues

**Connection errors:**
```bash
dbtf debug
```

**Model compilation errors:**
```bash
dbtf compile --select <model_name>
```

**Test failures:**
```bash
dbtf test --select <model_name> --store-failures
```

## 📖 Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [dbt Fusion Documentation](https://docs.getdbt.com/)
- [dbt Discourse](https://discourse.getdbt.com/)
- [dbt Slack Community](http://slack.getdbt.com/)
- [Snowflake Documentation](https://docs.snowflake.com/)

## 📝 License

This project is based on the dbt Learn fundamentals course and is intended for educational purposes.

---

**Project Type**: dbt Fusion  
**Last Updated**: October 2025
