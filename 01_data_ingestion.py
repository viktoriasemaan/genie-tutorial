# Databricks notebook source
# MAGIC %md
# MAGIC # Data Ingestion Notebook
# MAGIC
# MAGIC This notebook ingests CSV data from the Git repository into Delta tables in Unity Catalog.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Clone this repository to your Databricks workspace
# MAGIC - Have a catalog and schema created (or permissions to create them)
# MAGIC
# MAGIC **What this notebook does:**
# MAGIC 1. Creates a managed volume for raw data storage
# MAGIC 2. Copies CSV files from the repo's `data/` folder to the volume
# MAGIC 3. Reads the CSV files and writes them as Delta tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Enter your catalog and schema names below. These will be used for all objects created by this notebook.

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catalog Name")
dbutils.widgets.text("schema", "", "Schema Name")

# COMMAND ----------

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# Validate inputs
if not catalog or not schema:
    raise ValueError("Please provide both 'catalog' and 'schema' parameters in the widgets above.")

print(f"Using catalog: {catalog}")
print(f"Using schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup - Create Volume
# MAGIC Create a managed volume to store the raw CSV files.

# COMMAND ----------

# DBTITLE 1,Cell 6
volume_name = "raw_data"
volume_path = f"/Volumes/{catalog}/{schema}/{volume_name}"

# Create the catalog if it doesn't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

# Create the schema if it doesn't exist
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    COMMENT 'Schema for Genie demo data'
""")

# Create the volume if it doesn't exist
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume_name}
    COMMENT 'Raw data files for Genie demo'
""")

print(f"Volume created/verified: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 1: Accounts
# MAGIC Account master data with customer information.

# COMMAND ----------

# DBTITLE 1,Cell 13
import os
import shutil

# Get current notebook's directory, then navigate to data folder
current_dir = os.getcwd()
repo_root = os.path.dirname(current_dir)
data_path = os.path.join(repo_root, "genie-tutorial", "data")

print(f"Copying CSV files from: {data_path}")
print(f"To volume path: {volume_path}")

# Get list of CSV files
csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]
print(f"Found {len(csv_files)} CSV files\n")

for csv_file in csv_files:
    source = os.path.join(data_path, csv_file)
    dest = os.path.join(volume_path, csv_file)
    
    # Only copy if file doesn't exist
    if not os.path.exists(dest):
        try:
            shutil.copy2(source, dest)
            print(f"✓ Copied: {csv_file}")
        except Exception as e:
            print(f"✗ Failed to copy {csv_file}: {e}")
    else:
        print(f"✓ Already exists: {csv_file}")

print(f"\nProcessed {len(csv_files)} CSV file(s) in {volume_path}/")

# Read accounts CSV from volume
accounts_csv = f"{volume_path}/synthetic_accounts.csv"
accounts_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(accounts_csv)
)

print(f"\nAccounts data: {accounts_df.count()} rows")
accounts_df.printSchema()

# COMMAND ----------

# Preview the data
display(accounts_df.limit(10))

# COMMAND ----------

# Write to Delta table
accounts_table = f"{catalog}.{schema}.accounts"

# Create the table (will fail if table already exists)
accounts_df.write.saveAsTable(accounts_table)

print(f"Created table: {accounts_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table 2: Consumption Data
# MAGIC Monthly consumption data with DBU dollars and U2 units.

# COMMAND ----------

# DBTITLE 1,Cell 17
# Copy consumption CSV to volume using Python file operations
csv_source = os.path.join(data_path, "synthetic_data_all.csv")
csv_volume_path = f"{volume_path}/synthetic_data_all.csv"

# Only copy if file doesn't exist
if not os.path.exists(csv_volume_path):
    try:
        shutil.copy2(csv_source, csv_volume_path)
        print(f"✓ Copied: synthetic_data_all.csv to volume")
    except Exception as e:
        print(f"✗ Failed to copy synthetic_data_all.csv: {e}")
        raise
else:
    print(f"✓ File already exists: synthetic_data_all.csv")

# Read from volume with Spark
consumption_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csv_volume_path)
)

print(f"Consumption data: {consumption_df.count()} rows")
consumption_df.printSchema()

# COMMAND ----------

# Preview the data
display(consumption_df.limit(10))

# COMMAND ----------

# Write to Delta table
consumption_table = f"{catalog}.{schema}.consumption"

# Create the table (will fail if table already exists)
consumption_df.write.saveAsTable(consumption_table)

print(f"Created table: {consumption_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Results
# MAGIC Confirm that all tables were created successfully.

# COMMAND ----------

# List all tables in the schema
print(f"Tables in {catalog}.{schema}:")
tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
display(tables_df)

# COMMAND ----------

# Quick validation queries
print("=== Accounts Table Summary ===")
spark.sql(f"SELECT COUNT(*) as total_accounts FROM {catalog}.{schema}.accounts").show()
spark.sql(f"SELECT account_tier, COUNT(*) as count FROM {catalog}.{schema}.accounts GROUP BY account_tier").show()

print("\n=== Consumption Table Summary ===")
spark.sql(f"SELECT COUNT(*) as total_records FROM {catalog}.{schema}.consumption").show()
spark.sql(f"SELECT MIN(consumption_month) as min_month, MAX(consumption_month) as max_month FROM {catalog}.{schema}.consumption").show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC Your data has been ingested successfully. You now have:
# MAGIC
# MAGIC | Table | Description |
# MAGIC |-------|-------------|
# MAGIC | `accounts` | Account master data with customer details |
# MAGIC | `consumption` | Monthly consumption data with DBU dollars and U2 units |
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Use these tables to create a Genie Space for natural language queries
# MAGIC - Build dashboards and reports
# MAGIC - Explore the data with SQL

# COMMAND ----------

# DBTITLE 1,Create Metric View - synthetic_data_all_metric_view
# Create the metric view for consumption analytics
metric_view_name = f"{catalog}.{schema}.synthetic_data_all_metric_view"

# Set the current catalog
spark.sql(f"USE CATALOG {catalog}")

metric_view_sql = f"""
CREATE VIEW {metric_view_name}
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1

source: {catalog}.{schema}.consumption

dimensions:
  - name: account_id
    expr: '`account_id`'
    comment: Unique identifier for the account
    display_name: Account ID
  - name: account_name
    expr: '`account_name`'
    comment: Name of the account
    display_name: Account Name
  - name: region
    expr: '`region`'
    comment: Geographic region of the account
    display_name: Region
  - name: industry
    expr: '`industry`'
    comment: Industry sector of the account
    display_name: Industry
  - name: country
    expr: '`country`'
    comment: Country of the account
    display_name: Country
  - name: account_tier
    expr: '`account_tier`'
    comment: Account tier of the account. Values include Enterprise, Mid-Market, SMB
    display_name: Account Tier
  - name: sales_rep_name
    expr: '`sales_rep_name`'
    comment: Sales rep over the account
    display_name: Sales Rep Name
  - name: sales_manager_name
    expr: '`sales_manager_name`'
    comment: Sales manager over the account
    display_name: Sales Manager Name
  - name: account_status
    expr: '`account_status`'
    comment: Status of the account. Values include New, Churned, At Risk, and Active
    display_name: Account Status
  - name: consumption_month
    expr: '`consumption_month`'
    comment: Month of consumption
    display_name: Consumption Month
    format:
      type: date
      date_format: year_month_day
      leading_zeros: false

measures:
  - name: total_dbu_dollars
    expr: SUM(`dbu_dollars`)
    comment: Total DBU dollars consumed
    display_name: Total DBU Dollars
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
  - name: total_dbu_dollars_t12_months
    expr: SUM(`dbu_dollars`)
    window:
      - order: consumption_month
        semiadditive: last
        range: trailing 12 month
    comment: Total DBU dollars consumed in the trailing 12 months
    display_name: Total DBU Dollars - Trailing 12 Months
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
  - name: average_dbu_dollars
    expr: AVG(`dbu_dollars`)
    comment: Average (mean) DBU dollars consumed
    display_name: Average DBU Dollars
    format:
      type: currency
      currency_code: USD
      decimal_places:
        type: exact
        places: 2
      abbreviation: compact
  - name: distinct_accounts
    expr: count(distinct account_id)
    comment: Number of distinct accounts
    display_name: Distinct Account Count
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: total_u2_units
    expr: SUM(`u2_units`)
    comment: Total U2 units
    display_name: Total U2 Units
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: total_u2_units_t12_months
    expr: SUM(`u2_units`)
    window:
      - order: consumption_month
        semiadditive: last
        range: trailing 12 month
    comment: Total U2 units in the trailing 12 months
    display_name: Total U2 Units - Trailing 12 Months
    format:
      type: number
      decimal_places:
        type: exact
        places: 0
  - name: average_u2_units
    expr: AVG(`u2_units`)
    comment: Average (mean) U2 units
    display_name: Average U2 Units
    format:
      type: number
      decimal_places:
        type: exact
        places: 2
$$
"""

# Execute the SQL to create the metric view
spark.sql(metric_view_sql)

print(f"✓ Created metric view: {metric_view_name}")

# COMMAND ----------

# DBTITLE 1,Verify Metric View with Sample Query
# Verify the metric view was created
print(f"Metric view: {metric_view_name}\n")

# Sample query: Total DBU dollars by region and account tier
test_query = f"""
SELECT 
  region,
  account_tier,
  MEASURE(total_dbu_dollars) AS total_dbu_dollars,
  MEASURE(distinct_accounts) AS distinct_accounts,
  MEASURE(average_dbu_dollars) AS avg_dbu_dollars
FROM {metric_view_name}
GROUP BY ALL
ORDER BY total_dbu_dollars DESC
LIMIT 10
"""

print("Sample Query: Total DBU Dollars by Region and Account Tier")
print("=" * 60)
result_df = spark.sql(test_query)
display(result_df)

# COMMAND ----------

# DBTITLE 1,Test Trailing 12-Month Window Measures
# Test the trailing 12-month window measures
window_query = f"""
SELECT 
  consumption_month,
  region,
  MEASURE(total_dbu_dollars) AS monthly_dbu_dollars,
  MEASURE(total_dbu_dollars_t12_months) AS t12m_dbu_dollars,
  MEASURE(total_u2_units) AS monthly_u2_units,
  MEASURE(total_u2_units_t12_months) AS t12m_u2_units
FROM {metric_view_name}
WHERE region = 'AMER'
GROUP BY ALL
ORDER BY consumption_month DESC
LIMIT 12
"""

print("Trailing 12-Month Window Measures for AMER Region")
print("=" * 60)
window_df = spark.sql(window_query)
display(window_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Metric View Created Successfully! ✓
# MAGIC
# MAGIC ### View Details
# MAGIC **Name:** `viktoria_catalog.genie_test.synthetic_data_all_metric_view`
# MAGIC
# MAGIC **Source Table:** `viktoria_catalog.genie_test.consumption`
# MAGIC
# MAGIC ### Dimensions (10)
# MAGIC * `account_id` - Unique identifier for the account
# MAGIC * `account_name` - Name of the account
# MAGIC * `region` - Geographic region (AMER, EMEA, APJ, LATAM)
# MAGIC * `country` - Country of the account
# MAGIC * `industry` - Industry sector
# MAGIC * `account_tier` - Enterprise, Mid-Market, or SMB
# MAGIC * `sales_rep_name` - Sales representative
# MAGIC * `sales_manager_name` - Sales manager
# MAGIC * `account_status` - Active, At Risk, Churned, or New
# MAGIC * `consumption_month` - Month of consumption (date format)
# MAGIC
# MAGIC ### Measures (7)
# MAGIC 1. **total_dbu_dollars** - Total DBU dollars consumed (currency, USD)
# MAGIC 2. **total_dbu_dollars_t12_months** - Trailing 12-month DBU dollars (window measure)
# MAGIC 3. **average_dbu_dollars** - Average DBU dollars per record (currency, USD)
# MAGIC 4. **distinct_accounts** - Count of unique accounts (number)
# MAGIC 5. **total_u2_units** - Total U2 units consumed (number)
# MAGIC 6. **total_u2_units_t12_months** - Trailing 12-month U2 units (window measure)
# MAGIC 7. **average_u2_units** - Average U2 units per record (number)
# MAGIC
# MAGIC ### Key Features
# MAGIC * **Window Measures**: Trailing 12-month calculations for DBU dollars and U2 units
# MAGIC * **Semantic Metadata**: Display names, comments, and formatting for all dimensions and measures
# MAGIC * **Currency Formatting**: USD with 2 decimal places and compact abbreviation
# MAGIC * **Date Formatting**: Year-month-day format without leading zeros
# MAGIC
# MAGIC ### How to Query
# MAGIC Use the `MEASURE()` function to reference measures in your queries:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   region,
# MAGIC   account_tier,
# MAGIC   MEASURE(total_dbu_dollars) AS total_revenue,
# MAGIC   MEASURE(distinct_accounts) AS account_count
# MAGIC FROM viktoria_catalog.genie_test.synthetic_data_all_metric_view
# MAGIC GROUP BY ALL
# MAGIC ORDER BY total_revenue DESC
# MAGIC ```
# MAGIC
# MAGIC ### Next Steps
# MAGIC * Use this metric view in Genie Spaces for natural language queries
# MAGIC * Create dashboards with consistent metric definitions
# MAGIC * Build reports using the standardized measures
