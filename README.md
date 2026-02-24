# Genie & AI/BI Dashboard Tutorial

This tutorial walks you through ingesting data into Databricks, creating a Genie Space for natural language queries, and building an AI/BI Dashboard using the Dashboard Agent.

## Prerequisites

- A Databricks workspace (see Step 1 if you don't have one)
- Permissions to create catalogs, schemas, and tables in Unity Catalog

---

## Step 1: Sign Up for Databricks

If you don't have a Databricks workspace, sign up for the free edition:

**[Sign up for Databricks Free Edition](https://www.databricks.com/learn/free-edition)**

---

## Step 2: Ingest Data

Clone this repository to your Databricks workspace and run the data ingestion notebook.

### Instructions

1. In your Databricks workspace, go to **Workspace** > **Users** > your username
2. Click **Create** > **Git folder**
3. Enter this repository URL and click **Create Git folder**
4. Open `01_data_ingestion.py`
5. Enter your **catalog** and **schema** names in the widgets at the top
6. Click **Run all** to execute the notebook

### What Gets Created

| Object | Description |
|--------|-------------|
| `<catalog>.<schema>.raw_data` | Volume containing the source CSV files |
| `<catalog>.<schema>.accounts` | Delta table with account master data |
| `<catalog>.<schema>.consumption` | Delta table with monthly consumption data |
| `<catalog>.<schema>.synthetic_data_all_metric_view` | Metric view with pre-defined dimensions and measures |

---

## Step 3: Create a Genie Space

Create a Genie Space to query your data using natural language.

### Instructions

1. In the Databricks sidebar, click **Genie**
2. Click **New** to create a new Genie Space
3. Give your space a name (e.g., "Consumption Analytics")
4. Add the metric view created in Step 2: `<catalog>.<schema>.synthetic_data_all_metric_view`
5. Click **Save**

### Try It Out

Ask questions in natural language, such as:
- "Show me revenue by months in 2025 as a bar chart"
- "What drove a spike in June?"

![Genie Demo](images/Genie%20research%20gif%2016x9.gif)

For more information, see the [Genie documentation](https://docs.databricks.com/aws/en/genie/).

---

## Step 4: Create a Dashboard with the Dashboard Agent

Use the AI-powered Dashboard Agent to create visualizations from your data.

### Instructions

1. In the Databricks sidebar, click **Dashboards**
2. Click **Create dashboard**
3. Select **Create with AI**
4. Choose the metric view: `<catalog>.<schema>.synthetic_data_all_metric_view`
5. Enter the following prompt:

```
Present data in three widgets:

Monthly bar chart — DBU consumption over time, highlight peaks
Stacked bar chart — DBU by region (AMER, APJ, EMEA, LATAM) broken down by country
Leaderboard table — top reps ranked by $DBU with a progress bar and any other insights you think will be valuable
```

6. The Dashboard Agent will generate the visualizations for you

![Dashboard Agent Demo](images/dashboard%20agent%2016x9.gif)

For more information, see the [Dashboard Agent documentation](https://docs.databricks.com/aws/en/dashboards/dashboard-agent).

---

## Resources

- [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
- [Genie Documentation](https://docs.databricks.com/aws/en/genie/)
- [Dashboard Agent Documentation](https://docs.databricks.com/aws/en/dashboards/dashboard-agent)
