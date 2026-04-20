# Databricks notebook source
# DBTITLE 1,Configuration — Catalog, Schema, Variables
# ============================================================
# Construction Analytics — Project Configuration
# ============================================================
# Widget-based parameterization — override from job or manually
# ============================================================

# ---- Widgets ----
dbutils.widgets.text("catalog_name", "construction_demo", "Catalog Name")
dbutils.widgets.text("base_path", "/Workspace/Users/jaikaran.n@diggibyte.com/construction-project", "Project Base Path")
dbutils.widgets.dropdown("reset_schemas", "no", ["yes", "no"], "Create Schemas If Missing")

# ---- Read values ----
catalog_name  = dbutils.widgets.get("catalog_name").strip()
base_path     = dbutils.widgets.get("base_path").strip()
reset_schemas = dbutils.widgets.get("reset_schemas") == "yes"

# ---- Fixed schema names ----
schema_bronze = "bronze"
schema_silver = "silver"
schema_gold   = "gold"

# ---- Activate catalog & optionally create schemas ----
spark.sql(f"USE CATALOG {catalog_name}")
if reset_schemas:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_bronze}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_silver}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

print(f"✅ Variables configured:")
print(f"    Catalog:       {catalog_name}")
print(f"    Bronze Schema: {schema_bronze}")
print(f"    Silver Schema: {schema_silver}")
print(f"    Gold Schema:   {schema_gold}")
print(f"    Base Path:     {base_path}")
print(f"    Create Schemas:{reset_schemas}")

# COMMAND ----------

# DBTITLE 1,Bronze Table Registry
# ============================================================
# Table registry — driven by widget values
# ============================================================

_TABLE_NAMES = [
    "dim_employee", "dim_projects", "dim_vendors",
    "fact_schedule_milestones", "fact_project_costs",
    "fact_purchase_orders", "fact_invoices",
    "fact_subcontractor_performance",
]

BRONZE_TABLES = {t: f"{catalog_name}.{schema_bronze}.{t}" for t in _TABLE_NAMES}
SILVER_TABLES = {t: f"{catalog_name}.{schema_silver}.{t}" for t in _TABLE_NAMES}

GOLD_TABLES = [
    f"{catalog_name}.{schema_gold}.gold_cost_variance_monthly",
    f"{catalog_name}.{schema_gold}.gold_schedule_kpi",
    f"{catalog_name}.{schema_gold}.gold_vendor_scorecard",
    f"{catalog_name}.{schema_gold}.gold_invoice_aging",
    f"{catalog_name}.{schema_gold}.gold_project_summary",
    f"{catalog_name}.{schema_gold}.gold_financial_insights",
    f"{catalog_name}.{schema_gold}.gold_vendor_risk_performance",
]

ML_TABLES = [
    f"{catalog_name}.{schema_gold}.ml_feature_table",
    f"{catalog_name}.{schema_gold}.ml_project_risk_scores",
]

print(f" Bronze tables: {len(BRONZE_TABLES)}")
print(f" Silver tables: {len(SILVER_TABLES)}")
print(f" Gold tables:   {len(GOLD_TABLES)}")
print(f" ML tables:     {len(ML_TABLES)}")

# COMMAND ----------

# DBTITLE 1,Helper Functions
# ============================================================
# Helper functions for the project
# ============================================================

def display_launch(url, text, description=None):
    """Display a styled launch button in notebooks."""
    displayHTML(f"""
        <div style="padding: 20px; border-left: 5px solid #2196F3; background-color: #f0f8ff; margin: 15px 0; border-radius: 8px;">
            <h3 style="margin: 0 0 15px 0; color: #2196F3; display: flex; align-items: center;">
                {"" + description if description else ""}
            </h3>
            <p style="margin: 10px 0 0 0;">
                <a href='{url}' target='_blank' 
                   style='background-color: #2196F3; color: white; padding: 12px 24px; text-decoration: none; 
                          border-radius: 5px; font-weight: bold; display: inline-block; font-size: 16px;'>
                    {text} →
                </a>
            </p>
        </div>
    """)

def display_launch_error(message, detail):
    """Display a styled error banner."""
    displayHTML(f"""
    <div style="padding: 20px; border-left: 5px solid #dc3545; background-color: #fff5f5; margin: 15px 0; border-radius: 8px;">
        <h3 style="margin: 0 0 10px 0; color: #dc3545;">{message}</h3>
        <p style="margin: 5px 0; font-size: 16px;">{detail}</p>
    </div>
    """)

def drop_fs_table(table_name):
    """Drop a feature store table safely."""
    from databricks.feature_store import FeatureStoreClient
    fs = FeatureStoreClient()
    try:
        fs.drop_table(table_name)
    except Exception as e:
        print(f"Can't drop the fs table: {e}")
    try:
        spark.sql(f"DROP TABLE IF EXISTS `{table_name}`")
    except Exception as e:
        print(f"Can't drop the delta table: {e}")

print("Helper functions loaded")

# COMMAND ----------

# DBTITLE 1,Date Parsing Utility
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

def parse_messy_dates(df, *date_cols):
    """
    Parse messy date columns (dd/MM/yyyy, yyyyMMdd, dd-MMM-yyyy, MM-dd-yyyy, yyyy-MM-dd)
    into proper DateType columns with suffix '_dt'.
    """
    date_formats = [
        "yyyy-MM-dd",
        "dd/MM/yyyy",
        "yyyyMMdd",
        "dd-MMM-yyyy",
        "MM-dd-yyyy",
    ]
    for col_name in date_cols:
        new_col = col_name.replace("_date", "").replace("_str", "") + "_dt" if not col_name.endswith("_dt") else col_name
        parsed = F.lit(None).cast(DateType())
        for fmt in date_formats:
            parsed = F.coalesce(parsed, F.to_date(F.col(col_name), fmt))
        df = df.withColumn(new_col, parsed)
    return df

print("Date parsing utility loaded")
