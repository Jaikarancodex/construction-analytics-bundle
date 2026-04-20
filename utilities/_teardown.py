# Databricks notebook source
# DBTITLE 1,Header — Teardown
# MAGIC %md
# MAGIC # 🗑️ Teardown — Clean Up All Project Resources
# MAGIC
# MAGIC **⚠️ WARNING: This notebook drops ALL tables, schemas, models, and endpoints created by this project.**
# MAGIC
# MAGIC Run this when you want to:
# MAGIC * Reset and re-run the entire demo from scratch
# MAGIC * Clean up after a workshop or presentation
# MAGIC * Remove all resources before deleting the project folder
# MAGIC
# MAGIC | What Gets Dropped | Scope |
# MAGIC |---|---|
# MAGIC | Gold tables | `construction_demo.gold.*` |
# MAGIC | Silver tables | `construction_demo.silver.*` |
# MAGIC | Bronze tables | `construction_demo.bronze.*` |
# MAGIC | ML models | `construction_demo.gold.construction_cost_overrun_model` |
# MAGIC | Schemas | `gold`, `silver` (bronze preserved by default) |

# COMMAND ----------

# DBTITLE 1,Configuration — What to Clean
dbutils.widgets.text("catalog_name", "construction_demo", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")

# Set to True to also drop Bronze schema & tables
DROP_BRONZE = False  # ⚠️ Set True only if you want to regenerate data

# Set to True to drop the ML serving endpoint
DROP_SERVING_ENDPOINT = False

print(f"⚙️ Teardown configuration:")
print(f"   Catalog:               {catalog_name}")
print(f"   Drop Bronze:           {DROP_BRONZE}")
print(f"   Drop Serving Endpoint: {DROP_SERVING_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Step 1 — Drop Gold Tables & Schema
# ============================================================
# STEP 1: Drop Gold layer (reverse dependency order)
# ============================================================
gold_tables = [
    "ml_project_risk_scores", "ml_feature_table",
    "gold_project_summary", "gold_invoice_aging", "gold_vendor_scorecard",
    "gold_schedule_kpi", "gold_cost_variance_monthly", "_dashboard_config",
    "gold_financial_insights", "gold_vendor_risk_performance", "gold_employee_purchase_summary"
]

print("\n🗑️ Dropping Gold tables...")
for tbl in gold_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.gold.{tbl}")
        print(f"   ✅ Dropped {catalog_name}.gold.{tbl}")
    except Exception as e:
        print(f"   ⚠️ Could not drop {tbl}: {e}")

try:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.gold")
    print(f"\n   ✅ Dropped schema {catalog_name}.gold")
except Exception as e:
    print(f"   ⚠️ Could not drop gold schema: {e}")

# COMMAND ----------

# DBTITLE 1,Step 2 — Drop Silver Tables & Schema
# ============================================================
# STEP 2: Drop Silver layer
# ============================================================
silver_tables = [
    "dim_employee", "dim_projects", "dim_vendors",
    "fact_schedule_milestones", "fact_project_costs",
    "fact_purchase_orders", "fact_invoices",
    "fact_subcontractor_performance"
]

print("\n🗑️ Dropping Silver tables...")
for tbl in silver_tables:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.silver.{tbl}")
        print(f"   ✅ Dropped {catalog_name}.silver.{tbl}")
    except Exception as e:
        print(f"   ⚠️ Could not drop {tbl}: {e}")

try:
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.silver")
    print(f"\n   ✅ Dropped schema {catalog_name}.silver")
except Exception as e:
    print(f"   ⚠️ Could not drop silver schema: {e}")

# COMMAND ----------

# DBTITLE 1,Step 3 — Drop Bronze (Optional)
# ============================================================
# STEP 3: Drop Bronze layer (only if flag is True)
# ============================================================
if DROP_BRONZE:
    bronze_tables = [
        "dim_employee", "dim_projects", "dim_vendors",
        "fact_schedule_milestones", "fact_project_costs",
        "fact_purchase_orders", "fact_invoices",
        "fact_subcontractor_performance"
    ]
    print("\n🗑️ Dropping Bronze tables...")
    for tbl in bronze_tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.bronze.{tbl}")
            print(f"   ✅ Dropped {catalog_name}.bronze.{tbl}")
        except Exception as e:
            print(f"   ⚠️ Could not drop {tbl}: {e}")
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.bronze")
        print(f"\n   ✅ Dropped schema {catalog_name}.bronze")
    except Exception as e:
        print(f"   ⚠️ Could not drop bronze schema: {e}")
else:
    print("\n⏭️ Skipping Bronze teardown (DROP_BRONZE = False)")

# COMMAND ----------

# DBTITLE 1,Step 4 — Drop ML Model & Endpoint
# ============================================================
# STEP 4: Drop ML model from Unity Catalog & serving endpoint
# ============================================================
import mlflow
mlflow.set_registry_uri("databricks-uc")

model_name = f"{catalog_name}.gold.construction_cost_overrun_model"

try:
    client = mlflow.MlflowClient()
    client.delete_registered_model(model_name)
    print(f"\n✅ Deleted registered model: {model_name}")
except Exception as e:
    print(f"\n⚠️ Could not delete model {model_name}: {e}")

if DROP_SERVING_ENDPOINT:
    import requests
    workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}"}
    endpoint_name = "construction-cost-overrun"
    try:
        resp = requests.delete(f"https://{workspace_url}/api/2.0/serving-endpoints/{endpoint_name}", headers=headers)
        print(f"\n✅ Deleted serving endpoint: {endpoint_name}")
    except:
        print(f"\n⚠️ Could not delete endpoint: {endpoint_name}")
else:
    print("\n⏭️ Skipping endpoint teardown (DROP_SERVING_ENDPOINT = False)")

# COMMAND ----------

# DBTITLE 1,Teardown Complete
# ============================================================
# TEARDOWN COMPLETE
# ============================================================
displayHTML("""
<div style="padding: 25px; background: linear-gradient(135deg, #b71c1c 0%, #c62828 100%); border-radius: 12px; margin: 10px 0; text-align: center;">
  <h2 style="color: white;">🗑️ Teardown Complete</h2>
  <p style="color: #ffcdd2;">All project resources have been cleaned up.</p>
  <p style="color: #ef9a9a;">To rebuild, run the <strong>Construction Analytics Control Tower Demo</strong> notebook.</p>
</div>
""")
