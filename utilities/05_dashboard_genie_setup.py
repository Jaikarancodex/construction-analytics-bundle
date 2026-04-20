# Databricks notebook source
# DBTITLE 1,Header — Dashboard & Genie
# MAGIC %md
# MAGIC    
# MAGIC # 05 — AI/BI Dashboard: Construction Cost & Performance Control Tower
# MAGIC ## Programmatic Dashboard Creation via Lakeview API
# MAGIC
# MAGIC Creates a Lakeview dashboard with widgets for:
# MAGIC * Cost variance trends
# MAGIC * Schedule KPI heatmap
# MAGIC * Vendor leaderboard
# MAGIC * Invoice aging distribution
# MAGIC * Project health overview
# MAGIC * ML risk predictions
# MAGIC * Employee purchase order summary

# COMMAND ----------

# DBTITLE 1,Setup — Dashboard Configuration
import requests
import json

catalog_name = "construction_demo"

# Get workspace URL and token
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Gold tables for the dashboard
gold_tables = {
    "cost_variance": f"{catalog_name}.gold.gold_cost_variance_monthly",
    "schedule_kpi": f"{catalog_name}.gold.gold_schedule_kpi",
    "vendor_scorecard": f"{catalog_name}.gold.gold_vendor_scorecard",
    "invoice_aging": f"{catalog_name}.gold.gold_invoice_aging",
    "project_summary": f"{catalog_name}.gold.gold_project_summary",
    "risk_scores": f"{catalog_name}.gold.ml_project_risk_scores",
    "employee_purchase": f"{catalog_name}.gold.gold_employee_purchase_summary",
}

print(f"   Workspace: {workspace_url}")
for k, v in gold_tables.items():
    print(f"   {k}: {v}")

# COMMAND ----------

# DBTITLE 1,Define Dashboard Queries
# ============================================================
# SQL queries for each dashboard widget
# ============================================================

queries = {
    "cost_trend": f"""
        SELECT period_year_month, project_type,
               SUM(total_budget_eur) as budget,
               SUM(total_actual_eur) as actual,
               SUM(total_variance_eur) as variance
        FROM {gold_tables['cost_variance']}
        WHERE period_year_month IS NOT NULL
        GROUP BY period_year_month, project_type
        ORDER BY period_year_month
    """,
    "schedule_overview": f"""
        SELECT project_name, takt_zone,
               completion_rate_pct, delay_rate_pct, avg_delay_days,
               status
        FROM {gold_tables['schedule_kpi']}
        WHERE project_name IS NOT NULL
        ORDER BY completion_rate_pct ASC
        LIMIT 50
    """,
    "vendor_top_bottom": f"""
        SELECT vendor_name, vendor_type, trade_category,
               avg_overall_score, rehire_rate_pct,
               evaluation_count, total_invoices, disputed_invoices
        FROM {gold_tables['vendor_scorecard']}
        WHERE vendor_name IS NOT NULL AND avg_overall_score IS NOT NULL
        ORDER BY avg_overall_score DESC
        LIMIT 30
    """,
    "invoice_aging_dist": f"""
        SELECT aging_bucket, payment_status,
               SUM(invoice_count) as invoices,
               SUM(total_amount_eur) as amount_eur
        FROM {gold_tables['invoice_aging']}
        GROUP BY aging_bucket, payment_status
        ORDER BY aging_bucket
    """,
    "project_health": f"""
        SELECT project_name, project_type, municipality, status,
               budget_utilization_pct, milestone_completion_pct,
               avg_delay_days, risk_score, risk_level
        FROM {gold_tables['project_summary']}
        WHERE project_name IS NOT NULL
        ORDER BY risk_score DESC
        LIMIT 50
    """,
    "risk_predictions": f"""
        SELECT p.project_id, pr.project_name, pr.project_type,
               p.overrun_probability, p.risk_level
        FROM {gold_tables['risk_scores']} p
        JOIN {catalog_name}.silver.dim_projects pr ON p.project_id = pr.project_id
        WHERE p.risk_level = 'high'
        ORDER BY p.overrun_probability DESC
        LIMIT 20
    """,
    "employee_purchase_summary": f"""
        SELECT full_name, role, department,
               total_po_count, total_po_value_eur, total_invoiced_eur,
               over_invoiced_count, over_invoiced_pct,
               project_count, avg_po_value_eur
        FROM {gold_tables['employee_purchase']}
        WHERE full_name IS NOT NULL
        ORDER BY total_po_value_eur DESC
        LIMIT 30
    """,
}

print(f"✅ {len(queries)} dashboard queries defined")

# COMMAND ----------

# DBTITLE 1,Preview Dashboard Data
# ============================================================
# Preview each query to validate data availability
# ============================================================
for name, sql in queries.items():
    print(f"\n─── {name} ───")
    try:
        df = spark.sql(sql)
        print(f"   Rows: {df.count()}, Columns: {len(df.columns)}")
        display(df.limit(3))
    except Exception as e:
        print(f"   ⚠️ Error: {e}")

# COMMAND ----------

# DBTITLE 1,Create Lakeview Dashboard
# ============================================================
# Create the AI/BI Dashboard via Lakeview API
# NOTE: This creates the dashboard structure. Widget layout is
#       configured via the dashboard editor UI after creation.
# ============================================================

dashboard_name = "Construction Cost & Performance Control Tower"

# Create dashboard
response = requests.post(
    f"https://{workspace_url}/api/2.0/lakeview/dashboards",
    headers=headers,
    json={
        "display_name": dashboard_name,
    }
)

if response.status_code == 200:
    dashboard = response.json()
    dashboard_id = dashboard.get("dashboard_id")
    print(f"✅ Dashboard created: {dashboard_name}")
    print(f"   ID: {dashboard_id}")
    
    # Store for later use
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {catalog_name}.gold._dashboard_config (
        key STRING, value STRING
    ) USING DELTA""")
    spark.sql(f"""INSERT INTO {catalog_name}.gold._dashboard_config VALUES 
        ('dashboard_id', '{dashboard_id}'),
        ('dashboard_name', '{dashboard_name}')""")
else:
    print(f"❌ Failed to create dashboard: {response.status_code}")
    print(response.text)

# COMMAND ----------

# DBTITLE 1,Display Dashboard Link
# ============================================================
# Show link to the dashboard
# ============================================================
try:
    displayHTML(f"""
    <div style="padding: 20px; border-left: 5px solid #2196F3; background-color: #f0f8ff; margin: 15px 0; border-radius: 8px;">
        <h3 style="margin: 0 0 15px 0; color: #2196F3;">
            🏗️ Construction Cost & Performance Control Tower
        </h3>
        <p style="margin: 10px 0 0 0;">
            <a href='https://{workspace_url}/sql/dashboards/{dashboard_id}' target='_blank' 
               style='background-color: #2196F3; color: white; padding: 12px 24px; text-decoration: none; 
                      border-radius: 5px; font-weight: bold; display: inline-block; font-size: 16px;'>
               🚀 Open Dashboard →
            </a>
        </p>
        <p style="margin: 10px 0 0 0; color: #666;"><em>Add widgets using the queries defined above in the dashboard editor.</em></p>
    </div>
    """)
except:
    print("Dashboard link will be available after creating the dashboard.")
