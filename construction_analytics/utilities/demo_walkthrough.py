# Databricks notebook source
# DBTITLE 1,Cover — Construction Analytics Control Tower
# MAGIC %md
# MAGIC <div style="text-align: center; padding: 50px 30px; background: linear-gradient(135deg, #1a237e 0%, #0d47a1 50%, #01579b 100%); border-radius: 12px; margin: 10px 0;">
# MAGIC   <h1 style="color: white; font-size: 2.5em; margin-bottom: 10px;">🏗️ Construction Analytics</h1>
# MAGIC   <h2 style="color: #bbdefb; font-size: 1.5em; margin-bottom: 20px;">Project Cost & Performance Control Tower</h2>
# MAGIC   <p style="color: #90caf9; font-size: 1.1em;">End-to-End Intelligence Platform for Finnish Construction</p>
# MAGIC   <hr style="border: 1px solid #42a5f5; width: 50%; margin: 20px auto;">
# MAGIC   <p style="color: #e3f2fd; font-size: 0.9em;">Built on <strong>Databricks Lakehouse</strong> — Bronze → Silver → Gold → ML → Dashboard → Genie</p>
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Step 0 — Initialize Configuration
# ============================================================
# STEP 0: Load project configuration
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #2196F3; background: #e3f2fd; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #1565c0; margin: 0;">⚙️ Step 0: Initializing Configuration</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Setting catalog, schema, and helper functions...</p>
</div>
""")

%run ./_init

# COMMAND ----------

# DBTITLE 1,Step 1 — Bronze to Silver Cleansing
# ============================================================
# STEP 1: Cleanse all 8 Bronze tables → Silver
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #FF9800; background: #fff3e0; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #e65100; margin: 0;">🧹 Step 1: Data Cleansing — Bronze → Silver</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Parsing dates, standardizing enums, deduplicating, validating FKs across 8 tables...</p>
</div>
""")

%run ./01_bronze_to_silver

# COMMAND ----------

# DBTITLE 1,Step 2 — Silver to Gold Aggregates
# ============================================================
# STEP 2: Build 5 Gold aggregate tables
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #4CAF50; background: #e8f5e9; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #2e7d32; margin: 0;">📊 Step 2: Business Metrics — Silver → Gold</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Building cost variance, schedule KPIs, vendor scorecards, invoice aging, project summary...</p>
</div>
""")

%run ./02_silver_to_gold

# COMMAND ----------

# DBTITLE 1,Step 3 — Feature Engineering
# ============================================================
# STEP 3: Build ML feature table
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #9C27B0; background: #f3e5f5; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #6a1b9a; margin: 0;">🧩 Step 3: Feature Engineering</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Assembling cost, schedule, vendor & PO features for ML prediction...</p>
</div>
""")

%run ./03_feature_engineering

# COMMAND ----------

# DBTITLE 1,Step 4 — ML Model Training
# ============================================================
# STEP 4: Train & register cost overrun prediction model
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #F44336; background: #ffebee; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #c62828; margin: 0;">🤖 Step 4: ML Model — Cost Overrun Prediction</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Training LightGBM, logging to MLflow, registering in Unity Catalog, scoring all projects...</p>
</div>
""")

%run ./04_ml_model

# COMMAND ----------

# DBTITLE 1,Step 5 — Data Quality Audit
# ============================================================
# STEP 5: Run data quality audit
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #00BCD4; background: #e0f7fa; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #00838f; margin: 0;">🔍 Step 5: Data Quality Audit</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Validating Bronze vs Silver cleansing results — before/after DQ metrics...</p>
</div>
""")

%run ./07_Data_Quality_Audit

# COMMAND ----------

# DBTITLE 1,Step 6 — Catalog Governance
# ============================================================
# STEP 6: Apply Unity Catalog governance
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #607D8B; background: #eceff1; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #37474f; margin: 0;">🛡️ Step 6: Catalog Governance</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Adding table comments, column descriptions, and governance tags to Unity Catalog...</p>
</div>
""")

%run ./08_UC_Governance_Tags

# COMMAND ----------

# DBTITLE 1,Step 7 — Dashboard & Genie Space
# MAGIC %skip
# MAGIC # ============================================================
# MAGIC # STEP 7: Create dashboard and Genie Space
# MAGIC # ============================================================
# MAGIC displayHTML("""
# MAGIC <div style="padding: 15px; border-left: 5px solid #FF5722; background: #fbe9e7; border-radius: 6px; margin: 10px 0;">
# MAGIC   <h3 style="color: #bf360c; margin: 0;">📱 Step 7: Dashboard & Genie Space</h3>
# MAGIC   <p style="margin: 5px 0 0 0; color: #424242;">Creating Lakeview dashboard and natural-language Genie Space...</p>
# MAGIC </div>
# MAGIC """)
# MAGIC
# MAGIC %run ./05_dashboard_genie
# MAGIC %run ./06_create_genie_space

# COMMAND ----------

# DBTITLE 1,Step 8 — Validation Tests
# ============================================================
# STEP 8: Run pipeline validation tests
# ============================================================
displayHTML("""
<div style="padding: 15px; border-left: 5px solid #795548; background: #efebe9; border-radius: 6px; margin: 10px 0;">
  <h3 style="color: #4e342e; margin: 0;">✅ Step 8: Pipeline Validation</h3>
  <p style="margin: 5px 0 0 0; color: #424242;">Running assertion-based tests on all Bronze, Silver, and Gold tables...</p>
</div>
""")

%run ./99_Pipeline_Validation_Tests

# COMMAND ----------

# DBTITLE 1,Pipeline Complete — Summary
# ============================================================
# PIPELINE COMPLETE — Summary
# ============================================================
from pyspark.sql import functions as F

catalog_name = "construction_demo"

layers = {
    "Bronze": ["dim_employee","dim_projects","dim_vendors","fact_schedule_milestones","fact_project_costs","fact_purchase_orders","fact_invoices","fact_subcontractor_performance"],
    "Silver": ["dim_employee","dim_projects","dim_vendors","fact_schedule_milestones","fact_project_costs","fact_purchase_orders","fact_invoices","fact_subcontractor_performance"],
    "Gold": ["gold_cost_variance_monthly","gold_schedule_kpi","gold_vendor_scorecard","gold_invoice_aging","gold_project_summary","ml_feature_table","ml_project_risk_scores"]
}

table_rows = ""
for layer, tables in layers.items():
    schema = layer.lower()
    for tbl in tables:
        try:
            cnt = spark.table(f"{catalog_name}.{schema}.{tbl}").count()
            color = "#4CAF50" if cnt > 0 else "#F44336"
            table_rows += f'<tr><td style="padding:6px 12px;">{layer}</td><td style="padding:6px 12px;"><code>{catalog_name}.{schema}.{tbl}</code></td><td style="padding:6px 12px; color:{color}; font-weight:bold;">{cnt:,}</td></tr>'
        except:
            table_rows += f'<tr><td style="padding:6px 12px;">{layer}</td><td style="padding:6px 12px;"><code>{catalog_name}.{schema}.{tbl}</code></td><td style="padding:6px 12px; color:#F44336;">NOT FOUND</td></tr>'

displayHTML(f"""
<div style="padding: 30px; background: linear-gradient(135deg, #1b5e20 0%, #2e7d32 100%); border-radius: 12px; margin: 10px 0;">
  <h2 style="color: white; text-align: center;">✅ Construction Analytics Pipeline Complete!</h2>
  <p style="color: #c8e6c9; text-align: center;">All layers processed successfully. Here’s your data inventory:</p>
  <table style="width: 90%; margin: 20px auto; background: white; border-radius: 8px; border-collapse: collapse;">
    <tr style="background: #e8f5e9;"><th style="padding:10px 12px;">Layer</th><th style="padding:10px 12px;">Table</th><th style="padding:10px 12px;">Rows</th></tr>
    {table_rows}
  </table>
</div>
""")
