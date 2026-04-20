# Databricks notebook source
# DBTITLE 1,Header — Unity Catalog Governance
# MAGIC %md
# MAGIC # 08 — Unity Catalog Governance & Tags
# MAGIC ## Table Comments, Column Descriptions, Classification Tags
# MAGIC
# MAGIC Applies governance metadata to all Silver & Gold tables so that:
# MAGIC * **Catalog Explorer** shows rich descriptions for discovery
# MAGIC * **Genie Space** understands column semantics for better answers
# MAGIC * **Data stewards** can classify PII and sensitive columns
# MAGIC * **Compliance teams** can audit access and lineage

# COMMAND ----------

# DBTITLE 1,Gold Table Comments
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- TABLE-LEVEL COMMENTS — Gold Layer
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.gold.gold_cost_variance_monthly IS
# MAGIC   'Monthly cost variance aggregated by project and cost category. Tracks budget vs actual spend with utilization percentages. Use for cost trend dashboards and overrun detection.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.gold.gold_schedule_kpi IS
# MAGIC   'Schedule performance KPIs by project and takt zone. Includes milestone completion rates, delay metrics, and on-track percentages. Use for schedule adherence monitoring.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.gold.gold_vendor_scorecard IS
# MAGIC   'Composite vendor performance scorecard combining quality, schedule, safety, and communication scores with invoice behavior metrics. Use for vendor evaluation and procurement decisions.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.gold.gold_invoice_aging IS
# MAGIC   'Invoice aging analysis bucketed into current, 1-30, 31-60, 61-90, and 90+ day categories. Includes payment status distribution and overdue amounts per vendor.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.gold.gold_project_summary IS
# MAGIC   'Holistic project health dashboard combining budget utilization, milestone completion, delay metrics, and computed risk scores (low/medium/high). Primary table for executive reporting.';

# COMMAND ----------

# DBTITLE 1,Silver Table Comments
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- TABLE-LEVEL COMMENTS — Silver Layer
# MAGIC -- ============================================================
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.dim_employee IS
# MAGIC   'Cleansed employee dimension. Roles and departments standardized to lowercase valid enums. Null PKs removed, duplicates resolved. Source: construction_demo.bronze.dim_employee.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.dim_projects IS
# MAGIC   'Cleansed project dimension. Dates parsed from mixed string formats, enums standardized, negative values clamped, invalid FK references nullified. Source: construction_demo.bronze.dim_projects.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.dim_vendors IS
# MAGIC   'Cleansed vendor dimension. Boolean rala_certified parsed from string, Finnish Y-tunnus validated, enums standardized. Source: construction_demo.bronze.dim_vendors.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.fact_invoices IS
# MAGIC   'Cleansed invoice fact table. Dates parsed, payment_status standardized, days_overdue recalculated, logical errors fixed (paid_date < invoice_date). Source: construction_demo.bronze.fact_invoices.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.fact_project_costs IS
# MAGIC   'Cleansed project costs. Negative values clamped, variance_eur recalculated as budget-actual, cost_category standardized. Source: construction_demo.bronze.fact_project_costs.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.fact_purchase_orders IS
# MAGIC   'Cleansed purchase orders. Over-invoicing flag added, negative values clamped, enums standardized, FK references validated. Source: construction_demo.bronze.fact_purchase_orders.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.fact_schedule_milestones IS
# MAGIC   'Cleansed schedule milestones. delay_days recalculated from actual-planned dates, status standardized, logical errors corrected. Source: construction_demo.bronze.fact_schedule_milestones.';
# MAGIC
# MAGIC COMMENT ON TABLE construction_demo.silver.fact_subcontractor_performance IS
# MAGIC   'Cleansed subcontractor performance. Scores clamped to 1-10, overall_score recalculated (Q*0.4+S*0.3+Sf*0.2+C*0.1), would_rehire parsed from string. Source: construction_demo.bronze.fact_subcontractor_performance.';

# COMMAND ----------

# DBTITLE 1,Column Descriptions — Key Gold Tables
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- COLUMN-LEVEL COMMENTS — Gold Project Summary (most used table)
# MAGIC -- ============================================================
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_project_summary
# MAGIC   ALTER COLUMN project_id COMMENT 'Unique project identifier (format: FRA-YYYY-NNNN)';
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_project_summary
# MAGIC   ALTER COLUMN budget_utilization_pct COMMENT 'Percentage of budget consumed (actual/budget*100). Values >100 indicate overrun.';
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_project_summary
# MAGIC   ALTER COLUMN milestone_completion_pct COMMENT 'Percentage of milestones marked complete out of total milestones.';
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_project_summary
# MAGIC   ALTER COLUMN risk_score COMMENT 'Composite risk score based on budget overrun, delayed milestones, and over-invoiced POs. Higher = riskier.';
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_project_summary
# MAGIC   ALTER COLUMN risk_level COMMENT 'Categorized risk: low (<20), medium (20-50), high (>50). Drives escalation workflows.';
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- COLUMN-LEVEL COMMENTS — Gold Vendor Scorecard
# MAGIC -- ============================================================
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_vendor_scorecard
# MAGIC   ALTER COLUMN avg_overall_score COMMENT 'Weighted average: quality*0.4 + schedule*0.3 + safety*0.2 + communication*0.1. Scale 1-10.';
# MAGIC
# MAGIC ALTER TABLE construction_demo.gold.gold_vendor_scorecard
# MAGIC   ALTER COLUMN rehire_rate_pct COMMENT 'Percentage of evaluations where the vendor was recommended for rehire.';

# COMMAND ----------

# DBTITLE 1,Governance Tags — Sensitivity Classification
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GOVERNANCE TAGS — Data Classification
# MAGIC -- ============================================================
# MAGIC -- NOTE: Tags require Unity Catalog governance features.
# MAGIC -- These commands add sensitivity classification to key columns.
# MAGIC -- Uncomment and run if your workspace supports tags.
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- PII Tags
# MAGIC -- ALTER TABLE construction_demo.silver.dim_employee ALTER COLUMN full_name SET TAGS ('pii' = 'name');
# MAGIC -- ALTER TABLE construction_demo.silver.dim_vendors ALTER COLUMN business_id SET TAGS ('pii' = 'business_id', 'sensitivity' = 'high');
# MAGIC -- ALTER TABLE construction_demo.silver.dim_projects ALTER COLUMN site_address SET TAGS ('pii' = 'address');
# MAGIC
# MAGIC -- Financial Sensitivity
# MAGIC -- ALTER TABLE construction_demo.silver.dim_projects ALTER COLUMN contract_value_eur SET TAGS ('sensitivity' = 'high', 'domain' = 'finance');
# MAGIC -- ALTER TABLE construction_demo.silver.fact_invoices ALTER COLUMN amount_eur SET TAGS ('sensitivity' = 'high', 'domain' = 'finance');
# MAGIC -- ALTER TABLE construction_demo.silver.fact_project_costs ALTER COLUMN budget_eur SET TAGS ('sensitivity' = 'high', 'domain' = 'finance');
# MAGIC
# MAGIC -- Domain Tags
# MAGIC -- ALTER TABLE construction_demo.gold.gold_project_summary SET TAGS ('domain' = 'construction', 'layer' = 'gold', 'refresh' = 'daily');
# MAGIC -- ALTER TABLE construction_demo.gold.gold_vendor_scorecard SET TAGS ('domain' = 'procurement', 'layer' = 'gold', 'refresh' = 'daily');
# MAGIC
# MAGIC SELECT '✅ Governance tags ready (uncomment ALTER TAG statements to apply)' AS status;

# COMMAND ----------

# DBTITLE 1,Governance Summary
# ============================================================
# Summary: show all tables with their comments
# ============================================================
from pyspark.sql import functions as F

catalog_name = "construction_demo"

for schema in ["silver", "gold"]:
    print(f"\n{'='*60}")
    print(f"  {catalog_name}.{schema} — Table Comments")
    print(f"{'='*60}")
    tables = spark.sql(f"SHOW TABLES IN {catalog_name}.{schema}").collect()
    for row in tables:
        tbl = row["tableName"]
        try:
            desc = spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{schema}.{tbl}") \
                .filter("col_name = 'Comment'").select("data_type").collect()
            comment = desc[0][0] if desc else "(no comment)"
            print(f"  📌 {tbl}: {comment[:80]}..." if len(str(comment)) > 80 else f"  📌 {tbl}: {comment}")
        except:
            print(f"  ⚠️ {tbl}: could not read")

print("\n✅ Governance metadata applied")
