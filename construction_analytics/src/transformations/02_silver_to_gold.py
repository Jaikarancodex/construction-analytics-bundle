# Databricks notebook source
# DBTITLE 1,Header — Silver to Gold
# MAGIC %md
# MAGIC    
# MAGIC # 02 — Silver to Gold: Business-Ready Aggregates
# MAGIC ## Construction Analytics — `construction_demo.silver` → `construction_demo.gold`
# MAGIC
# MAGIC Builds **6 Gold tables** for dashboards, ML, and Genie Space:
# MAGIC
# MAGIC | Gold Table | Purpose |
# MAGIC |---|---|
# MAGIC | `gold_cost_variance_monthly` | Budget vs actual by project, category, month |
# MAGIC | `gold_schedule_kpi` | Milestone completion rates by project & takt zone |
# MAGIC | `gold_vendor_scorecard` | Composite vendor performance scores |
# MAGIC | `gold_invoice_aging` | Invoice aging buckets & overdue analysis |
# MAGIC | `gold_project_summary` | Holistic project health with risk scoring |
# MAGIC | `gold_employee_purchase_summary` | Purchase order metrics per employee (project manager) |

# COMMAND ----------

# DBTITLE 1,Setup
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_name = "construction_demo"
schema_silver = "silver"
schema_gold = "gold"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_gold}")

print("✅ Gold schema ready")

# COMMAND ----------

# DBTITLE 1,Gold: cost_variance_monthly
# ============================================================
# Gold: Monthly cost variance by project, category
# ============================================================
df_cost = spark.table(f"{catalog_name}.{schema_silver}.fact_project_costs")
df_proj = spark.table(f"{catalog_name}.{schema_silver}.dim_projects")

df_cost_gold = (df_cost
    .withColumn("period_year_month", F.date_format(F.col("period_month_dt"), "yyyy-MM"))
    .groupBy("project_id", "cost_category", "period_year_month")
    .agg(
        F.sum("budget_eur").alias("total_budget_eur"),
        F.sum("committed_eur").alias("total_committed_eur"),
        F.sum("actual_eur").alias("total_actual_eur"),
        F.sum("variance_eur").alias("total_variance_eur"),
        F.count("*").alias("line_count"),
    )
    .withColumn("budget_utilization_pct",
        F.round(F.col("total_actual_eur") / F.col("total_budget_eur") * 100, 2))
    .withColumn("is_over_budget",
        F.when(F.col("total_variance_eur") < 0, F.lit(True)).otherwise(F.lit(False)))
)

# Enrich with project name & type
df_cost_gold = df_cost_gold.join(
    df_proj.select("project_id", "project_name", "project_type", "municipality"),
    on="project_id", how="left")

df_cost_gold.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_cost_variance_monthly")
print(f"✅ gold.gold_cost_variance_monthly: {df_cost_gold.count()} rows")
display(df_cost_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: schedule_kpi
# ============================================================
# Gold: Schedule KPI — milestone hit rates by project & takt zone
# ============================================================
df_ms = spark.table(f"{catalog_name}.{schema_silver}.fact_schedule_milestones")
df_proj = spark.table(f"{catalog_name}.{schema_silver}.dim_projects")

df_sched = (df_ms
    .groupBy("project_id", "takt_zone")
    .agg(
        F.count("*").alias("total_milestones"),
        F.sum(F.when(F.col("milestone_status") == "complete", 1).otherwise(0)).alias("completed_milestones"),
        F.sum(F.when(F.col("milestone_status") == "delayed", 1).otherwise(0)).alias("delayed_milestones"),
        F.sum(F.when(F.col("milestone_status") == "on_track", 1).otherwise(0)).alias("on_track_milestones"),
        F.avg(F.when(F.col("delay_days").isNotNull(), F.col("delay_days"))).alias("avg_delay_days"),
        F.max(F.when(F.col("delay_days").isNotNull(), F.col("delay_days"))).alias("max_delay_days"),
    )
    .withColumn("completion_rate_pct",
        F.round(F.col("completed_milestones") / F.col("total_milestones") * 100, 2))
    .withColumn("delay_rate_pct",
        F.round(F.col("delayed_milestones") / F.col("total_milestones") * 100, 2))
    .withColumn("avg_delay_days", F.round(F.col("avg_delay_days"), 1))
)

df_sched = df_sched.join(
    df_proj.select("project_id", "project_name", "project_type", "municipality", "status"),
    on="project_id", how="left")

df_sched.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_schedule_kpi")
print(f"✅ gold.gold_schedule_kpi: {df_sched.count()} rows")
display(df_sched.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: vendor_scorecard
# ============================================================
# Gold: Vendor Scorecard — composite performance + invoice behavior
# ============================================================
df_perf = spark.table(f"{catalog_name}.{schema_silver}.fact_subcontractor_performance")
df_vnd = spark.table(f"{catalog_name}.{schema_silver}.dim_vendors")
df_inv = spark.table(f"{catalog_name}.{schema_silver}.fact_invoices")
df_po = spark.table(f"{catalog_name}.{schema_silver}.fact_purchase_orders")

# Performance aggregates per vendor
df_perf_agg = (df_perf
    .groupBy("vendor_id")
    .agg(
        F.avg("quality_score").alias("avg_quality_score"),
        F.avg("schedule_score").alias("avg_schedule_score"),
        F.avg("safety_score").alias("avg_safety_score"),
        F.avg("communication_score").alias("avg_communication_score"),
        F.avg("overall_score").alias("avg_overall_score"),
        F.count("*").alias("evaluation_count"),
        F.sum(F.when(F.col("would_rehire") == True, 1).otherwise(0)).alias("rehire_yes_count"),
    )
    .withColumn("rehire_rate_pct",
        F.round(F.col("rehire_yes_count") / F.col("evaluation_count") * 100, 2)))

# Invoice behavior per vendor
df_inv_agg = (df_inv
    .groupBy("vendor_id")
    .agg(
        F.count("*").alias("total_invoices"),
        F.sum("amount_eur").alias("total_invoice_amount"),
        F.avg("days_overdue").alias("avg_days_overdue"),
        F.sum(F.when(F.col("payment_status") == "disputed", 1).otherwise(0)).alias("disputed_invoices"),
    ))

# PO over-invoicing per vendor
df_po_agg = (df_po
    .groupBy("vendor_id")
    .agg(
        F.count("*").alias("total_pos"),
        F.sum(F.when(F.col("is_over_invoiced") == True, 1).otherwise(0)).alias("over_invoiced_pos"),
    ))

# Join everything
df_vendor_score = (df_vnd
    .join(df_perf_agg, on="vendor_id", how="left")
    .join(df_inv_agg, on="vendor_id", how="left")
    .join(df_po_agg, on="vendor_id", how="left"))

# Round scores
for c in ["avg_quality_score", "avg_schedule_score", "avg_safety_score", "avg_communication_score", "avg_overall_score"]:
    df_vendor_score = df_vendor_score.withColumn(c, F.round(F.col(c), 2))

df_vendor_score.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_vendor_scorecard")
print(f"✅ gold.gold_vendor_scorecard: {df_vendor_score.count()} rows")
display(df_vendor_score.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: invoice_aging
# ============================================================
# Gold: Invoice Aging — aging buckets & overdue analysis
# ============================================================
df_inv = spark.table(f"{catalog_name}.{schema_silver}.fact_invoices")
df_vnd = spark.table(f"{catalog_name}.{schema_silver}.dim_vendors")

df_aging = (df_inv
    .withColumn("aging_bucket",
        F.when(F.col("days_overdue") <= 0, "current")
         .when(F.col("days_overdue") <= 30, "1-30 days")
         .when(F.col("days_overdue") <= 60, "31-60 days")
         .when(F.col("days_overdue") <= 90, "61-90 days")
         .otherwise("90+ days"))
    .groupBy("vendor_id", "payment_status", "aging_bucket")
    .agg(
        F.count("*").alias("invoice_count"),
        F.sum("amount_eur").alias("total_amount_eur"),
        F.avg("days_overdue").alias("avg_days_overdue"),
        F.max("days_overdue").alias("max_days_overdue"),
    )
    .withColumn("avg_days_overdue", F.round(F.col("avg_days_overdue"), 1))
)

df_aging = df_aging.join(
    df_vnd.select("vendor_id", "vendor_name", "vendor_type"),
    on="vendor_id", how="left")

df_aging.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_invoice_aging")
print(f"✅ gold.gold_invoice_aging: {df_aging.count()} rows")
display(df_aging.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: project_summary
# ============================================================
# Gold: Project Summary — holistic project health
# ============================================================
df_proj = spark.table(f"{catalog_name}.{schema_silver}.dim_projects")
df_cost = spark.table(f"{catalog_name}.{schema_silver}.fact_project_costs")
df_ms = spark.table(f"{catalog_name}.{schema_silver}.fact_schedule_milestones")
df_po = spark.table(f"{catalog_name}.{schema_silver}.fact_purchase_orders")

# Cost summary per project
df_cost_sum = (df_cost.groupBy("project_id").agg(
    F.sum("budget_eur").alias("total_budget_eur"),
    F.sum("actual_eur").alias("total_actual_eur"),
    F.sum("variance_eur").alias("total_variance_eur"),
))

# Schedule summary per project
df_ms_sum = (df_ms.groupBy("project_id").agg(
    F.count("*").alias("total_milestones"),
    F.sum(F.when(F.col("milestone_status") == "complete", 1).otherwise(0)).alias("completed_milestones"),
    F.sum(F.when(F.col("milestone_status") == "delayed", 1).otherwise(0)).alias("delayed_milestones"),
    F.avg(F.when(F.col("delay_days").isNotNull(), F.col("delay_days"))).alias("avg_delay_days"),
))

# PO summary per project
df_po_sum = (df_po.groupBy("project_id").agg(
    F.sum("po_value_eur").alias("total_po_value_eur"),
    F.sum("invoiced_eur").alias("total_invoiced_eur"),
    F.sum(F.when(F.col("is_over_invoiced") == True, 1).otherwise(0)).alias("over_invoiced_pos"),
))

# Join all
df_summary = (df_proj
    .join(df_cost_sum, on="project_id", how="left")
    .join(df_ms_sum, on="project_id", how="left")
    .join(df_po_sum, on="project_id", how="left")
    .withColumn("budget_utilization_pct",
        F.round(F.col("total_actual_eur") / F.col("total_budget_eur") * 100, 2))
    .withColumn("milestone_completion_pct",
        F.round(F.col("completed_milestones") / F.col("total_milestones") * 100, 2))
    .withColumn("avg_delay_days", F.round(F.col("avg_delay_days"), 1))
    # Risk score: higher = riskier
    .withColumn("risk_score",
        F.round(
            F.coalesce(F.when(F.col("budget_utilization_pct") > 100,
                (F.col("budget_utilization_pct") - 100) * 0.5).otherwise(F.lit(0)), F.lit(0)) +
            F.coalesce(F.col("delayed_milestones") * 5, F.lit(0)) +
            F.coalesce(F.col("over_invoiced_pos") * 3, F.lit(0)) +
            F.coalesce(F.when(F.col("avg_delay_days") > 0, F.col("avg_delay_days") * 0.5).otherwise(F.lit(0)), F.lit(0)),
        2))
    .withColumn("risk_level",
        F.when(F.col("risk_score") > 50, "high")
         .when(F.col("risk_score") > 20, "medium")
         .otherwise("low"))
)

df_summary.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_project_summary")
print(f"✅ gold.gold_project_summary: {df_summary.count()} rows")
display(df_summary.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold: employee_purchase_summary
# ============================================================
# Gold: Employee Purchase Summary — PO metrics per project manager
# ============================================================
df_emp = spark.table(f"{catalog_name}.{schema_silver}.dim_employee")
df_proj = spark.table(f"{catalog_name}.{schema_silver}.dim_projects")
df_po = spark.table(f"{catalog_name}.{schema_silver}.fact_purchase_orders")

# Join employees (as project managers) → projects → purchase orders
df_emp_po = (df_emp
    .join(df_proj, df_proj["project_manager_id"] == df_emp["employee_id"], "inner")
    .join(df_po, df_po["project_id"] == df_proj["project_id"], "inner")
)

df_emp_gold = (df_emp_po
    .groupBy(
        df_emp["employee_id"],
        df_emp["full_name"],
        df_emp["role"],
        df_emp["department"],
    )
    .agg(
        F.countDistinct("po_id").alias("total_po_count"),
        F.round(F.sum("po_value_eur"), 2).alias("total_po_value_eur"),
        F.round(F.sum("invoiced_eur"), 2).alias("total_invoiced_eur"),
        F.sum(F.when(F.col("is_over_invoiced") == True, 1).otherwise(0)).alias("over_invoiced_count"),
        F.round(
            F.sum(F.when(F.col("is_over_invoiced") == True, 1).otherwise(0)) * 100.0
            / F.countDistinct("po_id"), 2
        ).alias("over_invoiced_pct"),
        F.countDistinct(df_proj["project_id"]).alias("project_count"),
        F.round(F.avg("po_value_eur"), 2).alias("avg_po_value_eur"),
    )
)

df_emp_gold.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.gold_employee_purchase_summary")

cnt = spark.table(f"{catalog_name}.{schema_gold}.gold_employee_purchase_summary").count()
print(f"✅ gold.gold_employee_purchase_summary: {cnt} rows")
display(df_emp_gold.limit(5))

# COMMAND ----------

# DBTITLE 1,Gold Layer Verification
# ============================================================
# Gold layer verification
# ============================================================
gold_tables = [
    "gold_cost_variance_monthly", "gold_schedule_kpi",
    "gold_vendor_scorecard", "gold_invoice_aging", "gold_project_summary",
    "gold_employee_purchase_summary"
]

print("=" * 70)
print("GOLD LAYER VERIFICATION")
print("=" * 70)

for tbl in gold_tables:
    fqn = f"{catalog_name}.{schema_gold}.{tbl}"
    cnt = spark.table(fqn).count()
    cols = len(spark.table(fqn).columns)
    print(f"  {fqn}: {cnt} rows, {cols} columns")

print("\n✅ All 6 gold tables created and verified")
