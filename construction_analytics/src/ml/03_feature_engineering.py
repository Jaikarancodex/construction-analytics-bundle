# Databricks notebook source
# DBTITLE 1,Header — Feature Engineering
# MAGIC %md
# MAGIC # 03 — Feature Engineering
# MAGIC ## Construction Analytics — ML Feature Table for Cost Overrun Prediction
# MAGIC
# MAGIC Builds a feature table by joining silver tables and engineering predictive features:
# MAGIC * Project characteristics (type, size, duration, municipality)
# MAGIC * Cost behavior (budget utilization, variance trend)
# MAGIC * Schedule health (milestone completion rate, avg delays)
# MAGIC * Vendor reliability (avg scores, over-invoicing rate)
# MAGIC * Target: `is_over_budget` (binary — actual > budget by 10%+)

# COMMAND ----------

# DBTITLE 1,Setup & Load Silver Tables
from pyspark.sql import functions as F
from pyspark.sql.window import Window

catalog_name = "construction_demo"
schema_silver = "silver"
schema_gold = "gold"

df_proj = spark.table(f"{catalog_name}.{schema_silver}.dim_projects")
df_cost = spark.table(f"{catalog_name}.{schema_silver}.fact_project_costs")
df_ms = spark.table(f"{catalog_name}.{schema_silver}.fact_schedule_milestones")
df_po = spark.table(f"{catalog_name}.{schema_silver}.fact_purchase_orders")
df_perf = spark.table(f"{catalog_name}.{schema_silver}.fact_subcontractor_performance")
df_inv = spark.table(f"{catalog_name}.{schema_silver}.fact_invoices")

print("✅ Silver tables loaded")

# COMMAND ----------

# DBTITLE 1,Cost Features per Project
# ============================================================
# Cost features: budget utilization, variance ratios
# ============================================================
df_cost_feat = (df_cost
    .groupBy("project_id")
    .agg(
        F.sum("budget_eur").alias("total_budget"),
        F.sum("actual_eur").alias("total_actual"),
        F.sum("committed_eur").alias("total_committed"),
        F.sum("variance_eur").alias("total_variance"),
        F.count("*").alias("cost_line_count"),
        F.countDistinct("cost_category").alias("distinct_cost_categories"),
        F.sum(F.when(F.col("variance_eur") < 0, 1).otherwise(0)).alias("overrun_line_count"),
    )
    .withColumn("budget_utilization_pct", F.round(F.col("total_actual") / F.col("total_budget") * 100, 2))
    .withColumn("committed_ratio", F.round(F.col("total_committed") / F.col("total_budget"), 4))
    .withColumn("overrun_line_pct", F.round(F.col("overrun_line_count") / F.col("cost_line_count") * 100, 2))
    # TARGET: over budget by 10%+
    .withColumn("is_over_budget", F.when(F.col("total_actual") > F.col("total_budget") * 1.10, 1).otherwise(0))
)

print(f"Cost features: {df_cost_feat.count()} projects")
display(df_cost_feat.limit(5))

# COMMAND ----------

# DBTITLE 1,Schedule Features per Project
# ============================================================
# Schedule features: milestone hit rate, delay patterns
# ============================================================
df_ms_feat = (df_ms
    .groupBy("project_id")
    .agg(
        F.count("*").alias("total_milestones"),
        F.sum(F.when(F.col("milestone_status") == "complete", 1).otherwise(0)).alias("completed_ms"),
        F.sum(F.when(F.col("milestone_status") == "delayed", 1).otherwise(0)).alias("delayed_ms"),
        F.avg(F.when(F.col("delay_days").isNotNull(), F.col("delay_days"))).alias("avg_delay_days"),
        F.max(F.when(F.col("delay_days").isNotNull(), F.col("delay_days"))).alias("max_delay_days"),
        F.countDistinct("takt_zone").alias("distinct_takt_zones"),
    )
    .withColumn("ms_completion_rate", F.round(F.col("completed_ms") / F.col("total_milestones"), 4))
    .withColumn("ms_delay_rate", F.round(F.col("delayed_ms") / F.col("total_milestones"), 4))
    .withColumn("avg_delay_days", F.round(F.col("avg_delay_days"), 2))
)

print(f"Schedule features: {df_ms_feat.count()} projects")

# COMMAND ----------

# DBTITLE 1,Vendor & PO Features per Project
# ============================================================
# Vendor performance features per project
# ============================================================
df_perf_feat = (df_perf
    .groupBy("project_id")
    .agg(
        F.avg("overall_score").alias("avg_vendor_score"),
        F.min("overall_score").alias("min_vendor_score"),
        F.countDistinct("vendor_id").alias("distinct_vendors"),
        F.sum(F.when(F.col("would_rehire") == False, 1).otherwise(0)).alias("no_rehire_count"),
    )
    .withColumn("avg_vendor_score", F.round(F.col("avg_vendor_score"), 2))
    .withColumn("min_vendor_score", F.round(F.col("min_vendor_score"), 2))
)

# PO features per project
df_po_feat = (df_po
    .groupBy("project_id")
    .agg(
        F.sum("po_value_eur").alias("total_po_value"),
        F.sum("invoiced_eur").alias("total_invoiced_from_po"),
        F.count("*").alias("total_pos"),
        F.sum(F.when(F.col("is_over_invoiced") == True, 1).otherwise(0)).alias("over_invoiced_pos"),
        F.countDistinct("vendor_id").alias("distinct_po_vendors"),
    )
    .withColumn("over_invoice_rate", F.round(F.col("over_invoiced_pos") / F.col("total_pos"), 4))
)

print(f"Vendor features: {df_perf_feat.count()} projects")
print(f"PO features: {df_po_feat.count()} projects")

# COMMAND ----------

# DBTITLE 1,Assemble Final Feature Table
# ============================================================
# Join all features into a single ML feature table
# ============================================================
from pyspark.ml.feature import StringIndexer

# Project base features
df_proj_feat = (df_proj
    .select(
        "project_id", "project_type", "municipality", "status",
        "contract_value_eur", "floor_area_m2",
        "start_date_dt", "planned_end_date_dt",
    )
    .withColumn("planned_duration_days",
        F.datediff(F.col("planned_end_date_dt"), F.col("start_date_dt")))
    .drop("start_date_dt", "planned_end_date_dt")
)

# Assemble
df_features = (df_proj_feat
    .join(df_cost_feat, on="project_id", how="inner")  # must have cost data
    .join(df_ms_feat, on="project_id", how="left")
    .join(df_perf_feat, on="project_id", how="left")
    .join(df_po_feat, on="project_id", how="left")
)

# Fill nulls for numeric features
numeric_cols = [c for c, t in df_features.dtypes if t in ("double", "int", "bigint", "long") and c != "is_over_budget"]
for c in numeric_cols:
    df_features = df_features.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

df_features = df_features.withColumn("ingestion_timestamp", F.current_timestamp())

df_features.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_gold}.ml_feature_table")

print(f"✅ Feature table: {df_features.count()} rows, {len(df_features.columns)} columns")
print(f"   Target distribution: {df_features.groupBy('is_over_budget').count().collect()}")
display(df_features.limit(10))
