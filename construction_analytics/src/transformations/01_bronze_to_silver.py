# Databricks notebook source
# DBTITLE 1,Header — Bronze to Silver
# MAGIC %md
# MAGIC # 01 — Bronze to Silver: Data Cleansing & Enrichment
# MAGIC ## Construction Analytics — `construction_demo.bronze` → `construction_demo.silver`
# MAGIC
# MAGIC Cleanses all **8 bronze tables** and writes to the **silver** schema:
# MAGIC
# MAGIC | Cleansing Rule | Tables Affected |
# MAGIC |---|---|
# MAGIC | Parse messy dates (5 formats) | dim_projects, fact_schedule_milestones, fact_project_costs, fact_purchase_orders, fact_invoices, fact_subcontractor_performance |
# MAGIC | Standardize enums to lowercase | All 8 tables |
# MAGIC | Remove duplicate PKs | All 8 tables |
# MAGIC | Filter null PKs | All 8 tables |
# MAGIC | Clamp negative values | dim_projects, fact_project_costs, fact_purchase_orders, fact_invoices |
# MAGIC | Parse boolean strings | dim_vendors |
# MAGIC | Validate Finnish Y-tunnus | dim_vendors |
# MAGIC | Recalculate derived fields | fact_project_costs (variance), fact_schedule_milestones (delay_days), fact_subcontractor_performance (overall_score) |

# COMMAND ----------

# DBTITLE 1,Setup — Run _init and imports
# %run ./_init

from pyspark.sql import functions as F
from pyspark.sql.types import DateType, BooleanType, DoubleType, IntegerType
from pyspark.sql.window import Window

catalog_name = "construction_demo"
schema_bronze = "bronze"
schema_silver = "silver"

spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_silver}")

# ---- Reusable date parsing function (uses try_to_date for ANSI mode) ----
def parse_messy_dates(df, *date_cols):
    date_formats = ["yyyy-MM-dd", "dd/MM/yyyy", "yyyyMMdd", "dd-MMM-yyyy", "MM-dd-yyyy"]
    for col_name in date_cols:
        new_col = col_name + "_dt"
        parsed = F.lit(None).cast(DateType())
        for fmt in date_formats:
            parsed = F.coalesce(parsed, F.expr(f"try_to_date(`{col_name}`, '{fmt}')"))
        df = df.withColumn(new_col, parsed)
    return df

# ---- Reusable dedup + null PK filter ----
def clean_pks(df, pk_col):
    """Remove null PKs and keep first occurrence of duplicates."""
    df = df.filter(F.col(pk_col).isNotNull())
    w = Window.partitionBy(pk_col).orderBy(F.monotonically_increasing_id())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    return df

print("✅ Setup complete (using try_to_date for ANSI compatibility)")

# COMMAND ----------

# DBTITLE 1,Silver: dim_employee
# ============================================================
# dim_employee: standardize enums, remove nulls/dups
# ============================================================
df_emp = spark.table(f"{catalog_name}.{schema_bronze}.dim_employee")

# Standardize enums
role_map = {
    "site_manager": "site_manager", "site  manager": "site_manager", "mgr": "site_manager",
    "project manager": "project_manager", "foreman": "foreman",
    "safety_officer": "safety_officer", "quantity_surveyor": "quantity_surveyor",
    "design_manager": "design_manager", "procurement_officer": "procurement_officer",
    "site_engineer": "site_engineer", "planner": "planner", "bim_coordinator": "bim_coordinator",
}
dept_map = {
    "construction": "construction", "design": "design", "procurement": "procurement",
    "safety": "safety", "planning": "planning", "management": "management", "finance": "finance",
    "sfaety": "safety",
    "unknown": "unassigned", "n/a": "unassigned", "": "unassigned",
}

valid_roles = list(set(role_map.values()))
valid_depts = list(set(dept_map.values()))

df_emp = df_emp.withColumn("role", F.lower(F.trim(F.col("role"))))
df_emp = df_emp.withColumn("department", F.lower(F.trim(F.col("department"))))

# Map known messy values
role_expr = F.col("role")
for messy, clean in role_map.items():
    role_expr = F.when(F.col("role") == messy, F.lit(clean)).otherwise(role_expr)
df_emp = df_emp.withColumn("role_clean", role_expr)
df_emp = df_emp.withColumn("role_clean",
    F.when(F.col("role_clean").isin(valid_roles), F.col("role_clean")).otherwise(F.lit(None)))

dept_expr = F.col("department")
for messy, clean in dept_map.items():
    dept_expr = F.when(F.col("department") == messy, F.lit(clean)).otherwise(dept_expr)
df_emp = df_emp.withColumn("department_clean", dept_expr)
df_emp = df_emp.withColumn("department_clean",
    F.when(F.col("department_clean").isin(valid_depts), F.col("department_clean")).otherwise(F.lit(None)))

df_emp = df_emp.drop("role", "department").withColumnRenamed("role_clean", "role").withColumnRenamed("department_clean", "department")
df_emp = clean_pks(df_emp, "employee_id")
df_emp = df_emp.withColumn("ingestion_timestamp", F.current_timestamp())

df_emp.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.dim_employee")
print(f"✅ silver.dim_employee: {df_emp.count()} rows")
display(df_emp.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: dim_projects
# ============================================================
# dim_projects: parse dates, standardize enums, clamp negatives, validate FKs
# ============================================================
df_proj = spark.table(f"{catalog_name}.{schema_bronze}.dim_projects")

# Parse messy dates
df_proj = parse_messy_dates(df_proj, "start_date", "planned_end_date", "actual_end_date")

# Standardize enums
type_map = {"residential": "residential", "commercial": "commercial", "renovation": "renovation",
            "reno": "renovation", "pipe_renovation": "pipe_renovation", "pipe reno": "pipe_renovation",
            "lifecycle": "lifecycle", "data_center": "data_center", "datacenter": "data_center"}
status_map = {"active": "active", "actve": "active", "completed": "completed", "complted": "completed",
              "tendering": "tendering", "on_hold": "on_hold", "on hold": "on_hold", "pending": "on_hold",
              "cancelled": "cancelled", "canceled": "cancelled"}
muni_map = {"hki": "HKI", "helsinki": "HKI", "esp": "ESP", "espoo": "ESP",
            "van": "VAN", "vantaa": "VAN", "hyv": "HYV", "hyvink\u00e4\u00e4": "HYV"}

df_proj = df_proj.withColumn("project_type", F.lower(F.trim(F.col("project_type"))))
type_expr = F.col("project_type")
for m, c in type_map.items():
    type_expr = F.when(F.col("project_type") == m, F.lit(c)).otherwise(type_expr)
df_proj = df_proj.withColumn("project_type", F.when(type_expr.isin(list(set(type_map.values()))), type_expr).otherwise(F.lit(None)))

df_proj = df_proj.withColumn("status", F.lower(F.trim(F.col("status"))))
status_expr = F.col("status")
for m, c in status_map.items():
    status_expr = F.when(F.col("status") == m, F.lit(c)).otherwise(status_expr)
df_proj = df_proj.withColumn("status", F.when(status_expr.isin(list(set(status_map.values()))), status_expr).otherwise(F.lit(None)))

df_proj = df_proj.withColumn("municipality", F.lower(F.trim(F.col("municipality"))))
muni_expr = F.col("municipality")
for m, c in muni_map.items():
    muni_expr = F.when(F.col("municipality") == m, F.lit(c)).otherwise(muni_expr)
df_proj = df_proj.withColumn("municipality", muni_expr)

# Clamp negatives
df_proj = df_proj.withColumn("contract_value_eur", F.abs(F.col("contract_value_eur")))
df_proj = df_proj.withColumn("floor_area_m2", F.abs(F.col("floor_area_m2")))

# Validate FK: project_manager_id must match EMP-NNNN pattern
df_proj = df_proj.withColumn("project_manager_id",
    F.when(F.col("project_manager_id").rlike("^EMP-\\d{4}$"), F.col("project_manager_id")).otherwise(F.lit(None)))
df_proj = df_proj.withColumn("client_id",
    F.when(F.col("client_id").rlike("^CLI-\\d{3}$"), F.col("client_id")).otherwise(F.lit(None)))

# Fix logical errors: actual_end before start
df_proj = df_proj.withColumn("actual_end_date_dt",
    F.when(F.col("actual_end_date_dt") < F.col("start_date_dt"), F.lit(None)).otherwise(F.col("actual_end_date_dt")))
# Null actual_end for active projects
df_proj = df_proj.withColumn("actual_end_date_dt",
    F.when(F.col("status").isin("active", "tendering", "on_hold"), F.lit(None)).otherwise(F.col("actual_end_date_dt")))

df_proj = clean_pks(df_proj, "project_id")
df_proj = df_proj.drop("start_date", "planned_end_date", "actual_end_date")
df_proj = df_proj.withColumn("ingestion_timestamp", F.current_timestamp())

df_proj.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.dim_projects")
print(f"✅ silver.dim_projects: {df_proj.count()} rows")
display(df_proj.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: dim_vendors
# ============================================================
# dim_vendors: parse booleans, validate Y-tunnus, standardize enums
# ============================================================
df_vnd = spark.table(f"{catalog_name}.{schema_bronze}.dim_vendors")

# Standardize enums
df_vnd = df_vnd.withColumn("vendor_type", F.lower(F.trim(F.col("vendor_type"))))
df_vnd = df_vnd.withColumn("vendor_type",
    F.when(F.col("vendor_type").isin("subcontractor", "sub contractor"), "subcontractor")
     .when(F.col("vendor_type").isin("material_supplier", "supplier"), "material_supplier")
     .when(F.col("vendor_type").isin("equipment_rental", "equip rental"), "equipment_rental")
     .when(F.col("vendor_type") == "professional_services", "professional_services")
     .otherwise(F.lit(None)))

df_vnd = df_vnd.withColumn("trade_category", F.lower(F.trim(F.col("trade_category"))))
df_vnd = df_vnd.withColumn("trade_category",
    F.when(F.col("trade_category").isin("concrete", "mep", "roofing", "finishing", "earthworks", "glazing", "steel", "electrical"),
           F.col("trade_category"))
     .when(F.col("trade_category") == "finising", "finishing")
     .when(F.col("trade_category") == "earth works", "earthworks")
     .otherwise(F.lit(None)))

df_vnd = df_vnd.withColumn("dun_bradstreet_rating", F.lower(F.trim(F.col("dun_bradstreet_rating"))))
df_vnd = df_vnd.withColumn("dun_bradstreet_rating",
    F.when(F.col("dun_bradstreet_rating").isin("low", "medium", "high"), F.col("dun_bradstreet_rating"))
     .otherwise(F.lit(None)))

df_vnd = df_vnd.withColumn("approved_status", F.lower(F.trim(F.col("approved_status"))))
df_vnd = df_vnd.withColumn("approved_status",
    F.when(F.col("approved_status").isin("approved", "aproved"), "approved")
     .when(F.col("approved_status").isin("probation", "on_probation"), "probation")
     .when(F.col("approved_status").isin("suspended", "blocked"), "suspended")
     .when(F.col("approved_status") == "blacklisted", "blacklisted")
     .otherwise(F.lit(None)))

# Parse boolean: rala_certified
df_vnd = df_vnd.withColumn("rala_certified",
    F.when(F.lower(F.trim(F.col("rala_certified"))).isin("true", "yes", "y", "1"), F.lit(True))
     .when(F.lower(F.trim(F.col("rala_certified"))).isin("false", "no", "n", "0"), F.lit(False))
     .otherwise(F.lit(None)).cast(BooleanType()))

# Validate Finnish Y-tunnus: format NNNNNNN-N
df_vnd = df_vnd.withColumn("business_id_valid",
    F.when(F.col("business_id").rlike("^\\d{7}-\\d$"), F.lit(True)).otherwise(F.lit(False)))

df_vnd = clean_pks(df_vnd, "vendor_id")
df_vnd = df_vnd.withColumn("ingestion_timestamp", F.current_timestamp())

df_vnd.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.dim_vendors")
print(f"✅ silver.dim_vendors: {df_vnd.count()} rows")
display(df_vnd.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: fact_schedule_milestones
# ============================================================
# fact_schedule_milestones: parse dates, recalculate delay_days, fix logical errors
# ============================================================
df_ms = spark.table(f"{catalog_name}.{schema_bronze}.fact_schedule_milestones")

# Parse messy dates
df_ms = parse_messy_dates(df_ms, "planned_date", "forecast_date", "actual_date")

# Standardize milestone_status
df_ms = df_ms.withColumn("milestone_status", F.lower(F.trim(F.col("milestone_status"))))
df_ms = df_ms.withColumn("milestone_status",
    F.when(F.col("milestone_status").isin("on_track", "on track"), "on_track")
     .when(F.col("milestone_status").isin("at_risk"), "at_risk")
     .when(F.col("milestone_status") == "delayed", "delayed")
     .when(F.col("milestone_status") == "complete", "complete")
     .when(F.col("milestone_status") == "overdue", "delayed")
     .otherwise(F.lit(None)))

# Recalculate delay_days = actual_date - planned_date (only when actual_date is set)
df_ms = df_ms.withColumn("delay_days_corrected",
    F.when(F.col("actual_date_dt").isNotNull() & F.col("planned_date_dt").isNotNull(),
           F.datediff(F.col("actual_date_dt"), F.col("planned_date_dt")))
     .otherwise(F.lit(None)))

# Fix: actual_date should only exist for 'complete' milestones
df_ms = df_ms.withColumn("actual_date_dt",
    F.when(F.col("milestone_status") == "complete", F.col("actual_date_dt")).otherwise(F.lit(None)))
df_ms = df_ms.withColumn("delay_days_corrected",
    F.when(F.col("milestone_status") == "complete", F.col("delay_days_corrected")).otherwise(F.lit(None)))

# Validate FK: project_id
df_ms = df_ms.withColumn("project_id",
    F.when(F.col("project_id").rlike("^FRA-\\d{4}-\\d{4}$"), F.col("project_id")).otherwise(F.lit(None)))

df_ms = df_ms.drop("planned_date", "forecast_date", "actual_date", "delay_days")
df_ms = df_ms.withColumnRenamed("delay_days_corrected", "delay_days")
df_ms = clean_pks(df_ms, "milestone_id")
df_ms = df_ms.withColumn("ingestion_timestamp", F.current_timestamp())

df_ms.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.fact_schedule_milestones")
print(f"✅ silver.fact_schedule_milestones: {df_ms.count()} rows")
display(df_ms.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: fact_project_costs
# ============================================================
# fact_project_costs: parse period_month, clamp negatives, recalculate variance
# ============================================================
df_cost = spark.table(f"{catalog_name}.{schema_bronze}.fact_project_costs")

# Parse period_month (multiple formats: yyyy-MM-dd, yyyy-MM, dd/MM/yyyy, Month YYYY)
df_cost = df_cost.withColumn("period_month_dt",
    F.coalesce(
        F.expr("try_to_date(period_month, 'yyyy-MM-dd')"),
        F.expr("try_to_date(concat(period_month, '-01'), 'yyyy-MM-dd')"),
        F.expr("try_to_date(period_month, 'dd/MM/yyyy')"),
        F.expr("try_to_date(period_month, 'MMMM yyyy')"),
    ))

# Standardize cost_category
df_cost = df_cost.withColumn("cost_category", F.lower(F.trim(F.col("cost_category"))))
df_cost = df_cost.withColumn("cost_category",
    F.when(F.col("cost_category").isin("labour"), "labour")
     .when(F.col("cost_category").isin("materials"), "materials")
     .when(F.col("cost_category").isin("subcontract", "sub-contract"), "subcontract")
     .when(F.col("cost_category").isin("equipment", "equip"), "equipment")
     .when(F.col("cost_category") == "overhead", "overhead")
     .when(F.col("cost_category") == "design", "design")
     .otherwise(F.lit(None)))

# Clamp negative budget/committed/actual to absolute value
df_cost = (df_cost
    .withColumn("budget_eur", F.abs(F.col("budget_eur")))
    .withColumn("committed_eur", F.abs(F.col("committed_eur")))
    .withColumn("actual_eur", F.abs(F.col("actual_eur"))))

# Recalculate variance = budget - actual
df_cost = df_cost.withColumn("variance_eur_corrected",
    F.round(F.col("budget_eur") - F.col("actual_eur"), 2))

# Validate FK
df_cost = df_cost.withColumn("project_id",
    F.when(F.col("project_id").rlike("^FRA-\\d{4}-\\d{4}$"), F.col("project_id")).otherwise(F.lit(None)))

df_cost = df_cost.drop("period_month", "variance_eur")
df_cost = df_cost.withColumnRenamed("variance_eur_corrected", "variance_eur")
df_cost = clean_pks(df_cost, "cost_id")
df_cost = df_cost.withColumn("ingestion_timestamp", F.current_timestamp())

df_cost.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.fact_project_costs")
print(f"✅ silver.fact_project_costs: {df_cost.count()} rows")
display(df_cost.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: fact_purchase_orders
# ============================================================
# fact_purchase_orders: parse dates, clamp negatives, standardize enums
# ============================================================
df_po = spark.table(f"{catalog_name}.{schema_bronze}.fact_purchase_orders")

# Parse dates (column is po_date, not order_date)
df_po = parse_messy_dates(df_po, "po_date")

# Standardize po_category
df_po = df_po.withColumn("po_category", F.lower(F.trim(F.col("po_category"))))
df_po = df_po.withColumn("po_category",
    F.when(F.col("po_category").isin("material"), "material")
     .when(F.col("po_category").isin("subcontract"), "subcontract")
     .when(F.col("po_category").isin("equipment_rental", "equipment rental", "equip"), "equipment_rental")
     .when(F.col("po_category").isin("professional_services", "prof_services"), "professional_services")
     .otherwise(F.lit(None)))

# Standardize po_status
df_po = df_po.withColumn("po_status", F.lower(F.trim(F.col("po_status"))))
df_po = df_po.withColumn("po_status",
    F.when(F.col("po_status") == "open", "open")
     .when(F.col("po_status").isin("partially_invoiced", "partial_invoiced"), "partially_invoiced")
     .when(F.col("po_status").isin("closed"), "closed")
     .when(F.col("po_status").isin("cancelled", "canceled", "cancled"), "cancelled")
     .otherwise(F.lit(None)))

# Clamp negatives
df_po = df_po.withColumn("po_value_eur", F.abs(F.col("po_value_eur")))
df_po = df_po.withColumn("invoiced_eur", F.abs(F.col("invoiced_eur")))

# Over-invoicing flag
df_po = df_po.withColumn("is_over_invoiced",
    F.when(F.col("invoiced_eur") > F.col("po_value_eur"), F.lit(True)).otherwise(F.lit(False)))

# Validate FKs
df_po = df_po.withColumn("project_id",
    F.when(F.col("project_id").rlike("^FRA-\\d{4}-\\d{4}$"), F.col("project_id")).otherwise(F.lit(None)))
df_po = df_po.withColumn("vendor_id",
    F.when(F.col("vendor_id").rlike("^VND-\\d{4}$"), F.col("vendor_id")).otherwise(F.lit(None)))

df_po = df_po.drop("po_date")
df_po = clean_pks(df_po, "po_id")
df_po = df_po.withColumn("ingestion_timestamp", F.current_timestamp())

df_po.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.fact_purchase_orders")
print(f"✅ silver.fact_purchase_orders: {df_po.count()} rows")
display(df_po.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: fact_invoices
# ============================================================
# fact_invoices: parse dates, fix logical errors, clamp negatives
# ============================================================
df_inv = spark.table(f"{catalog_name}.{schema_bronze}.fact_invoices")

# Parse dates
df_inv = parse_messy_dates(df_inv, "invoice_date", "due_date", "paid_date")

# Standardize payment_status
df_inv = df_inv.withColumn("payment_status", F.lower(F.trim(F.col("payment_status"))))
df_inv = df_inv.withColumn("payment_status",
    F.when(F.col("payment_status") == "pending", "pending")
     .when(F.col("payment_status").isin("approved", "aproved"), "approved")
     .when(F.col("payment_status").isin("paid", "payed"), "paid")
     .when(F.col("payment_status") == "disputed", "disputed")
     .when(F.col("payment_status") == "overdue", "overdue")
     .otherwise(F.lit(None)))

# Clamp negative amounts
df_inv = df_inv.withColumn("amount_eur", F.abs(F.col("amount_eur")))

# Fix: paid_date should only exist for 'paid' status
df_inv = df_inv.withColumn("paid_date_dt",
    F.when(F.col("payment_status") == "paid", F.col("paid_date_dt")).otherwise(F.lit(None)))

# Fix: paid_date must be >= invoice_date
df_inv = df_inv.withColumn("paid_date_dt",
    F.when(F.col("paid_date_dt") < F.col("invoice_date_dt"), F.lit(None)).otherwise(F.col("paid_date_dt")))

# Recalculate days_overdue: only for non-paid, = today - due_date (if overdue)
df_inv = df_inv.withColumn("days_overdue_corrected",
    F.when((F.col("payment_status") != "paid") & F.col("due_date_dt").isNotNull(),
           F.greatest(F.datediff(F.current_date(), F.col("due_date_dt")), F.lit(0)))
     .otherwise(F.lit(0)))

# Validate FKs
df_inv = df_inv.withColumn("po_id",
    F.when(F.col("po_id").rlike("^PO-\\d{5}$"), F.col("po_id")).otherwise(F.lit(None)))
df_inv = df_inv.withColumn("vendor_id",
    F.when(F.col("vendor_id").rlike("^VND-\\d{4}$"), F.col("vendor_id")).otherwise(F.lit(None)))

df_inv = df_inv.drop("invoice_date", "due_date", "paid_date", "days_overdue")
df_inv = df_inv.withColumnRenamed("days_overdue_corrected", "days_overdue")
df_inv = clean_pks(df_inv, "invoice_id")
df_inv = df_inv.withColumn("ingestion_timestamp", F.current_timestamp())

df_inv.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.fact_invoices")
print(f"✅ silver.fact_invoices: {df_inv.count()} rows")
display(df_inv.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver: fact_subcontractor_performance
# ============================================================
# fact_subcontractor_performance: clamp scores, recalculate overall, parse booleans
# ============================================================
df_perf = spark.table(f"{catalog_name}.{schema_bronze}.fact_subcontractor_performance")

# Parse evaluation_date
df_perf = parse_messy_dates(df_perf, "evaluation_date")

# Clamp scores to 1-10 range (null out-of-range)
for score_col in ["quality_score", "schedule_score", "safety_score", "communication_score"]:
    df_perf = df_perf.withColumn(score_col,
        F.when((F.col(score_col) >= 1) & (F.col(score_col) <= 10), F.col(score_col)).otherwise(F.lit(None)))

# Recalculate overall_score = quality*0.4 + schedule*0.3 + safety*0.2 + communication*0.1
df_perf = df_perf.withColumn("overall_score_corrected",
    F.when(
        F.col("quality_score").isNotNull() & F.col("schedule_score").isNotNull() &
        F.col("safety_score").isNotNull() & F.col("communication_score").isNotNull(),
        F.round(
            F.col("quality_score") * 0.4 + F.col("schedule_score") * 0.3 +
            F.col("safety_score") * 0.2 + F.col("communication_score") * 0.1, 2))
     .otherwise(F.lit(None)))

# Parse boolean: would_rehire
df_perf = df_perf.withColumn("would_rehire",
    F.when(F.lower(F.trim(F.col("would_rehire"))).isin("true", "yes", "y", "1"), F.lit(True))
     .when(F.lower(F.trim(F.col("would_rehire"))).isin("false", "no", "n", "0"), F.lit(False))
     .otherwise(F.lit(None)).cast(BooleanType()))

# Validate FKs
df_perf = df_perf.withColumn("vendor_id",
    F.when(F.col("vendor_id").rlike("^VND-\\d{4}$"), F.col("vendor_id")).otherwise(F.lit(None)))
df_perf = df_perf.withColumn("project_id",
    F.when(F.col("project_id").rlike("^FRA-\\d{4}-\\d{4}$"), F.col("project_id")).otherwise(F.lit(None)))

df_perf = df_perf.drop("evaluation_date", "overall_score")
df_perf = df_perf.withColumnRenamed("overall_score_corrected", "overall_score")
df_perf = clean_pks(df_perf, "perf_id")
df_perf = df_perf.withColumn("ingestion_timestamp", F.current_timestamp())

df_perf.write.format("delta").mode("overwrite").saveAsTable(f"{catalog_name}.{schema_silver}.fact_subcontractor_performance")
print(f"✅ silver.fact_subcontractor_performance: {df_perf.count()} rows")
display(df_perf.limit(5))

# COMMAND ----------

# DBTITLE 1,Silver Layer Verification Summary
# ============================================================
# Verification: Silver layer summary
# ============================================================
silver_tables = [
    "dim_employee", "dim_projects", "dim_vendors",
    "fact_schedule_milestones", "fact_project_costs",
    "fact_purchase_orders", "fact_invoices",
    "fact_subcontractor_performance"
]

print("=" * 70)
print("SILVER LAYER VERIFICATION")
print("=" * 70)

for tbl in silver_tables:
    fqn = f"{catalog_name}.{schema_silver}.{tbl}"
    df = spark.table(fqn)
    total = df.count()
    pk_col = df.columns[0]
    null_pks = df.filter(F.col(pk_col).isNull()).count()
    dup_pks = df.groupBy(pk_col).count().filter("count > 1").count()
    print(f"\n  {fqn}")
    print(f"    Total rows:    {total}")
    print(f"    Null PKs:      {null_pks}")
    print(f"    Duplicate PKs: {dup_pks}")

print("\n" + "=" * 70)
print("✅ All 8 silver tables created and verified")
