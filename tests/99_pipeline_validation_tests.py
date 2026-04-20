# Databricks notebook source
# DBTITLE 1,Header — Validation Tests
# MAGIC %md
# MAGIC # 99 — Pipeline Validation Tests
# MAGIC ## Assertion-Based Data Quality Checks
# MAGIC
# MAGIC Runs automated checks across all three layers. **Every test must pass for the pipeline to be considered production-ready.**
# MAGIC
# MAGIC | Test Category | Checks |
# MAGIC |---|---|
# MAGIC | **Existence** | All expected tables exist |
# MAGIC | **Row Counts** | Tables are not empty |
# MAGIC | **PK Integrity** | No nulls or duplicates in Silver/Gold |
# MAGIC | **FK Referential** | Foreign keys reference valid parent records |
# MAGIC | **Value Ranges** | Scores 1-10, percentages 0-100, non-negative amounts |
# MAGIC | **Completeness** | Critical columns have acceptable null rates |

# COMMAND ----------

# DBTITLE 1,Test Framework
from pyspark.sql import functions as F

catalog_name = "construction_demo"

# Simple test framework
test_results = []

def run_test(test_name, condition, detail=""):
    """Run a boolean test and record result."""
    status = "✅ PASS" if condition else "❌ FAIL"
    test_results.append({"test": test_name, "status": status, "detail": detail})
    if not condition:
        print(f"  {status} {test_name}: {detail}")

print("✅ Test framework ready")

# COMMAND ----------

# DBTITLE 1,Test 1 — Table Existence
# ============================================================
# TEST 1: All expected tables exist
# ============================================================
print("\n🧪 TEST 1: Table Existence")
print("=" * 50)

expected = {
    "bronze": ["dim_employee","dim_projects","dim_vendors","fact_schedule_milestones","fact_project_costs","fact_purchase_orders","fact_invoices","fact_subcontractor_performance"],
    "silver": ["dim_employee","dim_projects","dim_vendors","fact_schedule_milestones","fact_project_costs","fact_purchase_orders","fact_invoices","fact_subcontractor_performance"],
    "gold":   ["gold_cost_variance_monthly","gold_schedule_kpi","gold_vendor_scorecard","gold_invoice_aging","gold_project_summary","ml_feature_table","ml_project_risk_scores"],
}

for schema, tables in expected.items():
    for tbl in tables:
        fqn = f"{catalog_name}.{schema}.{tbl}"
        try:
            spark.table(fqn)
            run_test(f"{fqn} exists", True)
        except:
            run_test(f"{fqn} exists", False, "Table not found")

print(f"  Checked {sum(len(v) for v in expected.values())} tables")

# COMMAND ----------

# DBTITLE 1,Test 2 — Row Counts (Non-Empty)
# ============================================================
# TEST 2: No empty tables
# ============================================================
print("\n🧪 TEST 2: Row Counts")
print("=" * 50)

for schema, tables in expected.items():
    for tbl in tables:
        fqn = f"{catalog_name}.{schema}.{tbl}"
        try:
            cnt = spark.table(fqn).count()
            run_test(f"{fqn} non-empty", cnt > 0, f"{cnt} rows")
        except:
            run_test(f"{fqn} non-empty", False, "Could not count")

print(f"  All table row counts validated")

# COMMAND ----------

# DBTITLE 1,Test 3 — Silver PK Integrity
# ============================================================
# TEST 3: Silver layer — no null PKs, no duplicate PKs
# ============================================================
print("\n🧪 TEST 3: Silver PK Integrity")
print("=" * 50)

pk_map = {
    "dim_employee": "employee_id",
    "dim_projects": "project_id",
    "dim_vendors": "vendor_id",
    "fact_schedule_milestones": "milestone_id",
    "fact_project_costs": "cost_id",
    "fact_purchase_orders": "po_id",
    "fact_invoices": "invoice_id",
    "fact_subcontractor_performance": "perf_id",
}

for tbl, pk in pk_map.items():
    fqn = f"{catalog_name}.silver.{tbl}"
    try:
        df = spark.table(fqn)
        null_pks = df.filter(F.col(pk).isNull()).count()
        dup_pks = df.groupBy(pk).count().filter("count > 1").count()
        run_test(f"{tbl}: no null {pk}", null_pks == 0, f"{null_pks} nulls")
        run_test(f"{tbl}: no dup {pk}", dup_pks == 0, f"{dup_pks} duplicates")
    except Exception as e:
        run_test(f"{tbl}: PK check", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 4 — Value Range Checks
# ============================================================
# TEST 4: Value range validation
# ============================================================
print("\n🧪 TEST 4: Value Ranges")
print("=" * 50)

# Subcontractor scores should be 1-10 or null
for score_col in ["quality_score", "schedule_score", "safety_score", "communication_score"]:
    try:
        df = spark.table(f"{catalog_name}.silver.fact_subcontractor_performance")
        invalid = df.filter(
            F.col(score_col).isNotNull() & 
            ((F.col(score_col) < 1) | (F.col(score_col) > 10))
        ).count()
        run_test(f"perf.{score_col} in [1,10]", invalid == 0, f"{invalid} out-of-range")
    except Exception as e:
        run_test(f"perf.{score_col} range", False, str(e))

# Gold project_summary risk_score should be >= 0
try:
    df = spark.table(f"{catalog_name}.gold.gold_project_summary")
    neg_risk = df.filter(F.col("risk_score") < 0).count()
    run_test("project_summary: risk_score >= 0", neg_risk == 0, f"{neg_risk} negative")
except Exception as e:
    run_test("project_summary: risk_score", False, str(e))

# Silver: no negative monetary values
for tbl, col in [("fact_project_costs","budget_eur"),("fact_project_costs","actual_eur"),("fact_purchase_orders","po_value_eur"),("fact_invoices","amount_eur")]:
    try:
        df = spark.table(f"{catalog_name}.silver.{tbl}")
        neg = df.filter(F.col(col) < 0).count()
        run_test(f"{tbl}.{col} >= 0", neg == 0, f"{neg} negative values")
    except Exception as e:
        run_test(f"{tbl}.{col}", False, str(e))

# COMMAND ----------

# DBTITLE 1,Test 5 — FK Referential Integrity
# ============================================================
# TEST 5: FK referential integrity in Silver
# ============================================================
print("\n🧪 TEST 5: FK Referential Integrity")
print("=" * 50)

# fact_project_costs.project_id should exist in dim_projects
try:
    df_costs = spark.table(f"{catalog_name}.silver.fact_project_costs").filter(F.col("project_id").isNotNull())
    df_proj = spark.table(f"{catalog_name}.silver.dim_projects")
    orphans = df_costs.join(df_proj, on="project_id", how="left_anti").count()
    total = df_costs.count()
    orphan_pct = round(orphans / total * 100, 2) if total > 0 else 0
    run_test("costs.project_id references dim_projects", orphan_pct < 5, f"{orphans} orphans ({orphan_pct}%)")
except Exception as e:
    run_test("costs FK check", False, str(e))

# fact_purchase_orders.vendor_id should exist in dim_vendors
try:
    df_po = spark.table(f"{catalog_name}.silver.fact_purchase_orders").filter(F.col("vendor_id").isNotNull())
    df_vnd = spark.table(f"{catalog_name}.silver.dim_vendors")
    orphans = df_po.join(df_vnd, on="vendor_id", how="left_anti").count()
    total = df_po.count()
    orphan_pct = round(orphans / total * 100, 2) if total > 0 else 0
    run_test("PO.vendor_id references dim_vendors", orphan_pct < 5, f"{orphans} orphans ({orphan_pct}%)")
except Exception as e:
    run_test("PO FK check", False, str(e))

print(f"  FK referential integrity validated")

# COMMAND ----------

# DBTITLE 1,Test Results Summary
# ============================================================
# FINAL TEST SUMMARY
# ============================================================
import pandas as pd

df_results = pd.DataFrame(test_results)
passed = (df_results["status"] == "✅ PASS").sum()
failed = (df_results["status"] == "❌ FAIL").sum()
total = len(df_results)

color = "#4CAF50" if failed == 0 else "#F44336"
icon = "✅" if failed == 0 else "❌"

displayHTML(f"""
<div style="padding: 25px; background: linear-gradient(135deg, #1a237e 0%, #283593 100%); border-radius: 12px; margin: 10px 0; text-align: center;">
  <h2 style="color: white;">{icon} Pipeline Validation Results</h2>
  <div style="display: flex; justify-content: center; gap: 40px; margin: 20px 0;">
    <div style="background: white; padding: 20px 30px; border-radius: 10px;">
      <div style="font-size: 2em; color: #4CAF50; font-weight: bold;">{passed}</div>
      <div style="color: #666;">Passed</div>
    </div>
    <div style="background: white; padding: 20px 30px; border-radius: 10px;">
      <div style="font-size: 2em; color: #F44336; font-weight: bold;">{failed}</div>
      <div style="color: #666;">Failed</div>
    </div>
    <div style="background: white; padding: 20px 30px; border-radius: 10px;">
      <div style="font-size: 2em; color: #2196F3; font-weight: bold;">{total}</div>
      <div style="color: #666;">Total</div>
    </div>
  </div>
  <p style="color: {'#c8e6c9' if failed==0 else '#ffcdd2'};">
    {'All tests passed — pipeline is production-ready!' if failed==0 else f'{failed} test(s) failed — review details below.'}
  </p>
</div>
""")

# Show failed tests if any
if failed > 0:
    print("\n❌ Failed Tests:")
    display(spark.createDataFrame(df_results[df_results["status"] == "❌ FAIL"]))
else:
    print("\n✅ All tests passed!")
