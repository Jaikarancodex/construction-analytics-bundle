# Databricks notebook source
# DBTITLE 1,Cover Page
# MAGIC %md
# MAGIC <div style="text-align: center; padding: 40px 20px;">
# MAGIC
# MAGIC # Construction Analytics — Project Cost & Performance Control Tower
# MAGIC ### End-to-End Intelligence Platform for Finnish Construction
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Built on Databricks Lakehouse**
# MAGIC
# MAGIC *Turning raw construction project data into predictive insights that prevent cost overruns, reduce schedule delays, and protect project margins.*
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Industry** | Finnish Construction & Infrastructure |
# MAGIC | **Scope** | 1,500 Projects, 1,000 Vendors, 1,200 Employees |
# MAGIC | **Data Period** | March 2025 – March 2026 |
# MAGIC | **Catalog** | `construction_demo.bronze → silver → gold` |
# MAGIC
# MAGIC </div>

# COMMAND ----------

# DBTITLE 1,Business Challenge
# MAGIC %md
# MAGIC ## The Business Challenge
# MAGIC
# MAGIC > *"A single undetected cost overrun can escalate from €50K to €2M before traditional monthly reporting catches it."*
# MAGIC
# MAGIC Finnish construction companies manage complex multi-site portfolios with subcontractors, material suppliers, and tight regulatory requirements. The challenges:
# MAGIC
# MAGIC | Pain Point | Business Impact |
# MAGIC |---|---|
# MAGIC | **Cost Overruns** | 70% of projects exceed budget — average overrun is 15-20% |
# MAGIC | **Schedule Delays** | Takt zone handovers slip silently until production stops |
# MAGIC | **Vendor Blind Spots** | No unified view of subcontractor reliability across projects |
# MAGIC | **Invoice Leakage** | Over-invoicing, duplicate payments, and aging disputes |
# MAGIC | **Manual Reporting** | Project managers spend 30% of time compiling status reports |
# MAGIC | **Fragmented Data** | Dates in 5+ formats, inconsistent enums, broken FK references |

# COMMAND ----------

# DBTITLE 1,Solution Overview
# MAGIC %md
# MAGIC ## What We Built — The Solution at a Glance
# MAGIC
# MAGIC A complete **data-to-decisions** platform that ingests raw construction data, cleanses it, builds business-ready metrics, and uses **machine learning to predict cost overruns before they happen**.
# MAGIC
# MAGIC ```
# MAGIC  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC  │   RAW DATA  │────▶│   CLEANSED  │────▶│  BUSINESS   │────▶│  ML MODEL   │────▶│  DECISIONS  │
# MAGIC  │   (Bronze)  │     │   (Silver)  │     │   (Gold)    │     │ Predictions │     │ & Actions   │
# MAGIC  └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC    8 Raw Tables    8 Cleansed     5 Gold KPIs      Risk Scores     2 Dashboards
# MAGIC    ~14K records    Tables         + 2 ML Tables    & Predictions   + Genie Space
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Data Foundation
# MAGIC %md
# MAGIC ## Step 1 — Data Foundation
# MAGIC *Notebook: `data_generator`*
# MAGIC
# MAGIC The platform ingests data from **eight core operational systems** that every construction company already has:
# MAGIC
# MAGIC | Data Source | Volume | What It Captures |
# MAGIC |---|---|---|
# MAGIC | **Employees** | 1,200 | Site managers, engineers, planners, safety officers, BIM coordinators |
# MAGIC | **Projects** | 1,500 | Residential, commercial, renovation, data center projects across HKI/ESP/VAN/HYV |
# MAGIC | **Vendors** | 1,000 | Subcontractors, material suppliers, equipment rental firms with RALA certification |
# MAGIC | **Schedule Milestones** | 2,000 | Takt zone planning with planned/forecast/actual dates |
# MAGIC | **Project Costs** | 1,800 | Budget vs committed vs actual by cost category (labour, materials, subcontract) |
# MAGIC | **Purchase Orders** | 2,500 | Procurement across projects & vendors with PO value tracking |
# MAGIC | **Invoices** | 2,500 | Payment tracking with overdue analysis and aging |
# MAGIC | **Subcontractor Performance** | 1,500 | Quality, schedule, safety & communication scores |

# COMMAND ----------

# DBTITLE 1,Data Quality Layer
# MAGIC %md
# MAGIC ## Step 2 — Data Quality & Enrichment
# MAGIC *Notebook: `01_bronze_to_silver`*
# MAGIC
# MAGIC Raw data is never analysis-ready. This step automatically:
# MAGIC
# MAGIC * **Parses messy dates** — handles dd/MM/yyyy, yyyyMMdd, dd-MMM-yyyy, MM-dd-yyyy formats
# MAGIC * **Standardizes enums** — normalizes inconsistent casing ("ACTIVE", "Active", "actve" → "active")
# MAGIC * **Removes duplicate PKs** — keeps first occurrence, drops duplicates
# MAGIC * **Filters null PKs** — removes records with missing primary keys
# MAGIC * **Validates foreign keys** — flags invalid vendor/project/employee references
# MAGIC * **Clamps negative values** — sets monetary & area columns to absolute values
# MAGIC * **Parses boolean strings** — converts "yes"/"Y"/"1"/"true" → proper booleans
# MAGIC * **Recalculates derived fields** — fixes wrong variance_eur, delay_days, overall_score
# MAGIC * **Validates Finnish Y-tunnus** — flags malformed business IDs

# COMMAND ----------

# DBTITLE 1,Business Metrics Layer
# MAGIC %md
# MAGIC ## Step 3 — Business-Ready Metrics
# MAGIC *Notebook: `02_silver_to_gold`*
# MAGIC
# MAGIC Clean data is transformed into **five Gold tables** purpose-built for business decisions:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Cost Variance Monthly
# MAGIC Track budget vs actual by project, cost category, and month — spot overruns early.
# MAGIC > *"Which projects exceeded budget in Q4? What categories are driving the overrun?"*
# MAGIC
# MAGIC ### Schedule KPI
# MAGIC Milestone completion rates, average delays by takt zone, and on-track percentages.
# MAGIC > *"What is our milestone hit rate this month? Which takt zones are consistently late?"*
# MAGIC
# MAGIC ### Vendor Scorecard
# MAGIC Composite scores combining quality, schedule, safety, communication, and invoice performance.
# MAGIC > *"Which subcontractors should we re-hire? Who needs to be put on probation?"*
# MAGIC
# MAGIC ### Invoice Aging
# MAGIC Aging buckets (0-30, 31-60, 61-90, 90+), overdue rates, and payment velocity.
# MAGIC > *"How much is sitting in 90+ days overdue? Which vendors have the most disputes?"*
# MAGIC
# MAGIC ### Project Summary
# MAGIC Holistic project health — budget utilization, schedule adherence, vendor performance, risk score.
# MAGIC > *"Give me a one-page view of every active project's health."*

# COMMAND ----------

# DBTITLE 1,ML Prediction Engine
# MAGIC %md
# MAGIC ## Step 4 — Predictive Intelligence
# MAGIC *Notebooks: `03_feature_engineering` + `04_ml_model`*
# MAGIC
# MAGIC This is where the platform shifts from **reporting the past** to **predicting the future**.
# MAGIC
# MAGIC ### What the Model Does
# MAGIC For every active project, the model answers one question:
# MAGIC
# MAGIC > **"What is the probability this project will exceed its budget by more than 10%?"**
# MAGIC
# MAGIC It assigns a **risk score (0% – 100%)** and categorizes each project as:
# MAGIC
# MAGIC | Risk Level | Score | Action |
# MAGIC |---|---|---|
# MAGIC | 🟢 Low Risk | < 30% | Monitor normally |
# MAGIC | 🟡 Medium Risk | 30% – 70% | Weekly review, flag procurement |
# MAGIC | 🔴 High Risk | > 70% | Immediate escalation to project director |
# MAGIC
# MAGIC ### Key Features
# MAGIC * Historical cost variance trends
# MAGIC * Vendor reliability scores
# MAGIC * Milestone delay patterns
# MAGIC * Purchase order over-invoicing ratios
# MAGIC * Project complexity (floor area, duration, type)

# COMMAND ----------

# DBTITLE 1,Dashboards and Self-Service
# MAGIC %md
# MAGIC ## Step 5 — Dashboards & AI-Powered Self-Service
# MAGIC *Notebooks: `05_dashboard_genie` + `06_create_genie_space`*
# MAGIC
# MAGIC ### Dashboard 1: Construction Cost & Performance Control Tower
# MAGIC A visual command center for construction leadership:
# MAGIC
# MAGIC | Widget | What You See |
# MAGIC |---|---|
# MAGIC | **Cost Variance Trend** | Monthly budget vs actual across all projects |
# MAGIC | **Schedule Heatmap** | Milestone completion by takt zone and project |
# MAGIC | **Vendor Leaderboard** | Top/bottom vendors by composite score |
# MAGIC | **Invoice Aging Bars** | Overdue distribution by aging bucket |
# MAGIC | **Project Health Cards** | At-a-glance status for every active project |
# MAGIC | **ML Risk Predictions** | High-risk projects flagged with predicted overrun % |
# MAGIC
# MAGIC ### Dashboard 2: Construction Analytics — Executive Dashboard
# MAGIC High-level financial and vendor risk overview for executive stakeholders:
# MAGIC
# MAGIC | Widget | What You See |
# MAGIC |---|---|
# MAGIC | **Budget vs Actual by Project** | Side-by-side budget comparison per project |
# MAGIC | **Financial Health Distribution** | Project-vendor financial health breakdown |
# MAGIC | **Top 10 Vendors by Risk Score** | Highest-risk vendors ranked |
# MAGIC | **Vendor Risk Category & Performance Tier** | Risk and performance distributions |
# MAGIC | **Project Summary Table** | Detailed project-level metrics with filters |
# MAGIC
# MAGIC ### Model Serving Endpoint: `construction-cost-overrun`
# MAGIC Real-time REST API for cost overrun predictions — scale-to-zero enabled, registered in Unity Catalog.
# MAGIC
# MAGIC ### Genie Space: Natural Language Q&A
# MAGIC Business users ask questions in plain Finnish or English:
# MAGIC * *"Mikä on Kalasatama Tower -projektin budjettitilanne?"*
# MAGIC * *"Show me all vendors with safety score below 5"*
# MAGIC * *"Which projects are predicted to go over budget next quarter?"*

# COMMAND ----------

# DBTITLE 1,Pipeline Architecture
# MAGIC %md
# MAGIC ## Technical Architecture (Reference)
# MAGIC
# MAGIC | Notebook | Purpose | Key Output |
# MAGIC |---|---|---|
# MAGIC | `data_generator` | Generate 1 year of messy construction data | 8 Bronze Delta tables |
# MAGIC | `_init` | Parameterized configuration & helper functions | Catalog/schema variables (widget-driven) |
# MAGIC | `01_bronze_to_silver` | Cleanse, parse dates, standardize enums | 8 Silver Delta tables |
# MAGIC | `02_silver_to_gold` | Business aggregates & KPIs | 5 Gold Delta tables |
# MAGIC | `03_feature_engineering` | Build ML feature table | 1 Feature table |
# MAGIC | `04_ml_model` | Train, register, serve ML model | Unity Catalog model + serving endpoint |
# MAGIC | `05_dashboard_genie` | Create AI/BI dashboards | 2 Lakeview dashboards |
# MAGIC | `06_create_genie_space` | Create Genie space | Self-service Genie space |
# MAGIC | `07_Data_Quality_Audit` | Validate data quality across all layers | Audit scores & anomaly reports |
# MAGIC | `08_UC_Governance_Tags` | Apply Unity Catalog governance tags | PII, sensitivity, domain tags |
# MAGIC | `99_Pipeline_Validation_Tests` | End-to-end pipeline validation | 73 automated test assertions |
# MAGIC | `_teardown` | Drop all tables, schemas, models | Clean slate for re-demo (parameterized) |
# MAGIC | `spd_construction_pipeline` | SDP declarative pipeline | Automated Bronze→Silver→Gold (incremental) |
# MAGIC | `spd_tasks/*` | Individual SDP task definitions | Per-table streaming/materialized views |

# COMMAND ----------

# DBTITLE 1,Production Infrastructure
# MAGIC %md
# MAGIC ## Production Infrastructure
# MAGIC
# MAGIC ### Job Workflow: `Construction Analytics Pipeline Workflow`
# MAGIC An 8-task Lakeflow Job with parallel execution, running daily at 06:00 (Europe/Helsinki):
# MAGIC
# MAGIC ```
# MAGIC data_generator → bronze_to_silver → silver_to_gold ─┬→ feature_engineering → ml_model ────────┐
# MAGIC                                                   ├→ data_quality_audit ────────────────────┤
# MAGIC                                                   └→ uc_governance_tags ────────────────────┴→ pipeline_validation_tests
# MAGIC ```
# MAGIC
# MAGIC * **Failure/success alerts** → email notifications configured
# MAGIC * **2-hour timeout** with queue enabled
# MAGIC * **Last full run**: ~6 minutes, all 8/8 tasks passed
# MAGIC
# MAGIC ### SDP Declarative Pipeline (Incremental Alternative)
# MAGIC A Lakeflow Spark Declarative Pipeline that performs the same Bronze → Silver → Gold transformations using streaming tables and materialized views.
# MAGIC
# MAGIC > **⚠️ Architecture Note:** The Job (batch) and SDP Pipeline (incremental) both produce the same silver tables but different gold tables. They are **not run together** — the Job is the primary daily workflow, while the SDP pipeline serves as a reference implementation for when real streaming data sources replace the synthetic data generator.
# MAGIC
# MAGIC ### Model Serving Endpoint: `construction-cost-overrun`
# MAGIC * Registered model: `construction_demo.gold.construction_cost_overrun_model`
# MAGIC * Scale-to-zero enabled — no cost when idle
# MAGIC * REST API for real-time cost overrun probability scoring
# MAGIC
# MAGIC ### Unity Catalog Governance
# MAGIC * **Column-level tags**: `sensitivity=high` on financial/PII columns (contract\_value\_eur, amount\_eur, full\_name, business\_id, site\_address)
# MAGIC * **Table-level tags**: `domain`, `layer`, `refresh` on all gold tables
# MAGIC * **Table comments** on all gold tables for discoverability

# COMMAND ----------

# DBTITLE 1,Business Value
# MAGIC %md
# MAGIC ## Business Value & ROI
# MAGIC
# MAGIC ### Quantifiable Benefits
# MAGIC
# MAGIC | Benefit | Estimated Impact |
# MAGIC |---|---|
# MAGIC | **Early cost overrun detection** | Catching overruns 2 months earlier saves **€200K–€500K per project** |
# MAGIC | **Reduce schedule delays** | Proactive takt zone management reduces delays by **25–40%** |
# MAGIC | **Vendor accountability** | Data-driven scorecards reduce subcontractor issues by **30%** |
# MAGIC | **Invoice leakage prevention** | Automated over-invoicing detection saves **2–5% of procurement spend** |
# MAGIC | **Reporting automation** | Eliminates 30% of PM time spent on manual reporting → **€150K/year** savings |
# MAGIC | **Predictive maintenance of margins** | ML risk scoring prevents margin erosion on high-value projects |

# COMMAND ----------

# DBTITLE 1,KPI Dashboard — All Gold Layer Metrics
from pyspark.sql import functions as F

catalog = "construction_demo"

# ── COST KPIs ──
df_cost = spark.table(f"{catalog}.gold.gold_cost_variance_monthly")
cost_kpis = df_cost.agg(
    F.sum("total_budget_eur").alias("total_budget"),
    F.sum("total_actual_eur").alias("total_actual"),
    F.sum("total_variance_eur").alias("total_variance"),
    F.avg("budget_utilization_pct").alias("avg_utilization"),
    F.sum(F.when(F.col("is_over_budget") == True, 1).otherwise(0)).alias("overrun_lines"),
    F.count("*").alias("total_lines")
).collect()[0]

# ── SCHEDULE KPIs ──
df_sched = spark.table(f"{catalog}.gold.gold_schedule_kpi")
sched_kpis = df_sched.agg(
    F.avg("completion_rate_pct").alias("avg_completion_rate"),
    F.avg("delay_rate_pct").alias("avg_delay_rate"),
    F.avg("avg_delay_days").alias("avg_delay_days"),
    F.sum("total_milestones").alias("total_milestones"),
    F.sum("completed_milestones").alias("completed_milestones"),
    F.sum("delayed_milestones").alias("delayed_milestones")
).collect()[0]

# ── VENDOR KPIs ──
df_vendor = spark.table(f"{catalog}.gold.gold_vendor_scorecard")
vendor_kpis = df_vendor.agg(
    F.count("*").alias("total_vendors"),
    F.avg("avg_overall_score").alias("avg_vendor_score"),
    F.sum("total_invoices").alias("total_invoices_reviewed"),
    F.sum("disputed_invoices").alias("total_disputed"),
    F.avg("rehire_rate_pct").alias("avg_rehire_rate")
).collect()[0]

# ── INVOICE KPIs ──
df_inv = spark.table(f"{catalog}.gold.gold_invoice_aging")
inv_kpis = df_inv.agg(
    F.sum("invoice_count").alias("total_invoices"),
    F.sum("total_amount_eur").alias("total_invoice_value"),
    F.avg("avg_days_overdue").alias("avg_days_overdue")
).collect()[0]
inv_90plus = df_inv.filter(F.col("aging_bucket") == "90+ days").agg(
    F.sum("invoice_count").alias("count_90plus"),
    F.sum("total_amount_eur").alias("amount_90plus")
).collect()[0]

# ── ML RISK KPIs ──
df_risk = spark.table(f"{catalog}.gold.ml_project_risk_scores")
risk_kpis = df_risk.groupBy("risk_level").count().collect()
risk_dict = {r["risk_level"]: r["count"] for r in risk_kpis}
total_scored = df_risk.count()

# ── PROJECT SUMMARY KPIs ──
df_proj = spark.table(f"{catalog}.gold.gold_project_summary")
proj_kpis = df_proj.agg(
    F.count("*").alias("total_projects"),
    F.avg("budget_utilization_pct").alias("avg_budget_util"),
    F.avg("milestone_completion_pct").alias("avg_milestone_pct")
).collect()[0]
proj_by_risk = df_proj.groupBy("risk_level").count().collect()
risk_proj = {r["risk_level"]: r["count"] for r in proj_by_risk}

# ── DISPLAY ──
displayHTML(f"""
<div style="font-family: -apple-system, sans-serif; padding: 10px;">

<h2 style="text-align:center; color:#1a237e;">Construction Analytics — Key Performance Indicators</h2>
<hr style="border:2px solid #1a237e;">

<div style="display:flex; gap:20px; flex-wrap:wrap; justify-content:center; margin:20px 0;">

  <div style="background:linear-gradient(135deg,#1565c0,#0d47a1); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">💰 Cost Performance</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Total Budget</td><td style="text-align:right; font-weight:bold;">€{cost_kpis['total_budget']:,.0f}</td></tr>
      <tr><td>Total Actual Spend</td><td style="text-align:right; font-weight:bold;">€{cost_kpis['total_actual']:,.0f}</td></tr>
      <tr><td>Total Variance</td><td style="text-align:right; font-weight:bold; color:{'#ff8a80' if cost_kpis['total_variance'] < 0 else '#b9f6ca'};">€{cost_kpis['total_variance']:,.0f}</td></tr>
      <tr><td>Avg Budget Utilization</td><td style="text-align:right; font-weight:bold;">{cost_kpis['avg_utilization']:.1f}%</td></tr>
      <tr><td>Over-Budget Lines</td><td style="text-align:right; font-weight:bold;">{cost_kpis['overrun_lines']:,} / {cost_kpis['total_lines']:,}</td></tr>
    </table>
  </div>

  <div style="background:linear-gradient(135deg,#2e7d32,#1b5e20); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">📅 Schedule Performance</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Total Milestones</td><td style="text-align:right; font-weight:bold;">{int(sched_kpis['total_milestones']):,}</td></tr>
      <tr><td>Completed</td><td style="text-align:right; font-weight:bold;">{int(sched_kpis['completed_milestones']):,}</td></tr>
      <tr><td>Delayed</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">{int(sched_kpis['delayed_milestones']):,}</td></tr>
      <tr><td>Avg Completion Rate</td><td style="text-align:right; font-weight:bold;">{sched_kpis['avg_completion_rate']:.1f}%</td></tr>
      <tr><td>Avg Delay (days)</td><td style="text-align:right; font-weight:bold;">{sched_kpis['avg_delay_days']:.1f}</td></tr>
    </table>
  </div>

  <div style="background:linear-gradient(135deg,#e65100,#bf360c); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">🏢 Vendor Performance</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Vendors Evaluated</td><td style="text-align:right; font-weight:bold;">{vendor_kpis['total_vendors']:,}</td></tr>
      <tr><td>Avg Vendor Score</td><td style="text-align:right; font-weight:bold;">{vendor_kpis['avg_vendor_score']:.2f} / 10</td></tr>
      <tr><td>Avg Rehire Rate</td><td style="text-align:right; font-weight:bold;">{vendor_kpis['avg_rehire_rate']:.1f}%</td></tr>
      <tr><td>Invoices Reviewed</td><td style="text-align:right; font-weight:bold;">{int(vendor_kpis['total_invoices_reviewed']):,}</td></tr>
      <tr><td>Disputed Invoices</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">{int(vendor_kpis['total_disputed']):,}</td></tr>
    </table>
  </div>

</div>

<div style="display:flex; gap:20px; flex-wrap:wrap; justify-content:center; margin:20px 0;">

  <div style="background:linear-gradient(135deg,#6a1b9a,#4a148c); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">📄 Invoice Aging</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Total Invoices</td><td style="text-align:right; font-weight:bold;">{int(inv_kpis['total_invoices']):,}</td></tr>
      <tr><td>Total Invoice Value</td><td style="text-align:right; font-weight:bold;">€{inv_kpis['total_invoice_value']:,.0f}</td></tr>
      <tr><td>Avg Days Overdue</td><td style="text-align:right; font-weight:bold;">{inv_kpis['avg_days_overdue']:.0f} days</td></tr>
      <tr><td>90+ Day Invoices</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">{int(inv_90plus['count_90plus']):,}</td></tr>
      <tr><td>90+ Day Exposure</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">€{inv_90plus['amount_90plus']:,.0f}</td></tr>
    </table>
  </div>

  <div style="background:linear-gradient(135deg,#c62828,#b71c1c); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">🤖 ML Risk Predictions</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Projects Scored</td><td style="text-align:right; font-weight:bold;">{total_scored:,}</td></tr>
      <tr><td>🔴 High Risk</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">{risk_dict.get('high', 0):,}</td></tr>
      <tr><td>🟡 Medium Risk</td><td style="text-align:right; font-weight:bold;">{risk_dict.get('medium', 0):,}</td></tr>
      <tr><td>🟢 Low Risk</td><td style="text-align:right; font-weight:bold; color:#b9f6ca;">{risk_dict.get('low', 0):,}</td></tr>
      <tr><td>High Risk %</td><td style="text-align:right; font-weight:bold;">{risk_dict.get('high', 0)/total_scored*100:.1f}%</td></tr>
    </table>
  </div>

  <div style="background:linear-gradient(135deg,#37474f,#263238); color:white; padding:25px; border-radius:12px; min-width:280px; flex:1;">
    <h3 style="margin:0 0 15px 0;">📊 Project Portfolio</h3>
    <table style="color:white; width:100%; font-size:14px;">
      <tr><td>Total Projects</td><td style="text-align:right; font-weight:bold;">{proj_kpis['total_projects']:,}</td></tr>
      <tr><td>Avg Budget Utilization</td><td style="text-align:right; font-weight:bold;">{proj_kpis['avg_budget_util']:.1f}%</td></tr>
      <tr><td>Avg Milestone Completion</td><td style="text-align:right; font-weight:bold;">{proj_kpis['avg_milestone_pct']:.1f}%</td></tr>
      <tr><td>Low Risk Projects</td><td style="text-align:right; font-weight:bold; color:#b9f6ca;">{risk_proj.get('low', 0):,}</td></tr>
      <tr><td>High Risk Projects</td><td style="text-align:right; font-weight:bold; color:#ff8a80;">{risk_proj.get('high', 0):,}</td></tr>
    </table>
  </div>

</div>
</div>
""")
