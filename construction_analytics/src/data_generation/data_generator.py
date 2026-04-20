# Databricks notebook source
# DBTITLE 1,Header — Construction Analytics Data Generator
# MAGIC %md
# MAGIC    
# MAGIC # Data Generation
# MAGIC ## Construction Analytics — Project Cost & Performance Control Tower
# MAGIC
# MAGIC Generate **1+ year** of synthetic Finnish construction project data (**Mar 2025 – Mar 2026**):
# MAGIC
# MAGIC | # | Bronze Table | Records | Description |
# MAGIC |---|---|---|---|
# MAGIC | 1 | `dim_employee` | ~1,200 | Site managers, engineers, planners, safety officers |
# MAGIC | 2 | `dim_projects` | ~1,500 | Residential, commercial, renovation, data center projects |
# MAGIC | 3 | `dim_vendors` | ~1,000 | Subcontractors, material suppliers, equipment rental firms |
# MAGIC | 4 | `fact_schedule_milestones` | ~2,000 | Milestone tracking with takt zone planning |
# MAGIC | 5 | `fact_project_costs` | ~1,800 | Budget, committed, actual costs by category & period |
# MAGIC | 6 | `fact_purchase_orders` | ~2,500 | Procurement orders across projects & vendors |
# MAGIC | 7 | `fact_invoices` | ~2,500 | Invoice & payment tracking with overdue analysis |
# MAGIC | 8 | `fact_subcontractor_performance` | ~1,500 | Quality, schedule, safety & communication scores |
# MAGIC
# MAGIC **Total: ~14,000 records across 8 tables → `construction_demo.bronze`**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Data Quality Issues Injected
# MAGIC - **Null primary keys** (~2–3% per table)
# MAGIC - **Duplicate primary keys** (20–40 per table)
# MAGIC - **Invalid foreign keys** (~7–8% of FK columns)
# MAGIC - **Dates as mixed-format strings** (dd/MM/yyyy, yyyyMMdd, dd-MMM-yyyy)
# MAGIC - **Negative monetary & area values** (~4–5%)
# MAGIC - **Inconsistent enum casing & invalid values** (~10–12%)
# MAGIC - **Wrong calculated fields** (variance, delay_days, overall_score)
# MAGIC - **Logical errors** (paid_date < invoice_date, actual_end < start_date)
# MAGIC - **Booleans stored as strings** (yes/no/Y/N/1/0/true/false)
# MAGIC - **Malformed Finnish Business IDs** (Y-tunnus)

# COMMAND ----------

# DBTITLE 1,Setup — imports, schema, helpers, seed data
import random
import string
from datetime import datetime, timedelta, date
from pyspark.sql.types import *
from pyspark.sql import functions as F

random.seed(42)

# --------------- Create bronze schema ---------------
spark.sql("USE CATALOG construction_demo")
spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")

# --------------- Record counts ---------------
NUM_EMPLOYEES  = 1200
NUM_PROJECTS   = 1500
NUM_VENDORS    = 1000
NUM_MILESTONES = 2000
NUM_COSTS      = 1800
NUM_POS        = 2500
NUM_INVOICES   = 2500
NUM_PERF       = 1500

# --------------- Helper functions ---------------
def rand_date(start_year=2025, end_year=2026):
    start = date(start_year, 3, 1)
    end   = date(end_year, 3, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))

def maybe_null(val, pct=0.05):
    return None if random.random() < pct else val

def messy_enum(valid_vals, messy_vals, messy_pct=0.10):
    if random.random() < messy_pct:
        return random.choice(messy_vals)
    return random.choice(valid_vals)

def messy_date_str(d, pct=0.08):
    """Return date as string — sometimes in wrong format."""
    if random.random() < pct:
        return d.strftime(random.choice(["%d/%m/%Y", "%Y%m%d", "%d-%b-%Y", "%m-%d-%Y"]))
    return str(d)

# --------------- Seed data ---------------
first_names = [
    "Mikko","Jari","Antti","Matti","Pekka","Sami","Timo","Juha","Heikki","Kari",
    "Ville","Tuomas","Janne","Petri","Markku","Risto","Seppo","Eero","Lauri","Olli",
    "Anna","Maria","Sanna","Tiina","Minna","Laura","Hanna","Kaisa","Elina",
    "Aleksi","Niko","Tommi","Joni","Teemu","Ilkka","Kimmo","Harri","Jussi","Oskari"]
last_names = [
    "Virtanen","Korhonen","Nieminen","Mäkinen","Hämäläinen","Laine","Heikkinen",
    "Koskinen","Järvinen","Lehtonen","Lehtinen","Saarinen","Niemi","Salonen","Heinonen",
    "Turunen","Salo","Laitinen","Tuominen","Rantanen","Aalto","Aaltonen","Laakso","Mattila"]

roles_valid  = ["site_manager","project_manager","foreman","safety_officer","quantity_surveyor",
                "design_manager","procurement_officer","site_engineer","planner","bim_coordinator"]
roles_messy  = ["Site_Manager","PROJECT MANAGER","FOREMAN","site  manager","mgr","N/A","","unknown","NULL"]
depts_valid  = ["construction","design","procurement","safety","planning","management","finance"]
depts_messy  = ["Construction","DESIGN","Procurement ","sfaety","","N/A","unknown","  construction"]

project_names = [
    "Puistola School","Kalasatama Tower","Leppävaara Hospital Extension","Tikkurila Mall",
    "Hyvinkää Datacenter","Matinkylä Apartments","Keilaniemi Office","Pasila Station Renovation",
    "Espoo Cultural Center","Myyrmäki Residential","Herttoniemi Warehouse","Vuosaari Logistics Hub",
    "Tapiola Renovation","Itäkeskus Pipe Renewal","Kontula Lifecycle Reno","Pitäjänmäki DC",
    "Lauttasaari Terrace","Munkkiniemi Park Houses","Sörnäinen Mixed Use","Kamppi Underground"]

vendor_names_seed = [
    "Lujabetoni Oy","YIT Oyj","SRV Group","Peab Finland","Skanska Finland",
    "Caverion Oyj","Are Oy","Bravida Finland","Onninen Oy","Rautaruukki Oyj",
    "Parma Oy","Rudus Oy","Saint-Gobain Finland","Hilti Finland","Cramo Finland",
    "Ramirent Finland","Peri Suomi","Doka Finland","NCC Finland","Destia Oy",
    "Fescon Oy","Tikkurila Oyj","Uponor Oyj","Kone Oyj","Schindler Finland"]

municipalities = ["HKI","ESP","VAN","HYV"]
project_types  = ["residential","commercial","renovation","pipe_renovation","lifecycle","data_center"]
statuses_valid = ["active","completed","tendering","on_hold"]
statuses_messy = ["Active","ACTIVE","complted","actve","on hold","On_Hold","pending","cancelled"]

streets = {
    "HKI": ["Mannerheimintie","Hämeentie","Sörnäisten rantatie","Mechelininkatu","Bulevardi"],
    "ESP": ["Leppävaarankatu","Niittykumpu","Otaniementie","Keilalahdentie","Tapiolantie"],
    "VAN": ["Tikkurilantie","Myyrmäentie","Martinlaaksontie","Koivukylänväylä","Jokiniementie"],
    "HYV": ["Hämeenkatu","Uudenmaankatu","Kauppakatu","Munckinkatu","Torikatu"]
}

# --------------- Pre-generated valid IDs ---------------
valid_emp_ids    = [f"EMP-{str(i).zfill(4)}" for i in range(1, NUM_EMPLOYEES + 1)]
valid_proj_ids   = [f"FRA-{random.choice(['2025','2026'])}-{str(i).zfill(4)}" for i in range(1, NUM_PROJECTS + 1)]
valid_vendor_ids = [f"VND-{str(i).zfill(4)}" for i in range(1, NUM_VENDORS + 1)]
valid_po_ids     = [f"PO-{str(i).zfill(5)}" for i in range(1, NUM_POS + 1)]
client_ids       = [f"CLI-{str(i).zfill(3)}" for i in range(1, 201)]

# Invalid FK pools for injecting bad references
invalid_fk_emp    = ["EMP-9999","EMP-0000","EMP-XXXX","","NULL",None]
invalid_fk_proj   = ["FRA-2020-0000","PRJ-999","INVALID","","NULL",None]
invalid_fk_vendor = ["VND-0000","VND-XXXX","SUPPLIER-1","","NULL",None]
invalid_fk_client = ["CLI-000","CLIENT-X","","NULL",None]
invalid_fk_po     = ["PO-00000","PO-XXXXX","INVALID","","NULL",None]

print("Setup complete — ready to generate messy Bronze data (Mar 2025 – Mar 2026)")

# COMMAND ----------

# DBTITLE 1,dim_employee — ~1200 rows
# ========================================
# dim_employee — ~1,200 records
# Messy: null PKs, duplicate PKs, invalid roles/depts, null names
# ========================================

rows_emp = []
for i in range(1, NUM_EMPLOYEES + 1):
    emp_id = maybe_null(f"EMP-{str(i).zfill(4)}", 0.03)
    name   = maybe_null(f"{random.choice(first_names)} {random.choice(last_names)}", 0.05)
    role   = messy_enum(roles_valid, roles_messy, 0.10)
    dept   = messy_enum(depts_valid, depts_messy, 0.10)
    rows_emp.append((emp_id, name, role, dept))

# Inject ~30 duplicate PKs
for _ in range(30):
    dup_id = random.choice(valid_emp_ids)
    rows_emp.append((
        dup_id,
        f"{random.choice(first_names)} {random.choice(last_names)}",
        random.choice(roles_valid),
        random.choice(depts_valid)
    ))

random.shuffle(rows_emp)

df_dim_employee = spark.createDataFrame(rows_emp, schema=StructType([
    StructField("employee_id", StringType(), True),
    StructField("full_name",   StringType(), True),
    StructField("role",        StringType(), True),
    StructField("department",  StringType(), True),
]))

print(f"dim_employee: {df_dim_employee.count()} rows")
display(df_dim_employee.limit(10))

# COMMAND ----------

# DBTITLE 1,dim_projects — ~1500 rows
# ========================================
# dim_projects — ~1,500 records
# Messy: dates as strings, negative values, invalid FKs, inconsistent
#        enums, actual_end before start, non-null actual_end for active
# ========================================

rows_proj = []
for i in range(1, NUM_PROJECTS + 1):
    pid   = maybe_null(valid_proj_ids[i - 1], 0.02)
    pname = maybe_null(
        f"{random.choice(project_names)} {random.choice(['Phase I','Phase II','Block A','Block B','Extension',''])}".strip(),
        0.04
    )
    ptype  = messy_enum(project_types, ["Residential","COMMERCIAL","reno","datacenter","pipe reno","unknown",""], 0.12)
    muni   = messy_enum(municipalities, ["Helsinki","espoo","hki","VANTAA","Hyv","","unknown"], 0.10)

    # Address
    clean_muni = random.choice(municipalities)
    addr = maybe_null(
        f"{random.choice(streets[clean_muni])} {random.randint(1, 120)}, {clean_muni}",
        0.06
    )

    status = messy_enum(statuses_valid, statuses_messy, 0.12)

    # --- Dates (sometimes wrong format, sometimes logically wrong) ---
    sd  = rand_date(2025, 2026)
    ped = sd + timedelta(days=random.randint(90, 900))

    start_str   = messy_date_str(sd)
    planned_str = messy_date_str(ped)

    # actual_end_date
    if status in ["completed", "complted"]:
        aed = ped + timedelta(days=random.randint(-30, 120))
        # 5% logical error: actual_end before start
        if random.random() < 0.05:
            aed = sd - timedelta(days=random.randint(30, 365))
        actual_str = messy_date_str(aed)
    else:
        actual_str = None
        # 3% logical error: non-null actual_end for active projects
        if random.random() < 0.03:
            actual_str = str(rand_date(2025, 2026))

    # Contract value
    contract_val = round(random.uniform(500_000, 50_000_000), 2)
    if random.random() < 0.05:
        contract_val = -abs(contract_val)  # negative
    contract_val = maybe_null(contract_val, 0.03)

    # Floor area
    floor_area = round(random.uniform(200, 50_000), 1)
    if random.random() < 0.04:
        floor_area = -abs(floor_area)  # negative
    floor_area = maybe_null(floor_area, 0.04)

    # Foreign keys
    pm_id = random.choice(invalid_fk_emp) if random.random() < 0.07 else random.choice(valid_emp_ids)
    cid   = random.choice(invalid_fk_client) if random.random() < 0.07 else random.choice(client_ids)

    rows_proj.append((
        pid, pname, ptype, muni, addr, status,
        start_str, planned_str, actual_str,
        contract_val, floor_area, pm_id, cid
    ))

# Inject ~25 duplicate PKs
for _ in range(25):
    dup = random.choice(valid_proj_ids)
    rows_proj.append((
        dup, random.choice(project_names), random.choice(project_types),
        random.choice(municipalities), "Duplicate Street 1, HKI",
        random.choice(statuses_valid), str(rand_date(2025, 2026)),
        str(rand_date(2025, 2026)), None,
        round(random.uniform(1_000_000, 10_000_000), 2),
        round(random.uniform(500, 10_000), 1),
        random.choice(valid_emp_ids), random.choice(client_ids)
    ))

random.shuffle(rows_proj)

df_dim_projects = spark.createDataFrame(rows_proj, schema=StructType([
    StructField("project_id",         StringType(),  True),
    StructField("project_name",       StringType(),  True),
    StructField("project_type",       StringType(),  True),
    StructField("municipality",       StringType(),  True),
    StructField("site_address",       StringType(),  True),
    StructField("status",             StringType(),  True),
    StructField("start_date",         StringType(),  True),   # intentionally string
    StructField("planned_end_date",   StringType(),  True),   # intentionally string
    StructField("actual_end_date",    StringType(),  True),   # intentionally string
    StructField("contract_value_eur", DoubleType(),  True),
    StructField("floor_area_m2",      DoubleType(),  True),
    StructField("project_manager_id", StringType(),  True),
    StructField("client_id",          StringType(),  True),
]))

print(f"dim_projects: {df_dim_projects.count()} rows")
display(df_dim_projects.limit(10))

# COMMAND ----------

# DBTITLE 1,dim_vendors — ~1000 rows
# ========================================
# dim_vendors — ~1,000 records
# Messy: booleans as strings, malformed Y-tunnus, inconsistent enums,
#        duplicate PKs, null PKs
# ========================================

vt_valid = ["subcontractor","material_supplier","equipment_rental","professional_services"]
vt_messy = ["Subcontractor","MATERIAL_SUPPLIER","sub contractor","supplier","equip rental","","unknown"]
tc_valid = ["concrete","MEP","roofing","finishing","earthworks","glazing","steel"]
tc_messy = ["Concrete","mep","ROOFING","finising","earth works","","unknown","electrical"]
db_valid = ["low","medium","high"]
db_messy = ["Low","HIGH","Medium","very_high","unknown","","N/A"]
ap_valid = ["approved","probation","suspended","blacklisted"]
ap_messy = ["Approved","SUSPENDED","aproved","on_probation","blocked","","unknown"]

cities = ["Helsinki","Espoo","Vantaa","Turku","Tampere","Oulu"]

rows_vnd = []
for i in range(1, NUM_VENDORS + 1):
    vid   = maybe_null(f"VND-{str(i).zfill(4)}", 0.03)
    vname = maybe_null(f"{random.choice(vendor_names_seed)} ({random.choice(cities)})", 0.04)
    vtype = messy_enum(vt_valid, vt_messy, 0.12)
    trade = messy_enum(tc_valid, tc_messy, 0.12)

    # Finnish Y-tunnus: 1234567-8
    biz_id = f"{random.randint(1_000_000, 9_999_999)}-{random.randint(0, 9)}"
    biz_id = maybe_null(biz_id, 0.06)
    if random.random() < 0.03:
        biz_id = random.choice(["12345", "ABCDEFG-H", "0000000-0", ""])

    db_rating = messy_enum(db_valid, db_messy, 0.10)

    # rala_certified — boolean stored as string (Bronze messiness)
    rala = random.choice([True, False])
    if random.random() < 0.08:
        rala = random.choice(["yes","no","Y","N","true","1",""])

    approved = messy_enum(ap_valid, ap_messy, 0.12)

    rows_vnd.append((vid, vname, vtype, trade, biz_id, db_rating, str(rala), approved))

# Inject ~20 duplicate PKs
for _ in range(20):
    dup = random.choice(valid_vendor_ids)
    rows_vnd.append((
        dup, random.choice(vendor_names_seed), random.choice(vt_valid),
        random.choice(tc_valid),
        f"{random.randint(1_000_000, 9_999_999)}-{random.randint(0, 9)}",
        random.choice(db_valid), str(random.choice([True, False])),
        random.choice(ap_valid)
    ))

random.shuffle(rows_vnd)

df_dim_vendors = spark.createDataFrame(rows_vnd, schema=StructType([
    StructField("vendor_id",             StringType(), True),
    StructField("vendor_name",           StringType(), True),
    StructField("vendor_type",           StringType(), True),
    StructField("trade_category",        StringType(), True),
    StructField("business_id",           StringType(), True),
    StructField("dun_bradstreet_rating", StringType(), True),
    StructField("rala_certified",        StringType(), True),  # intentionally string
    StructField("approved_status",       StringType(), True),
]))

print(f"dim_vendors: {df_dim_vendors.count()} rows")
display(df_dim_vendors.limit(10))

# COMMAND ----------

# DBTITLE 1,fact_schedule_milestones — ~2000 rows
# ========================================
# fact_schedule_milestones — ~2,000 records
# Messy: wrong delay_days, invalid FKs, actual_date for incomplete,
#        dates as strings, outlier delays
# ========================================

ms_names = [
    "Foundation complete","Roof seal","MEP rough-in","Takt zone handover",
    "Structural frame","Interior finishing","Facade complete","Elevator install",
    "Fire safety inspection","Final inspection","Handover prep","Landscaping complete",
    "Mechanical completion","Electrical rough-in","Plumbing complete","HVAC commissioning"]
takt_zones = ["A1","A2","A3","B1","B2","B3","C1","C2","D1","D2","E1"]
ms_valid   = ["on_track","at_risk","delayed","complete"]
ms_messy   = ["On_Track","AT_RISK","Delayed","COMPLETE","on track","pending","overdue",""]

rows_ms = []
for i in range(1, NUM_MILESTONES + 1):
    mid = maybe_null(f"MS-{str(i).zfill(5)}", 0.02)
    pid = random.choice(invalid_fk_proj) if random.random() < 0.08 else random.choice(valid_proj_ids)

    mname = maybe_null(random.choice(ms_names), 0.04)
    tz    = maybe_null(random.choice(takt_zones), 0.06)

    pd_dt = rand_date(2025, 2026)
    fd_dt = pd_dt + timedelta(days=random.randint(-15, 60))

    planned_str  = messy_date_str(pd_dt)
    forecast_str = messy_date_str(fd_dt)

    status = messy_enum(ms_valid, ms_messy, 0.12)

    if status in ["complete", "COMPLETE"]:
        ad = pd_dt + timedelta(days=random.randint(-10, 45))
        actual_str = messy_date_str(ad)
        real_delay = (ad - pd_dt).days
        # 15% deliberately wrong delay_days
        delay = random.randint(-100, 200) if random.random() < 0.15 else real_delay
    else:
        actual_str = None
        delay = None
        # 5% logical error: actual_date set despite not complete
        if random.random() < 0.05:
            actual_str = str(rand_date(2025, 2026))
            delay = random.randint(-30, 90)

    # 3% extreme outlier delays
    if delay is not None and random.random() < 0.03:
        delay = random.choice([-999, 9999, 0])

    rows_ms.append((mid, pid, mname, tz, planned_str, forecast_str, actual_str, delay, status))

# Inject ~35 duplicate PKs
for _ in range(35):
    dup = f"MS-{str(random.randint(1, NUM_MILESTONES)).zfill(5)}"
    rows_ms.append((
        dup, random.choice(valid_proj_ids), random.choice(ms_names),
        random.choice(takt_zones), str(rand_date(2025, 2026)),
        str(rand_date(2025, 2026)), str(rand_date(2025, 2026)),
        random.randint(-20, 60), random.choice(ms_valid)
    ))

random.shuffle(rows_ms)

df_fact_milestones = spark.createDataFrame(rows_ms, schema=StructType([
    StructField("milestone_id",     StringType(),  True),
    StructField("project_id",       StringType(),  True),
    StructField("milestone_name",   StringType(),  True),
    StructField("takt_zone",        StringType(),  True),
    StructField("planned_date",     StringType(),  True),  # intentionally string
    StructField("forecast_date",    StringType(),  True),  # intentionally string
    StructField("actual_date",      StringType(),  True),  # intentionally string
    StructField("delay_days",       IntegerType(), True),
    StructField("milestone_status", StringType(),  True),
]))

print(f"fact_schedule_milestones: {df_fact_milestones.count()} rows")
display(df_fact_milestones.limit(10))

# COMMAND ----------

# DBTITLE 1,fact_project_costs — ~1800 rows
# ========================================
# fact_project_costs — ~1,800 records
# Messy: wrong variance_eur (~15%), negative budgets/actuals,
#        period_month in mixed string formats, invalid FKs
# ========================================

cc_valid = ["labour","materials","subcontract","equipment","overhead","design"]
cc_messy = ["Labour","MATERIALS","sub-contract","equip","Overhead","","unknown","misc"]

rows_cost = []
for i in range(1, NUM_COSTS + 1):
    cid = maybe_null(f"CST-{str(i).zfill(5)}", 0.02)
    pid = random.choice(invalid_fk_proj) if random.random() < 0.08 else random.choice(valid_proj_ids)
    cat = messy_enum(cc_valid, cc_messy, 0.12)

    budget    = round(random.uniform(10_000, 2_000_000), 2)
    committed = round(budget * random.uniform(0.3, 1.2), 2)
    actual    = round(budget * random.uniform(0.2, 1.4), 2)

    # 5% negative budgets / actuals
    if random.random() < 0.05:
        budget = -abs(budget)
    if random.random() < 0.05:
        actual = -abs(actual)

    # Variance: correct = budget - actual
    real_var = round(budget - actual, 2)
    variance = round(random.uniform(-500_000, 500_000), 2) if random.random() < 0.15 else real_var

    # period_month — first of month, sometimes wrong format
    yr = random.choice([2025, 2026])
    mn = random.randint(1, 12)
    pm = date(yr, mn, 1)
    if random.random() < 0.08:
        period_str = pm.strftime(random.choice(["%Y-%m", "%d/%m/%Y", "%B %Y"]))
    else:
        period_str = str(pm)

    rows_cost.append((cid, pid, cat, budget, committed, actual, variance, period_str))

# Inject ~25 duplicate PKs
for _ in range(25):
    dup = f"CST-{str(random.randint(1, NUM_COSTS)).zfill(5)}"
    rows_cost.append((
        dup, random.choice(valid_proj_ids), random.choice(cc_valid),
        round(random.uniform(50_000, 500_000), 2),
        round(random.uniform(30_000, 400_000), 2),
        round(random.uniform(20_000, 450_000), 2),
        round(random.uniform(-100_000, 100_000), 2),
        str(date(random.choice([2025, 2026]), random.randint(1, 12), 1))
    ))

random.shuffle(rows_cost)

df_fact_costs = spark.createDataFrame(rows_cost, schema=StructType([
    StructField("cost_id",       StringType(), True),
    StructField("project_id",    StringType(), True),
    StructField("cost_category", StringType(), True),
    StructField("budget_eur",    DoubleType(), True),
    StructField("committed_eur", DoubleType(), True),
    StructField("actual_eur",    DoubleType(), True),
    StructField("variance_eur",  DoubleType(), True),
    StructField("period_month",  StringType(), True),  # intentionally string
]))

print(f"fact_project_costs: {df_fact_costs.count()} rows")
display(df_fact_costs.limit(10))

# COMMAND ----------

# DBTITLE 1,fact_purchase_orders — ~2500 rows
# ========================================
# fact_purchase_orders — ~2,500 records
# Messy: over-invoiced POs, negative values, invalid vendor/project FKs,
#        dates as strings, inconsistent enums
# ========================================

poc_valid = ["material","subcontract","equipment_rental","professional_services"]
poc_messy = ["Material","SUBCONTRACT","equipment rental","prof_services","equip","","other"]
pos_valid = ["open","partially_invoiced","closed","cancelled"]
pos_messy = ["Open","CLOSED","partial_invoiced","canceled","cancled","","unknown","pending"]

rows_po = []
for i in range(1, NUM_POS + 1):
    po_id = maybe_null(f"PO-{str(i).zfill(5)}", 0.02)
    pid   = random.choice(invalid_fk_proj)   if random.random() < 0.08 else random.choice(valid_proj_ids)
    vid   = random.choice(invalid_fk_vendor) if random.random() < 0.07 else random.choice(valid_vendor_ids)

    po_dt     = rand_date(2025, 2026)
    po_dt_str = messy_date_str(po_dt)

    cat = messy_enum(poc_valid, poc_messy, 0.12)

    po_val = round(random.uniform(5_000, 2_000_000), 2)
    if random.random() < 0.04:
        po_val = -abs(po_val)

    # invoiced can exceed PO value (over-invoicing)
    invoiced = round(po_val * random.uniform(0, 1.3), 2)
    if random.random() < 0.04:
        invoiced = -abs(invoiced)

    status = messy_enum(pos_valid, pos_messy, 0.12)

    rows_po.append((po_id, pid, vid, po_dt_str, cat, po_val, invoiced, status))

# Inject ~40 duplicate PKs
for _ in range(40):
    dup = random.choice(valid_po_ids)
    rows_po.append((
        dup, random.choice(valid_proj_ids), random.choice(valid_vendor_ids),
        str(rand_date(2025, 2026)), random.choice(poc_valid),
        round(random.uniform(10_000, 500_000), 2),
        round(random.uniform(5_000, 400_000), 2),
        random.choice(pos_valid)
    ))

random.shuffle(rows_po)

df_fact_pos = spark.createDataFrame(rows_po, schema=StructType([
    StructField("po_id",         StringType(), True),
    StructField("project_id",    StringType(), True),
    StructField("vendor_id",     StringType(), True),
    StructField("po_date",       StringType(), True),  # intentionally string
    StructField("po_category",   StringType(), True),
    StructField("po_value_eur",  DoubleType(), True),
    StructField("invoiced_eur",  DoubleType(), True),
    StructField("po_status",     StringType(), True),
]))

print(f"fact_purchase_orders: {df_fact_pos.count()} rows")
display(df_fact_pos.limit(10))

# COMMAND ----------

# DBTITLE 1,fact_invoices — ~2500 rows
# ========================================
# fact_invoices — ~2,500 records
# Messy: paid_date before invoice_date, wrong days_overdue,
#        paid_date set for unpaid, days_overdue set for paid,
#        negative amounts, messy payment statuses
# ========================================

ps_valid = ["pending","approved","paid","disputed","overdue"]
ps_messy = ["Pending","APPROVED","Paid","Disputed","OVERDUE","payed","aproved","","unknown"]

rows_inv = []
for i in range(1, NUM_INVOICES + 1):
    inv_id = maybe_null(f"INV-{str(i).zfill(5)}", 0.02)
    po_id  = random.choice(invalid_fk_po) if random.random() < 0.08 else random.choice(valid_po_ids)
    vid    = random.choice(invalid_fk_vendor) if random.random() < 0.07 else random.choice(valid_vendor_ids)

    inv_dt = rand_date(2025, 2026)
    due_dt = inv_dt + timedelta(days=random.choice([14, 21, 30, 45, 60]))

    pay_status = messy_enum(ps_valid, ps_messy, 0.12)

    # --- paid_date logic ---
    paid_date_str = None
    if pay_status in ["paid", "Paid", "payed"]:
        paid_dt = due_dt + timedelta(days=random.randint(-15, 30))
        # 8% logical error: paid before invoice date
        if random.random() < 0.08:
            paid_dt = inv_dt - timedelta(days=random.randint(10, 180))
        paid_date_str = messy_date_str(paid_dt)
    else:
        # 4% logical error: paid_date set for non-paid status
        if random.random() < 0.04:
            paid_date_str = str(rand_date(2025, 2026))

    inv_date_str = messy_date_str(inv_dt)
    due_date_str = messy_date_str(due_dt)

    amount = round(random.uniform(1_000, 500_000), 2)
    if random.random() < 0.04:
        amount = -abs(amount)

    # --- days_overdue ---
    if pay_status in ["paid", "Paid", "payed"]:
        days_od = None
        # 5% wrong: days_overdue set even when paid
        if random.random() < 0.05:
            days_od = random.randint(1, 90)
    else:
        real_od = (date(2026, 3, 31) - due_dt).days
        if real_od > 0:
            days_od = real_od
            # 10% wrong calculation
            if random.random() < 0.10:
                days_od = random.choice([-abs(real_od), real_od * 2, random.randint(-100, 0)])
        else:
            days_od = None

    rows_inv.append((inv_id, po_id, vid, inv_date_str, due_date_str,
                     paid_date_str, amount, pay_status, days_od))

# Inject ~35 duplicate PKs
for _ in range(35):
    dup = f"INV-{str(random.randint(1, NUM_INVOICES)).zfill(5)}"
    rows_inv.append((
        dup, random.choice(valid_po_ids), random.choice(valid_vendor_ids),
        str(rand_date(2025, 2026)), str(rand_date(2025, 2026)),
        str(rand_date(2025, 2026)),
        round(random.uniform(5_000, 200_000), 2),
        random.choice(ps_valid), random.randint(0, 60)
    ))

random.shuffle(rows_inv)

df_fact_invoices = spark.createDataFrame(rows_inv, schema=StructType([
    StructField("invoice_id",     StringType(),  True),
    StructField("po_id",          StringType(),  True),
    StructField("vendor_id",      StringType(),  True),
    StructField("invoice_date",   StringType(),  True),  # intentionally string
    StructField("due_date",       StringType(),  True),  # intentionally string
    StructField("paid_date",      StringType(),  True),  # intentionally string
    StructField("amount_eur",     DoubleType(),  True),
    StructField("payment_status", StringType(),  True),
    StructField("days_overdue",   IntegerType(), True),
]))

print(f"fact_invoices: {df_fact_invoices.count()} rows")
display(df_fact_invoices.limit(10))

# COMMAND ----------

# DBTITLE 1,fact_subcontractor_performance — ~1500 rows
# ========================================
# fact_subcontractor_performance — ~1,500 records
# Messy: out-of-range scores, wrong overall_score (~20%),
#        booleans as strings, invalid FKs
# ========================================

def messy_score(pct_bad=0.08):
    """Return 1-10 score with occasional out-of-range / null values."""
    if random.random() < pct_bad:
        return random.choice([-1, 0, 11, 15, 100, None])
    return random.randint(1, 10)

rows_perf = []
for i in range(1, NUM_PERF + 1):
    perf_id = maybe_null(f"PRF-{str(i).zfill(5)}", 0.02)
    vid     = random.choice(invalid_fk_vendor) if random.random() < 0.07 else random.choice(valid_vendor_ids)
    pid     = random.choice(invalid_fk_proj)   if random.random() < 0.08 else random.choice(valid_proj_ids)

    eval_dt_str = messy_date_str(rand_date(2025, 2026))

    quality  = messy_score()
    schedule = messy_score()
    safety   = messy_score()
    comms    = messy_score()

    # overall_score: correct = quality*0.4 + schedule*0.3 + safety*0.2 + comms*0.1
    if all(s is not None for s in [quality, schedule, safety, comms]):
        real_overall = round(quality * 0.4 + schedule * 0.3 + safety * 0.2 + comms * 0.1, 2)
        # 20% deliberately wrong
        overall = round(random.uniform(0, 15), 2) if random.random() < 0.20 else real_overall
    else:
        overall = maybe_null(round(random.uniform(1, 10), 2), 0.30)

    # would_rehire — boolean stored as string
    rehire = random.choice([True, False])
    if random.random() < 0.08:
        rehire = random.choice(["yes","no","Y","N","maybe","1","0",""])

    rows_perf.append((perf_id, vid, pid, eval_dt_str,
                      quality, schedule, safety, comms, overall, str(rehire)))

# Inject ~25 duplicate PKs
for _ in range(25):
    dup = f"PRF-{str(random.randint(1, NUM_PERF)).zfill(5)}"
    rows_perf.append((
        dup, random.choice(valid_vendor_ids), random.choice(valid_proj_ids),
        str(rand_date(2025, 2026)),
        random.randint(1, 10), random.randint(1, 10),
        random.randint(1, 10), random.randint(1, 10),
        round(random.uniform(3, 9), 2), str(random.choice([True, False]))
    ))

random.shuffle(rows_perf)

df_fact_perf = spark.createDataFrame(rows_perf, schema=StructType([
    StructField("perf_id",             StringType(),  True),
    StructField("vendor_id",           StringType(),  True),
    StructField("project_id",          StringType(),  True),
    StructField("evaluation_date",     StringType(),  True),  # intentionally string
    StructField("quality_score",       IntegerType(), True),
    StructField("schedule_score",      IntegerType(), True),
    StructField("safety_score",        IntegerType(), True),
    StructField("communication_score", IntegerType(), True),
    StructField("overall_score",       DoubleType(),  True),
    StructField("would_rehire",        StringType(),  True),  # intentionally string
]))

print(f"fact_subcontractor_performance: {df_fact_perf.count()} rows")
display(df_fact_perf.limit(10))

# COMMAND ----------

# DBTITLE 1,Save all tables to Delta + verification summary
# ========================================
# Save all 8 tables as Delta into bronze schema
# ========================================

tables = {
    "bronze.dim_employee":                   df_dim_employee,
    "bronze.dim_projects":                   df_dim_projects,
    "bronze.dim_vendors":                    df_dim_vendors,
    "bronze.fact_schedule_milestones":        df_fact_milestones,
    "bronze.fact_project_costs":              df_fact_costs,
    "bronze.fact_purchase_orders":            df_fact_pos,
    "bronze.fact_invoices":                   df_fact_invoices,
    "bronze.fact_subcontractor_performance":  df_fact_perf,
}

for table_name, df in tables.items():
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    cnt = spark.table(table_name).count()
    print(f"  {table_name}: {cnt} rows saved")

print("\n" + "=" * 70)
print("BRONZE LAYER DATA QUALITY ISSUES SUMMARY")
print("=" * 70)

for table_name in tables:
    df = spark.table(table_name)
    total  = df.count()
    pk_col = df.columns[0]
    null_pks = df.filter(F.col(pk_col).isNull()).count()
    dup_pks  = df.groupBy(pk_col).count().filter("count > 1").count()
    print(f"\n  {table_name}")
    print(f"    Total rows:    {total}")
    print(f"    Null PKs:      {null_pks}")
    print(f"    Duplicate PKs: {dup_pks}")

print("\n" + "=" * 70)
print("INTENTIONAL DATA ISSUES INJECTED:")
print("=" * 70)
print("""
  Null primary keys (~2-3% per table)
  Duplicate primary keys (20-40 per table)
  Invalid foreign keys (~7-8% of FK columns)
  Dates stored as strings in mixed formats (dd/mm/yyyy, yyyymmdd, etc.)
  Negative values in monetary and area columns (~4-5%)
  Invalid / inconsistent enum values and casing (~10-12%)
  Incorrect calculated fields (variance_eur, delay_days, overall_score)
  Logical errors (paid_date < invoice_date, actual_end < start_date)
  Boolean fields stored as strings (yes/no/Y/N/1/0/true/false)
  Malformed Finnish business IDs (Y-tunnus)
""")

# COMMAND ----------


