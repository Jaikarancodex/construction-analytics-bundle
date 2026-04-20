# Databricks notebook source
# DBTITLE 1,Header — Genie Space
# MAGIC %md
# MAGIC # 06 — Create Genie Space: Natural Language Q&A
# MAGIC ## Construction Analytics — Self-Service Data Exploration
# MAGIC
# MAGIC Creates a **Genie Space** backed by the Gold tables, allowing business users to ask questions in natural language:
# MAGIC * *"Which projects are over budget?"*
# MAGIC * *"Show vendor scores below 5"*
# MAGIC * *"What is the average milestone delay for residential projects?"*

# COMMAND ----------

# DBTITLE 1,Setup — Genie Space Configuration
import requests
import json

catalog_name = "construction_demo"

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Tables for Genie Space
genie_tables = [
    f"{catalog_name}.gold.gold_cost_variance_monthly",
    f"{catalog_name}.gold.gold_schedule_kpi",
    f"{catalog_name}.gold.gold_vendor_scorecard",
    f"{catalog_name}.gold.gold_invoice_aging",
    f"{catalog_name}.gold.gold_project_summary",
    f"{catalog_name}.gold.ml_project_risk_scores",
]

print("✅ Genie Space configuration ready")

# COMMAND ----------

# DBTITLE 1,Create Genie Space via API
# ============================================================
# Create the Genie Space
# ============================================================

# # --- Step 1: Delete existing Genie Space if it was already created ---
# existing_genie_id = "01f139594cd2154a8f7ce8df2aa393fa"
# try:
#     del_resp = requests.delete(
#         f"https://{workspace_url}/api/2.0/genie/spaces/{existing_genie_id}",
#         headers=headers
#     )
#     if del_resp.status_code == 200:
#         print(f"🗑️ Deleted existing Genie Space: {existing_genie_id}")
#     else:
#         print(f"   Existing space delete returned {del_resp.status_code} (may not exist, continuing...)")
# except Exception as e:
#     print(f"   Delete step skipped: {e}")

# ---Ensure target folder exists ---
parent_path = "/Workspace/Users/jaikaran.n@diggibyte.com/construction-project/genie_space"
try:
    mkdirs_resp = requests.post(
        f"https://{workspace_url}/api/2.0/workspace/mkdirs",
        headers=headers,
        json={"path": parent_path}
    )
    if mkdirs_resp.status_code == 200:
        print(f"📁 Folder ready: {parent_path}")
    else:
        print(f"   Folder creation returned {mkdirs_resp.status_code}: {mkdirs_resp.text}")
except Exception as e:
    print(f"   Folder creation error: {e}")

# --- Get a warehouse (prefer running, fall back to any available) ---
warehouse_id = "8f4eab898acf8786"  # fallback
try:
    wh_resp = requests.get(
        f"https://{workspace_url}/api/2.0/sql/warehouses",
        headers=headers
    )
    if wh_resp.status_code == 200:
        warehouses = wh_resp.json().get("warehouses", [])
        running_wh = [w for w in warehouses if w.get("state") == "RUNNING"]
        if running_wh:
            warehouse_id = running_wh[0]["id"]
            print(f"   Using running warehouse: {running_wh[0]['name']}")
        elif warehouses:
            warehouse_id = warehouses[0]["id"]
            print(f"   No running warehouses. Using: {warehouses[0]['name']} (state: {warehouses[0]['state']})")
except Exception as e:
    print(f"   Warehouse lookup error: {e}")

# --- Build the serialized_space JSON string (required by API) ---
# NOTE: tables MUST be sorted alphabetically by identifier
serialized_space_obj = {
    "version": 2,
    "config": {
        "sample_questions": [
            {"id": "a1b2c3d4e5f60000000000000000000a", "question": ["Which projects are over budget?"]},
            {"id": "b2c3d4e5f6a70000000000000000000b", "question": ["Show vendor scores below 5"]},
            {"id": "c3d4e5f6a7b80000000000000000000c", "question": ["What is the average milestone delay for residential projects?"]}
        ]
    },
    "data_sources": {
        "tables": [{"identifier": t} for t in sorted(genie_tables)]
    },
    "instructions": {
        "text_instructions": [
            {
                "id": "d4e5f6a7b8c90000000000000000000d",
                "content": ["This space contains analytics for Finnish construction projects. Use gold tables for project budgets, schedule milestones, vendor performance, invoice aging, and ML risk predictions."]
            }
        ]
    }
}

# --- Step 5: Create Genie Space with parent_path ---
genie_config = {
    "title": "Construction Analytics Control Tower",
    "description": "Self-service analytics for Finnish construction projects. Ask about project budgets, schedule milestones, vendor performance, invoice aging, and ML risk predictions.",
    "warehouse_id": warehouse_id,
    "parent_path": parent_path,
    "serialized_space": json.dumps(serialized_space_obj)
}

print(f"\n📡 Creating Genie Space in {parent_path}...")
response = requests.post(
    f"https://{workspace_url}/api/2.0/genie/spaces",
    headers=headers,
    json=genie_config
)

if response.status_code == 200:
    genie_space = response.json()
    genie_id = genie_space.get("space_id", genie_space.get("id"))
    print(f"✅ Genie Space created: Construction Analytics Control Tower")
    print(f"   ID: {genie_id}")
    print(f"   Location: {parent_path}")
else:
    print(f"⚠️ Genie Space creation failed: {response.status_code}")
    print(f"   Response: {response.text}")
    genie_id = None

# COMMAND ----------

# DBTITLE 1,Display Genie Space Link
# ============================================================
# Show link to the Genie Space
# ============================================================
if genie_id:
    displayHTML(f"""
    <div style="padding: 20px; border-left: 5px solid #4CAF50; background-color: #f0fff0; margin: 15px 0; border-radius: 8px;">
        <h3 style="margin: 0 0 15px 0; color: #4CAF50;">
            🧠 Construction Analytics Genie Space
        </h3>
        <p>Ask questions in natural language about your construction portfolio.</p>
        <p style="margin: 10px 0 0 0;">
            <a href='https://{workspace_url}/genie/rooms/{genie_id}' target='_blank' 
               style='background-color: #4CAF50; color: white; padding: 12px 24px; text-decoration: none; 
                      border-radius: 5px; font-weight: bold; display: inline-block; font-size: 16px;'>
               🚀 Open Genie Space →
            </a>
        </p>
    </div>
    """)
else:
    print("Create the Genie Space manually from the Genie UI with these tables:")
    for t in genie_tables:
        print(f"  - {t}")
