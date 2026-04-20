-- ============================================================
-- Bronze: fact_project_costs
-- Ingest raw project costs from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_fact_project_costs
COMMENT 'Raw project costs from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.fact_project_costs;
