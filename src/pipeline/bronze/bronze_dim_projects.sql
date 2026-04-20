-- ============================================================
-- Bronze: dim_projects
-- Ingest raw project dimension from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_dim_projects
COMMENT 'Raw project data from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.dim_projects;
