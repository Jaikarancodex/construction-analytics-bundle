-- ============================================================
-- Bronze: dim_vendors
-- Ingest raw vendor dimension from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_dim_vendors
COMMENT 'Raw vendor data from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.dim_vendors;
