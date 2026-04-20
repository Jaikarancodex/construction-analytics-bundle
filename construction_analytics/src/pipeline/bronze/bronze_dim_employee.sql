-- ============================================================
-- Bronze: dim_employee
-- Ingest raw employee dimension from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_dim_employee
COMMENT 'Raw employee data from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.dim_employee;
