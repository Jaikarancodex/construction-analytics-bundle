-- ============================================================
-- Bronze: fact_subcontractor_performance
-- Ingest raw subcontractor performance from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_fact_subcontractor_performance
COMMENT 'Raw subcontractor performance from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.fact_subcontractor_performance;
