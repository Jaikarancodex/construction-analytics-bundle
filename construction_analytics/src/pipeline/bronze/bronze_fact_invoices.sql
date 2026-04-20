-- ============================================================
-- Bronze: fact_invoices
-- Ingest raw invoices from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_fact_invoices
COMMENT 'Raw invoices from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.fact_invoices;
