-- ============================================================
-- Bronze: fact_purchase_orders
-- Ingest raw purchase orders from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_fact_purchase_orders
COMMENT 'Raw purchase orders from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.fact_purchase_orders;
