-- ============================================================
-- Bronze: fact_schedule_milestones
-- Ingest raw schedule milestones from source bronze layer
-- ============================================================
CREATE OR REFRESH STREAMING TABLE bronze_fact_schedule_milestones
COMMENT 'Raw schedule milestones from source bronze'
AS SELECT * FROM STREAM construction_demo.bronze.fact_schedule_milestones;
