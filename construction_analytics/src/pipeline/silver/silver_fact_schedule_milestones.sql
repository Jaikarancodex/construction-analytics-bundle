-- ============================================================
-- Silver: fact_schedule_milestones
-- Cleansed milestones with recalculated delay_days
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_schedule_milestones(
  CONSTRAINT valid_milestone_id EXPECT (milestone_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed milestones with recalculated delay_days'
AS
SELECT
  *,
  COALESCE(try_to_date(planned_date, 'yyyy-MM-dd'), try_to_date(planned_date, 'dd/MM/yyyy'), try_to_date(planned_date, 'yyyyMMdd')) AS planned_dt,
  COALESCE(try_to_date(forecast_date, 'yyyy-MM-dd'), try_to_date(forecast_date, 'dd/MM/yyyy'), try_to_date(forecast_date, 'yyyyMMdd')) AS forecast_dt,
  COALESCE(try_to_date(actual_date, 'yyyy-MM-dd'), try_to_date(actual_date, 'dd/MM/yyyy'), try_to_date(actual_date, 'yyyyMMdd')) AS actual_dt,
  LOWER(TRIM(milestone_status)) AS status_clean,
  current_timestamp()           AS ingestion_timestamp
FROM bronze_fact_schedule_milestones
QUALIFY ROW_NUMBER() OVER (PARTITION BY milestone_id ORDER BY milestone_id) = 1;
