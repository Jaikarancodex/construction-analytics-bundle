-- ============================================================
-- Silver: dim_projects
-- Cleansed projects with parsed dates and standardized enums
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_dim_projects(
  CONSTRAINT valid_project_id      EXPECT (project_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_contract_val EXPECT (contract_value_eur >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_floor_area   EXPECT (floor_area_m2 >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed projects with parsed dates and standardized enums'
AS
SELECT
  *,
  COALESCE(try_to_date(start_date, 'yyyy-MM-dd'), try_to_date(start_date, 'dd/MM/yyyy'), try_to_date(start_date, 'yyyyMMdd')) AS start_dt,
  COALESCE(try_to_date(planned_end_date, 'yyyy-MM-dd'), try_to_date(planned_end_date, 'dd/MM/yyyy'), try_to_date(planned_end_date, 'yyyyMMdd')) AS planned_end_dt,
  COALESCE(try_to_date(actual_end_date, 'yyyy-MM-dd'), try_to_date(actual_end_date, 'dd/MM/yyyy'), try_to_date(actual_end_date, 'yyyyMMdd')) AS actual_end_dt,
  ABS(contract_value_eur)            AS contract_value_eur_clean,
  ABS(floor_area_m2)                 AS floor_area_m2_clean,
  LOWER(TRIM(project_type))          AS project_type_clean,
  LOWER(TRIM(status))                AS status_clean,
  current_timestamp()                AS ingestion_timestamp
FROM bronze_dim_projects
QUALIFY ROW_NUMBER() OVER (PARTITION BY project_id ORDER BY project_id) = 1;
