-- ============================================================
-- Silver: dim_employee
-- Cleansed employees with dedup, standardized roles/departments
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_dim_employee(
  CONSTRAINT valid_employee_id EXPECT (employee_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_full_name   EXPECT (full_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT no_empty_role     EXPECT (LENGTH(TRIM(role)) > 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed employees with standardized roles and departments'
AS
SELECT
  *,
  LOWER(TRIM(role))       AS role_clean,
  LOWER(TRIM(department)) AS department_clean,
  current_timestamp()     AS ingestion_timestamp
FROM bronze_dim_employee
QUALIFY ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY employee_id) = 1;
