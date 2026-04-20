-- ============================================================
-- Silver: fact_project_costs
-- Cleansed costs with recalculated variance
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_project_costs(
  CONSTRAINT valid_cost_id   EXPECT (cost_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_budget EXPECT (ABS(budget_eur) >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed costs with recalculated variance'
AS
SELECT
  *,
  ABS(budget_eur)                AS budget_eur_clean,
  ABS(actual_eur)                AS actual_eur_clean,
  ROUND(ABS(budget_eur) - ABS(actual_eur), 2) AS variance_eur_corrected,
  LOWER(TRIM(cost_category))     AS cost_category_clean,
  COALESCE(
    try_to_date(period_month, 'yyyy-MM-dd'),
    try_to_date(CONCAT(period_month, '-01'), 'yyyy-MM-dd'),
    try_to_date(period_month, 'dd/MM/yyyy')
  ) AS period_month_dt,
  current_timestamp()            AS ingestion_timestamp
FROM bronze_fact_project_costs
QUALIFY ROW_NUMBER() OVER (PARTITION BY cost_id ORDER BY cost_id) = 1;
