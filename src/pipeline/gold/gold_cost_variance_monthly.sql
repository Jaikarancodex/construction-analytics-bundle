-- ============================================================
-- Gold: cost_variance_monthly
-- Monthly cost variance by project and category
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW gold_cost_variance_monthly
COMMENT 'Monthly cost variance by project and category'
AS
SELECT
  c.project_id,
  p.project_name,
  p.project_type_clean       AS project_type,
  c.cost_category_clean      AS cost_category,
  DATE_FORMAT(c.period_month_dt, 'yyyy-MM') AS period_year_month,
  SUM(c.budget_eur_clean)    AS total_budget_eur,
  SUM(c.actual_eur_clean)    AS total_actual_eur,
  SUM(c.variance_eur_corrected) AS total_variance_eur,
  ROUND(SUM(c.actual_eur_clean) / NULLIF(SUM(c.budget_eur_clean), 0) * 100, 2) AS budget_utilization_pct
FROM silver_fact_project_costs c
LEFT JOIN silver_dim_projects p ON c.project_id = p.project_id
GROUP BY c.project_id, p.project_name, p.project_type_clean, c.cost_category_clean,
  DATE_FORMAT(c.period_month_dt, 'yyyy-MM');
