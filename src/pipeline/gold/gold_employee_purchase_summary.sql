-- ============================================================
-- Gold: employee_purchase_summary
-- Purchase order metrics per employee (project manager)
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW gold_employee_purchase_summary
COMMENT 'Purchase order metrics per employee (project manager)'
AS
SELECT
  e.employee_id,
  e.full_name,
  e.role_clean,
  e.department_clean,
  COUNT(DISTINCT po.po_id)                          AS total_po_count,
  ROUND(SUM(po.po_value_eur_clean), 2)              AS total_po_value_eur,
  ROUND(SUM(po.invoiced_eur_clean), 2)              AS total_invoiced_eur,
  SUM(CASE WHEN po.is_over_invoiced = TRUE THEN 1 ELSE 0 END) AS over_invoiced_count,
  ROUND(
    SUM(CASE WHEN po.is_over_invoiced = TRUE THEN 1 ELSE 0 END) * 100.0
      / NULLIF(COUNT(DISTINCT po.po_id), 0), 2
  )                                                  AS over_invoiced_pct,
  COUNT(DISTINCT p.project_id)                       AS project_count,
  ROUND(AVG(po.po_value_eur_clean), 2)              AS avg_po_value_eur
FROM silver_dim_employee e
INNER JOIN silver_dim_projects p
  ON p.project_manager_id = e.employee_id
INNER JOIN silver_fact_purchase_orders po
  ON po.project_id = p.project_id
GROUP BY e.employee_id, e.full_name, e.role_clean, e.department_clean;
