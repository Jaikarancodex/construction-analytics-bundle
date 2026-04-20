-- ============================================================
-- Gold: schedule_kpi
-- Milestone completion rates by project and takt zone
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW gold_schedule_kpi
COMMENT 'Milestone completion rates by project and takt zone'
AS
SELECT
  m.project_id,
  p.project_name,
  m.takt_zone,
  COUNT(*)                                     AS total_milestones,
  SUM(CASE WHEN m.status_clean = 'complete' THEN 1 ELSE 0 END)  AS completed_milestones,
  SUM(CASE WHEN m.status_clean = 'delayed'  THEN 1 ELSE 0 END)  AS delayed_milestones,
  ROUND(SUM(CASE WHEN m.status_clean = 'complete' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS completion_rate_pct,
  ROUND(AVG(CASE WHEN m.status_clean = 'complete' THEN DATEDIFF(m.actual_dt, m.planned_dt) END), 1) AS avg_delay_days
FROM silver_fact_schedule_milestones m
LEFT JOIN silver_dim_projects p ON m.project_id = p.project_id
GROUP BY m.project_id, p.project_name, m.takt_zone;
