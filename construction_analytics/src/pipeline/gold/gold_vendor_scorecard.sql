-- ============================================================
-- Gold: vendor_scorecard
-- Composite vendor performance with invoice behavior
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW gold_vendor_scorecard
COMMENT 'Composite vendor performance with invoice behavior'
AS
SELECT
  v.vendor_id,
  v.vendor_name,
  v.vendor_type_clean     AS vendor_type,
  v.trade_category_clean  AS trade_category,
  ROUND(AVG(sp.overall_score_corrected), 2) AS avg_overall_score,
  ROUND(AVG(sp.quality_score_clean), 2)     AS avg_quality_score,
  ROUND(AVG(sp.safety_score_clean), 2)      AS avg_safety_score,
  COUNT(DISTINCT sp.perf_id)                AS evaluation_count,
  ROUND(SUM(CASE WHEN sp.would_rehire_bool = TRUE THEN 1 ELSE 0 END) * 100.0
    / NULLIF(COUNT(sp.perf_id), 0), 2)      AS rehire_rate_pct
FROM silver_dim_vendors v
LEFT JOIN silver_fact_subcontractor_performance sp ON v.vendor_id = sp.vendor_id
GROUP BY v.vendor_id, v.vendor_name, v.vendor_type_clean, v.trade_category_clean;
