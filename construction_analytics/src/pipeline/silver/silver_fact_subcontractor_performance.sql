-- ============================================================
-- Silver: fact_subcontractor_performance
-- Cleansed performance with corrected overall_score and range checks
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_subcontractor_performance(
  CONSTRAINT valid_perf_id   EXPECT (perf_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT quality_in_range EXPECT (quality_score_clean BETWEEN 1 AND 10 OR quality_score_clean IS NULL),
  CONSTRAINT safety_in_range  EXPECT (safety_score_clean BETWEEN 1 AND 10 OR safety_score_clean IS NULL)
)
COMMENT 'Cleansed performance with corrected overall_score and range expectations'
AS
SELECT
  *,
  CASE WHEN quality_score BETWEEN 1 AND 10 THEN quality_score ELSE NULL END AS quality_score_clean,
  CASE WHEN schedule_score BETWEEN 1 AND 10 THEN schedule_score ELSE NULL END AS schedule_score_clean,
  CASE WHEN safety_score BETWEEN 1 AND 10 THEN safety_score ELSE NULL END AS safety_score_clean,
  CASE WHEN communication_score BETWEEN 1 AND 10 THEN communication_score ELSE NULL END AS communication_score_clean,
  ROUND(
    CASE WHEN quality_score BETWEEN 1 AND 10 THEN quality_score ELSE NULL END * 0.4 +
    CASE WHEN schedule_score BETWEEN 1 AND 10 THEN schedule_score ELSE NULL END * 0.3 +
    CASE WHEN safety_score BETWEEN 1 AND 10 THEN safety_score ELSE NULL END * 0.2 +
    CASE WHEN communication_score BETWEEN 1 AND 10 THEN communication_score ELSE NULL END * 0.1,
  2) AS overall_score_corrected,
  CASE LOWER(TRIM(would_rehire))
    WHEN 'true' THEN TRUE WHEN 'yes' THEN TRUE WHEN 'y' THEN TRUE WHEN '1' THEN TRUE
    WHEN 'false' THEN FALSE WHEN 'no' THEN FALSE WHEN 'n' THEN FALSE WHEN '0' THEN FALSE
    ELSE NULL
  END AS would_rehire_bool,
  COALESCE(try_to_date(evaluation_date, 'yyyy-MM-dd'), try_to_date(evaluation_date, 'dd/MM/yyyy'), try_to_date(evaluation_date, 'yyyyMMdd')) AS evaluation_dt,
  current_timestamp() AS ingestion_timestamp
FROM bronze_fact_subcontractor_performance
QUALIFY ROW_NUMBER() OVER (PARTITION BY perf_id ORDER BY perf_id) = 1;
