-- ============================================================
-- Silver: dim_vendors
-- Cleansed vendors with parsed booleans and Y-tunnus validation
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_dim_vendors(
  CONSTRAINT valid_vendor_id EXPECT (vendor_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed vendors with parsed booleans and validated Y-tunnus'
AS
SELECT
  *,
  CASE LOWER(TRIM(rala_certified))
    WHEN 'true' THEN TRUE WHEN 'yes' THEN TRUE WHEN 'y' THEN TRUE WHEN '1' THEN TRUE
    WHEN 'false' THEN FALSE WHEN 'no' THEN FALSE WHEN 'n' THEN FALSE WHEN '0' THEN FALSE
    ELSE NULL
  END AS rala_certified_bool,
  CASE WHEN business_id RLIKE '^\\d{7}-\\d$' THEN TRUE ELSE FALSE END AS business_id_valid,
  LOWER(TRIM(vendor_type))      AS vendor_type_clean,
  LOWER(TRIM(trade_category))   AS trade_category_clean,
  LOWER(TRIM(approved_status))  AS approved_status_clean,
  current_timestamp()           AS ingestion_timestamp
FROM bronze_dim_vendors
QUALIFY ROW_NUMBER() OVER (PARTITION BY vendor_id ORDER BY vendor_id) = 1;
