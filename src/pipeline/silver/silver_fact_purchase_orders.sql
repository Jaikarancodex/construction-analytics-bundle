-- ============================================================
-- Silver: fact_purchase_orders
-- Cleansed POs with over-invoicing flag
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_purchase_orders(
  CONSTRAINT valid_po_id       EXPECT (po_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_po_value EXPECT (ABS(po_value_eur) >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed POs with over-invoicing flag'
AS
SELECT
  *,
  ABS(po_value_eur)              AS po_value_eur_clean,
  ABS(invoiced_eur)              AS invoiced_eur_clean,
  CASE WHEN ABS(invoiced_eur) > ABS(po_value_eur) THEN TRUE ELSE FALSE END AS is_over_invoiced,
  LOWER(TRIM(po_category))       AS po_category_clean,
  LOWER(TRIM(po_status))         AS po_status_clean,
  COALESCE(try_to_date(po_date, 'yyyy-MM-dd'), try_to_date(po_date, 'dd/MM/yyyy'), try_to_date(po_date, 'yyyyMMdd')) AS po_dt,
  current_timestamp()            AS ingestion_timestamp
FROM bronze_fact_purchase_orders
QUALIFY ROW_NUMBER() OVER (PARTITION BY po_id ORDER BY po_id) = 1;
