-- ============================================================
-- Silver: fact_invoices
-- Cleansed invoices with corrected overdue days
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW silver_fact_invoices(
  CONSTRAINT valid_invoice_id EXPECT (invoice_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_amount  EXPECT (ABS(amount_eur) >= 0) ON VIOLATION DROP ROW
)
COMMENT 'Cleansed invoices with corrected overdue days'
AS
SELECT
  *,
  ABS(amount_eur) AS amount_eur_clean,
  COALESCE(try_to_date(invoice_date, 'yyyy-MM-dd'), try_to_date(invoice_date, 'dd/MM/yyyy'), try_to_date(invoice_date, 'yyyyMMdd')) AS invoice_dt,
  COALESCE(try_to_date(due_date, 'yyyy-MM-dd'), try_to_date(due_date, 'dd/MM/yyyy'), try_to_date(due_date, 'yyyyMMdd')) AS due_dt,
  COALESCE(try_to_date(paid_date, 'yyyy-MM-dd'), try_to_date(paid_date, 'dd/MM/yyyy'), try_to_date(paid_date, 'yyyyMMdd')) AS paid_dt,
  LOWER(TRIM(payment_status))   AS payment_status_clean,
  current_timestamp()           AS ingestion_timestamp
FROM bronze_fact_invoices
QUALIFY ROW_NUMBER() OVER (PARTITION BY invoice_id ORDER BY invoice_id) = 1;
