-- ============================================================
-- Gold: invoice_aging
-- Invoice aging buckets and overdue analysis
-- ============================================================
CREATE OR REFRESH MATERIALIZED VIEW gold_invoice_aging
COMMENT 'Invoice aging buckets and overdue analysis'
AS
SELECT
  i.vendor_id,
  v.vendor_name,
  i.payment_status_clean AS payment_status,
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 0  THEN 'current'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 30 THEN '1-30 days'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 60 THEN '31-60 days'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 90 THEN '61-90 days'
    ELSE '90+ days'
  END AS aging_bucket,
  COUNT(*)                  AS invoice_count,
  SUM(i.amount_eur_clean)   AS total_amount_eur
FROM silver_fact_invoices i
LEFT JOIN silver_dim_vendors v ON i.vendor_id = v.vendor_id
GROUP BY i.vendor_id, v.vendor_name, i.payment_status_clean,
  CASE
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 0  THEN 'current'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 30 THEN '1-30 days'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 60 THEN '31-60 days'
    WHEN DATEDIFF(CURRENT_DATE(), i.due_dt) <= 90 THEN '61-90 days'
    ELSE '90+ days'
  END;
