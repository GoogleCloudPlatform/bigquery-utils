/*
Query returns the previous month's costs and credits by service
*/

SELECT
  invoice.month AS invoice_month,
  service.id AS service_id,
  service.description AS service_description,
  ROUND(SUM(cost), 2) AS costs,
  ROUND(SUM(credits.amount), 2) AS credits 
FROM `bqutil.billing.billing_dashboard_export`
LEFT JOIN UNNEST(credits) AS credits
WHERE invoice.month = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
GROUP BY invoice_month, service_id, service_description
ORDER BY costs DESC
