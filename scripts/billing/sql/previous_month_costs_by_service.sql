/*
Query returns the previous month's costs and credits by service
*/

SELECT
  invoice.month AS invoice_month,
  service.id AS service_id,
  service.description AS service_description,
  ROUND(SUM(cost), 2) AS costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))),2) AS credits 
FROM `data-analytics-pocs.public.billing_dashboard_export`
WHERE invoice.month = FORMAT_DATE("%Y%m", DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
GROUP BY invoice.month, service_id, service_description
ORDER BY costs DESC
