/*
Query returns the previous month's cost and credits by specified label key

This query uses a label key of "service" as an example
*/

SELECT
  invoice.month as invoice_month,
  labels.value as service,
  SUM(cost) as costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))),2) as credits 
FROM `data-analytics-pocs.public.billing_dashboard_export`
LEFT JOIN UNNEST(labels) as labels
  ON labels.key = "service"
WHERE invoice.month = FORMAT_DATE("%Y%m", DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
# for all invoice months, uncomment below (and comment above)
# WHERE invoice.month IS NOT null
GROUP BY invoice_month, service
ORDER BY invoice_month, costs DESC
