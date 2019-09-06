/*
Query returns the previous month's cost and credits by specified label key

This query uses a label key of 'service' as an example
*/

SELECT
  invoice.month AS invoice_month,
  labels.value AS service,
  labels.key AS key,
  SUM(cost) AS costs,
  ROUND(SUM(credits.amount), 2) AS credits
FROM
`bqutil.billing.billing_dashboard_export`
LEFT JOIN UNNEST(credits) AS credits
LEFT JOIN UNNEST(labels) AS labels
WHERE invoice.month = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
AND labels.key = 'service'
-- for all invoice months, uncomment below (and comment above)
-- WHERE invoice.month IS NOT null
GROUP BY
  invoice_month,
  service,
  key
