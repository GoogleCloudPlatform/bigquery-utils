/*
Query returns the previous month's costs and credits by invoice month,
label value, and service

This example uses a label key of 'cost_center' and label value of 'sales'
*/

SELECT
  invoice.month AS invoice_month,
  labels.value AS label_value,
  service.description AS description,
  SUM(cost) AS costs,
  ROUND(SUM(credits.amount), 2) AS credits 
FROM `bqutil.billing.billing_dashboard_export`
LEFT JOIN UNNEST(labels) AS labels
LEFT JOIN UNNEST(credits) AS credits
WHERE
  invoice.month = FORMAT_DATE('%Y%m', DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  AND labels.value = 'sales'
  AND labels.key = 'cost_center'
GROUP BY
  invoice_month,
  description,
  label_value
ORDER BY
  invoice_month,
  costs DESC
