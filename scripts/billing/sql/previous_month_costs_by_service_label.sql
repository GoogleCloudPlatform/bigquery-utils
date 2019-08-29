/*
Query returns the previous month's costs and credits by invoice month,
label value, and service

This example uses a label key of "cost_center" and label value of "sales"
*/

SELECT
  invoice.month AS invoice_month,
  labels.value AS label_value,
  service.description AS description,
  SUM(cost) AS costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))),2) AS credits 
FROM `data-analytics-pocs.public.billing_dashboard_export`
LEFT JOIN UNNEST(labels) AS labels
  ON labels.key = "cost_center"
WHERE
  invoice.month = FORMAT_DATE("%Y%m", DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH))
  AND labels.value = "sales"
GROUP BY invoice_month, description, label_value
ORDER BY invoice_month, costs DESC
