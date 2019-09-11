/*
Query returns total costs by label values for a specific key

This example uses a label key of 'environment' as an example
*/

SELECT
  labels.value AS environment,
  SUM(cost) AS costs
FROM `bqutil.billing.billing_dashboard_export`
LEFT JOIN UNNEST(labels) AS labels
WHERE
  labels.key = 'environment'
  AND labels.value IS NOT NULL 
GROUP BY environment
