/*
Query returns total costs by label values for a specific key

This example uses a label key of "environment" as an example
*/

SELECT
  labels.value AS environment,
  SUM(cost) AS costs
FROM `data-analytics-pocs.public.billing_dashboard_export`
LEFT JOIN UNNEST(labels) AS labels
  ON labels.key = "environment"
GROUP BY environment
