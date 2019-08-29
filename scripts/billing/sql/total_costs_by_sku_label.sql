/*
Query returns total costs by sku and all labels
*/

SELECT
  sku.description,
  TO_JSON_STRING(labels) AS labels,
  sum(cost) AS cost
FROM `data-analytics-pocs.public.billing_dashboard_export`
GROUP BY 1,2
