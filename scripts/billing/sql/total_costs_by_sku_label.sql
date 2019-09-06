/*
Query returns total costs by sku and all labels
*/

SELECT
  sku.description AS sku_description,
  TO_JSON_STRING(labels) AS labels,
  sum(cost) AS cost
FROM `bqutil.billing.billing_dashboard_export`
GROUP BY sku_description, labels
