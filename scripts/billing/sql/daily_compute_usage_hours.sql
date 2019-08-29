/*
Query returns compute usage hours by sku and component on a daily basis
*/

SELECT
  FORMAT_DATETIME("%Y%m%d", DATETIME(usage_end_time)) AS date_ymd, 
  sku.id AS sku_id,
  sku.description as component,
  SUM(usage.amount) as component_usage,
  usage.unit as usage_unit,
  ROUND(SUM(cost - credits.amount) * 100) / 100 AS actual_cost
FROM
  `data-analytics-pocs.public.billing_dashboard_export`,
  UNNEST(credits) AS credits
WHERE service.id = "6F81-5844-456A"
GROUP BY
  date_ymd,
  sku.id,
  sku.description,
  usage.unit
ORDER BY
  date_ymd ASC,
  sku.description ASC
