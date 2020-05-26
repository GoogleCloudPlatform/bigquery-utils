/*
Query returns compute cost including discounts on a daily basis
*/

SELECT
  FORMAT_DATETIME('%Y%m%d', DATETIME(usage_end_time)) AS date_ymd, 
  ROUND(SUM(cost - credits.amount) * 100, 2) / 100 AS actual_cost
FROM
  `bqutil.billing.billing_dashboard_export`,
  UNNEST(credits) AS credits
WHERE service.id = '6F81-5844-456A'
GROUP BY
  date_ymd
ORDER BY
  date_ymd ASC
