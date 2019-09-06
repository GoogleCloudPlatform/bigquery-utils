/*
Query returns number of cores executed on average on a daily basis
*/

SELECT
  FORMAT_DATETIME('%Y%m%d', DATETIME(usage_end_time)) AS date_ymdh, 
  AVG(CAST(system_labels.value AS INT64)) AS avg_core_usage
FROM
  `bqutil.billing.billing_dashboard_export`,
  UNNEST(system_labels) AS system_labels
WHERE 
  service.id = '6F81-5844-456A'
  AND system_labels.key = 'compute.googleapis.com/cores'
GROUP BY
  date_ymdh
ORDER BY
  date_ymdh ASC
