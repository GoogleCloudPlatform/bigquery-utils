/*
Query returns the previous day's total costs by service
*/

SELECT
  service.id,
  service.description,
  sum(cost) AS costs
FROM `data-analytics-pocs.public.billing_dashboard_export` 
WHERE DATE(export_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1, 2
ORDER BY costs DESC
