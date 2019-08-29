/*
Query returns the previous day's total costs by project name
*/

SELECT
  project.name,
  sum(cost) AS costs 
FROM `data-analytics-pocs.public.billing_dashboard_export` 
WHERE DATE(export_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY 1
ORDER BY costs DESC
