/*
Query returns the previous day's total costs by service
*/

SELECT
  service.id AS service_id,
  service.description AS service_description,
  sum(cost) AS costs
FROM `bqutil.billing.billing_dashboard_export` 
WHERE DATE(export_time) = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY service_id, service_description
ORDER BY costs DESC
