/*
Query returns monthly total costs and credits
*/

SELECT
  EXTRACT(MONTH FROM export_time) AS month,
  EXTRACT(YEAR FROM export_time) AS year,
  ROUND(SUM(cost), 2) AS costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))), 2) AS credits
FROM `bqutil.billing.billing_dashboard_export`
GROUP BY year, month
ORDER by year, month
