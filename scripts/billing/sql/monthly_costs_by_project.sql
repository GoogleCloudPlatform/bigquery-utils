/*
Query returns monthly costs and credits by project
*/

SELECT
  project.name AS project,
  EXTRACT(MONTH FROM usage_start_time) AS month,
  ROUND(SUM(cost), 2) AS costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))), 2) AS credits
FROM `bqutil.billing.billing_dashboard_export`
GROUP BY project, month
ORDER by project, month
