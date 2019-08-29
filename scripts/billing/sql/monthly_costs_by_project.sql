/*
Query returns monthly costs and credits by project
*/

SELECT
  project.name as project,
  EXTRACT(MONTH FROM usage_start_time) as month,
  ROUND(SUM(cost), 2) as costs,
  ROUND(SUM((SELECT SUM(amount) FROM UNNEST(credits))),2) as credits
FROM `data-analytics-pocs.public.billing_dashboard_export`
GROUP BY project, month
ORDER by project, month
