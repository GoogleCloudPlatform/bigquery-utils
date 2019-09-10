/*
Query returns the invoice total for each month as a sum of regular costs,
taxes, adjustments, and rounding errors.
*/

SELECT
  invoice.month AS invoice_month,
  SUM(cost) + SUM(IFNULL(credits.amount, 0)) AS total,
  (SUM(CAST(cost * 1000000 AS INT64)) +
    SUM(IFNULL(CAST(credits.amount * 1000000 AS INT64), 0))) / 1000000
    AS total_exact
FROM `bqutil.billing.billing_dashboard_export`
LEFT JOIN UNNEST(credits) AS credits
GROUP BY invoice_month
ORDER BY invoice_month ASC
