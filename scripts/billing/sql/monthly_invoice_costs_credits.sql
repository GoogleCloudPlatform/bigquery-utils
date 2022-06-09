/*
Query returns the invoice total for each month as a sum of regular costs,
taxes, adjustments, and rounding errors.
*/

SELECT
  invoice.month AS invoice_month,
  SUM(cost) + SUM((SELECT SUM(amount) FROM UNNEST(credits))) AS total,
  (SUM(CAST(cost * 1000000 AS INT64)) +
    SUM((SELECT SUM(CAST(amount * 1000000 AS INT64)) FROM UNNEST(credits)))) / 1000000
    AS total_exact
FROM `bqutil.billing.billing_dashboard_export`
GROUP BY invoice_month
ORDER BY invoice_month ASC
