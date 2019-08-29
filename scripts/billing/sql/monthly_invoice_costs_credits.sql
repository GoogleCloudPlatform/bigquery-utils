/*
Query returns the invoice total for each month, as a sum of regular costs,
taxes, adjustments, and rounding errors.
*/

SELECT
  invoice.month,
  SUM(cost) + SUM(IFNULL((SELECT SUM(c.amount) FROM UNNEST(credits) c), 0)) AS total,
  (SUM(CAST(cost * 1000000 AS int64))
    + SUM(IFNULL((SELECT SUM(CAST(c.amount * 1000000 as int64))
                  FROM UNNEST(credits) c), 0))) / 1000000
    AS total_exact
FROM `data-analytics-pocs.public.billing_dashboard_export`
GROUP BY 1
ORDER BY 1 ASC
