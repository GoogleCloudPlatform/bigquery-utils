#standardSQL

-- Query: Monthly CUD and SUD savings
-- Last Updated: 2019-07-23
--
-- This query calculates the monthly amount of savings from Committed Use Discounts (CUD)
-- and Sustained Use Discounts (SUD).

WITH 
  gce_data AS
  (
    SELECT *
    FROM
    -- *****************************************************************
    -- *** INSERT YOUR BILLING BQ EXPORT TABLE NAME ON THE NEXT LINE ***
    -- *****************************************************************
      `PROJECT.DATASET.TABLE`
    WHERE
      service.description = "Compute Engine"
  ),

  cost_data AS
  (
    SELECT
      invoice.month AS invoice_month,
      SUM(IF(LOWER(sku.description) NOT LIKE "%commitment%",
          cost,
          0)) AS usage_costs,
      SUM(IF(LOWER(sku.description) LIKE "%commitment%",
          cost,
          0)) AS commitment_costs
    FROM gce_data
    GROUP BY 1
  ),

  credit_data AS
  (
    SELECT
      invoice.month AS invoice_month,
      SUM(IF(LOWER(credits.name) LIKE "committed%",
          credits.amount,
          0)) AS CUD_credits,
      SUM(IF(LOWER(credits.name) LIKE "sustained%",
          credits.amount,
          0)) AS SUD_credits
    FROM gce_data
    LEFT JOIN UNNEST(credits) AS credits
    GROUP BY 1
  )

SELECT
  cost_data.invoice_month,
  ROUND(usage_costs,2) AS usage_costs,
  ROUND(commitment_costs,2) AS commitment_costs,
  ROUND(CUD_credits,2) AS CUD_credits,
  ROUND(-1*(commitment_costs + CUD_credits),2) AS net_CUD_savings,
  ROUND(SUD_credits,2) AS SUD_credits
FROM cost_data
INNER JOIN credit_data
  ON cost_data.invoice_month = credit_data.invoice_month
WHERE cost_data.invoice_month IS NOT NULL
ORDER BY 1 ASC
