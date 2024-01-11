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
  ),

  cost_data AS
  (
    SELECT
      invoice.month AS invoice_month,
      SUM(IF(LOWER(sku.description) NOT LIKE "%commitment%",
          cost,
          0)) AS usage_costs,
      SUM(IF(NOT REGEXP_CONTAINS(LOWER(sku.description),r"commitment (- dollar based)") AND LOWER(sku.description) LIKE "%commitment%",
          cost,
          0)) AS commitment_resource_costs,
      SUM(IF(REGEXP_CONTAINS(LOWER(sku.description),r"commitment (- dollar based)"),
          cost,
          0)) AS commitment_spend_costs
    FROM gce_data
    GROUP BY 1, 2
  ),

  credit_data AS
  (
    SELECT
      invoice.month AS invoice_month,
      SUM(IF(credits.type LIKE "COMMITTED_USAGE_DISCOUNT",
          credits.amount,
          0)) AS CUD_resource_credits,
      SUM(IF(credits.type LIKE "COMMITTED_USAGE_DISCOUNT_DOLLAR_BASE",
          credits.amount,
          0)) AS CUD_spend_credits,
      SUM(IF(credits.type LIKE "SUSTAINED_USAGE_DISCOUNT",
          credits.amount,
          0)) AS SUD_credits
    FROM gce_data
    LEFT JOIN UNNEST(credits) AS credits
    GROUP BY 1
  )

SELECT
  cost_data.invoice_month,
  ROUND(usage_costs,2) AS usage_costs,
  ROUND(commitment_resource_costs,2) AS commitment_resource_costs,
  ROUND(commitment_spend_costs,2) AS commitment_spend_costs,
  ROUND(CUD_resource_credits,2) AS CUD_resource_credits,
  ROUND(CUD_spend_credits,2) AS CUD_spend_credits,
  ROUND(-1*(commitment_resource_costs + CUD_resource_credits),2) AS net_CUD_resource_savings,
  ROUND(-1*(commitment_spend_costs + CUD_spend_credits),2) AS net_CUD_spend_savings,
  ROUND(SUD_credits,2) AS SUD_credits
FROM cost_data
LEFT JOIN credit_data
  ON cost_data.invoice_month = credit_data.invoice_month
WHERE cost_data.invoice_month IS NOT NULL
ORDER BY 1 ASC