#standardSQL

-- Query: CUD + SUD Coverage Query
-- Last Major Update: 2019-08-02
--
-- This query calculates the cost and usage amount (E.g. core*hrs) for Compute Engine usage.
-- This can be used to answer questions like: how much of my usage is covered by CUD and SUD?

WITH
  usage_data AS (
    SELECT
      CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) as usage_date,
      *
    -- *****************************************************************
    -- *** INSERT YOUR BILLING BQ EXPORT TABLE NAME ON THE NEXT LINE ***
    -- *****************************************************************
    FROM `PROJECT.DATASET.TABLE`
    WHERE service.description = "Compute Engine"
    -- *****************************************************************
    -- *** INPUT START DATE FOR YOUR DATA ON THE NEXT LINE           ***
    -- *****************************************************************
    AND CAST(DATETIME(usage_start_time, "America/Los_Angeles") AS DATE) >= "2019-01-01"
  ),

  -- create temporary table prices, in order to calculate unit price per (date, sku, region) tuple.
  -- Export table only includes the credit $ amount in the credit.amount field. We can get the credit
  -- usage amount (e.g. core hours) by dividing credit.amount ($) by unit price for that sku.
  -- This assumes that the unit price for the usage is equal to the unit price for the associated
  -- CUD credit. This should be correct, except in rare cases where unit price for that sku changed
  -- during the day (i.e. a price drop, change in spending-based discount %)
  -- It is necessary to do this in a separate table and join back into the main data set vs.
  -- separately on each usage line because some line items have CUD credit but no associated
  -- usage. We would not otherwise be able to calculate a unit price for these line items.
  prices AS (
    SELECT  
      usage_date,
      sku.id AS sku_id,
      location.region AS region,
      -- calculate unit price per sku for each day. Catch line items with 0 usage to avoid divide by zero.
      -- using 1 assumes that there are no relevant (CUD related) skus with cost but 0 usage, 
      -- which is correct for current billing data
      IF(SUM(usage.amount) = 0, 0, SUM(cost) / SUM(usage.amount)) AS unit_price
    FROM usage_data, UNNEST(credits) AS cred
    WHERE TRUE
      AND cred.name LIKE "Committed%"
      OR cred.name LIKE "Sustained%"
    GROUP BY 1,2,3
    ORDER BY 1,2,3
  ),

  -- Temporary table to pull out CUD + SUD credits cost and usage data.
  -- Talculate usage amount (e.g. core*hrs) data by dividing costs by unit price.
  credit_data AS (
    SELECT
      usage_date,
      region,
      sku_id,
      sku_description,
      machine_spec,
      project_id,
      SUM(cud_covered_usage) AS cud_covered_usage,
      SUM(cud_cost) AS cud_cost,
      SUM(sud_covered_usage) AS sud_covered_usage,
      SUM(sud_cost) AS sud_cost
    FROM
    (
      SELECT
        u.usage_date,
        u.location.region AS region,
        u.project.id AS project_id,
        u.sku.id AS sku_id,
        u.sku.description AS sku_description,
        IFNULL(system_labels.value, "UNKNOWN") AS machine_spec,
        usage.unit,
        prices.unit_price,
        cred.name,
        IF (
          prices.unit_price = 0 OR lower(cred.name) NOT LIKE "committed%",
          0, 
          CASE
            -- Divide credit $ amount by unit price to calculate amount of usage offset by credit
            WHEN LOWER(usage.unit) LIKE "seconds" THEN -1 * SUM(cred.amount) / prices.unit_price      
            WHEN LOWER(usage.unit) = "byte-seconds" THEN -1 * SUM(cred.amount) / prices.unit_price
            ELSE NULL
          END
        ) AS cud_covered_usage,
        IF (LOWER(cred.name) LIKE "committed%", SUM(cred.amount), 0) AS cud_cost,
        IF (
          prices.unit_price = 0 OR LOWER(cred.name) NOT LIKE "sustained%",
          0, 
          CASE
            -- Divide credit $ amount by unit price to calculate amount of usage offset by credit
            WHEN LOWER(usage.unit) LIKE "seconds" THEN -1 * SUM(cred.amount) / prices.unit_price
            WHEN LOWER(usage.unit) = "byte-seconds" THEN -1 * SUM(cred.amount) / prices.unit_price
            ELSE NULL
          END
        ) AS sud_covered_usage,
        IF (LOWER(cred.name) LIKE "sustained%", SUM(cred.amount), 0) AS sud_cost
      FROM usage_data AS u, UNNEST(credits) AS cred
      LEFT JOIN UNNEST(system_labels) AS system_labels
        ON system_labels.key = "compute.googleapis.com/machine_spec"
      INNER JOIN prices 
        ON u.sku.id = prices.sku_id
        AND u.location.region = prices.region
        AND u.usage_date = prices.usage_date
      GROUP BY 1,2,3,4,5,6,7,8,9
      ORDER BY 1,2,3,4,5,6,7,8,9 DESC
    )
    GROUP BY 1,2,3,4,5,6
  ),

  -- Temporary table containing usage amount and usage cost 
  -- before credit based discounts (e.g. CUD, SUD).
  cost_data AS
  (
    SELECT
      u.usage_date,
      u.location.region AS region,
      u.usage.unit AS unit,
      u.project.id AS project_id,
      u.project.name AS project_name,
      u.sku.id AS sku_id,
      u.sku.description AS sku_description,
      IFNULL(system_labels.value, "UNKNOWN") AS machine_spec,
      SUM(usage.amount) AS usage_amount,
      SUM(cost) AS cost
    FROM usage_data AS u
    LEFT JOIN UNNEST(system_labels) AS system_labels
      ON system_labels.key = "compute.googleapis.com/machine_spec"
    GROUP BY 1,2,3,4,5,6,7,8
    ORDER BY 1,2,3,4,5,6,7,8 ASC
  )

-- Final select statement to join everything together into one dataset
SELECT
  a.usage_date,
  a.region,
  a.unit,
  a.project_id,
  a.project_name,
  a.sku_id,
  a.sku_description,
  a.machine_spec,
  SUM(a.usage_amount) AS usage_amount,
  SUM(a.cost) AS cost,
  SUM(CUD_covered_usage) AS CUD_covered_usage,
  SUM(CUD_cost) AS CUD_cost,
  SUM(SUD_covered_usage) AS SUD_covered_usage,
  SUM(SUD_cost) AS SUD_cost
FROM cost_data a
FULL OUTER JOIN credit_data b 
  ON a.usage_date = b.usage_date
  AND a.region = b.region
  AND a.sku_id = b.sku_id
  AND a.sku_description = b.sku_description
  AND a.machine_spec = b.machine_spec
  AND a.project_id = b.project_id
GROUP BY 1,2,3,4,5,6,7,8
ORDER BY 1,2,3,4,5,6,7,8 ASC
