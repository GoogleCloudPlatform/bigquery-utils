-- WARNING #########################################################################################################################################
-- WARNING If you change the storage billing model of a dataset then you have to wait 14 days before changing it again.
-- WARNING See https://cloud.google.com/bigquery/docs/datasets-intro#dataset_limitations
-- WARNING #########################################################################################################################################


-- Instructions
--  Search for marker 'REMEMBER' to tune the queries at your will
--  Run the query to obtain the results including ALTER SCHEMA DDL


-- Permissions needed
-- * To run this query
--  * Permissions to run queries in the current project
--  * Permissions to create a table and write on it in the configured work dataset
--  * https://cloud.google.com/bigquery/docs/information-schema-table-storage-by-organization#required_permissions
--  * https://cloud.google.com/bigquery/docs/information-schema-datasets-schemata-options#before_you_begin
-- * To run the ALTER SCHEMA DDL
--  * https://cloud.google.com/bigquery/docs/updating-datasets#required_permissions


-- REMEMBER: Put here the prices of the region of interest, current values are for the US
-- See https://cloud.google.com/bigquery/pricing#storage
DECLARE active_logical_gib_price FLOAT64 DEFAULT 0.02;
DECLARE long_term_logical_gib_price FLOAT64 DEFAULT 0.01;
DECLARE active_physical_gib_price FLOAT64 DEFAULT 0.04;
DECLARE long_term_physical_gib_price FLOAT64 DEFAULT 0.02;


-- REMEMBER: (optional) Change this if you plan to change the time travel window (e.g. if you currently use the default 7 days and want to reduce it to 2 days put 2.0/7.0)
-- See https://cloud.google.com/bigquery/docs/time-travel#configuring_the_time_travel_window
DECLARE time_travel_rescale FLOAT64 DEFAULT 1.0;
-- REMEMBER: (optional) Change this if you want that the generated DDL statements include time travel window settings (e.g. 2.0*24.0)
DECLARE time_travel_hours FLOAT64 DEFAULT NULL;
-- REMEMBER: (optional) Change this to filter based on savings absolute value
DECLARE min_monthly_saving FLOAT64 DEFAULT 0.0;
-- REMEMBER: (optional) Change this to filter based on savings % value (e.g. 1% is 0.01)
DECLARE min_monthly_saving_pct FLOAT64 DEFAULT 0.00;

DECLARE query string;
CREATE OR REPLACE TEMPORARY TABLE get_phy_datasets_at_org_level_results (
  project_name STRING,
  dataset_name STRING,
);
FOR record IN (
  SELECT DISTINCT project_id
  FROM `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
) DO
  SET query = '''
    INSERT INTO get_phy_datasets_at_org_level_results 
    SELECT
      catalog_name AS project_name,
      schema_name AS dataset_name,
    FROM `''' || record.project_id || '''.region-us.INFORMATION_SCHEMA.SCHEMATA_OPTIONS` -- REMEMBER: Change here to the region of interest
    WHERE 
      option_name = "storage_billing_model" 
      AND option_value = 'PHYSICAL'
  ''';
  BEGIN
    EXECUTE IMMEDIATE query;
    EXCEPTION WHEN ERROR THEN
  END;
END FOR;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.storage_billing_model_savings_ddl AS
WITH
storage_sizes AS (
 SELECT
   project_id AS project_name,
   table_schema AS dataset_name,
   -- Logical
   SUM(active_logical_bytes) / power(1024, 3) AS active_logical_gib,
   SUM(long_term_logical_bytes) / power(1024, 3) AS long_term_logical_gib,
   -- Physical
   SUM(active_physical_bytes - time_travel_physical_bytes) / power(1024, 3) AS active_no_tt_no_fs_physical_gib,
   SUM(time_travel_physical_bytes) / power(1024, 3) * time_travel_rescale AS time_travel_physical_gib,
   SUM(fail_safe_physical_bytes) / power(1024, 3) AS fail_safe_physical_gib,
   SUM(long_term_physical_bytes) / power(1024, 3) AS long_term_physical_gib,
 FROM
    -- REMEMBER: Change here to the region of interest
    `region-us`.INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION
   -- See https://cloud.google.com/bigquery/docs/information-schema-table-storage-by-organization#schema
 WHERE TRUE
   AND total_physical_bytes > 0
   -- REMEMBER: (optional) You may want to enable this filter to exclude ML models, materialized views, etc...
   -- AND table_type  = 'BASE TABLE'
   GROUP BY 1,2
),
storage_prices AS (
SELECT
  project_name,
  dataset_name,
  -- Logical
  active_logical_gib AS active_logical_gib,
  long_term_logical_gib AS long_term_logical_gib,
  -- Physical
  active_no_tt_no_fs_physical_gib AS active_no_tt_no_fs_physical_gib,
  time_travel_physical_gib AS time_travel_physical_gib,
  fail_safe_physical_gib AS fail_safe_physical_gib,
  long_term_physical_gib AS long_term_physical_gib,
  -- Compression ratio
  SAFE_DIVIDE(active_logical_gib, active_no_tt_no_fs_physical_gib) AS active_compression_ratio,
  SAFE_DIVIDE(long_term_logical_gib, long_term_physical_gib) AS long_term_compression_ratio,
  -- Forecast costs logical
  active_logical_gib * active_logical_gib_price AS forecast_active_logical_cost,
  long_term_logical_gib * long_term_logical_gib_price AS forecast_long_term_logical_cost,
  -- Forecast costs physical
  active_no_tt_no_fs_physical_gib * active_physical_gib_price AS forecast_active_no_tt_no_fs_physical_cost,
  time_travel_physical_gib * active_physical_gib_price AS forecast_travel_physical_cost,
  fail_safe_physical_gib * active_physical_gib_price AS forecast_failsafe_physical_cost,
  long_term_physical_gib * long_term_physical_gib_price AS forecast_long_term_physical_cost,
FROM
  storage_sizes
),
storage_prices_total AS (
SELECT
 project_name,
 dataset_name,
 (forecast_active_logical_cost+forecast_long_term_logical_cost) AS forecast_logical,
 (forecast_active_no_tt_no_fs_physical_cost+forecast_travel_physical_cost+forecast_failsafe_physical_cost+forecast_long_term_physical_cost) AS forecast_physical,
FROM storage_prices
),
storage_prices_compare AS (
SELECT
 SPT.project_name,
 SPT.dataset_name,
 SPT.forecast_logical,
 SPT.forecast_physical,
 (SPT.forecast_logical - SPT.forecast_physical) AS forecast_compare,
 IF(SPT.forecast_logical > SPT.forecast_physical,"physical","logical") AS better_on,
 IF(PD.dataset_name IS NULL, "logical", "physical") AS currently_on
FROM
 storage_prices_total AS SPT
LEFT JOIN get_phy_datasets_at_org_level_results AS PD
USING(project_name, dataset_name)
),
storage_prices_compare2 AS (
SELECT
 *,
 IF(currently_on = "logical", forecast_logical, forecast_physical) AS monthly_spending,
 ABS(forecast_compare) AS monthly_savings,
 ABS(forecast_compare)/IF(currently_on = "logical", forecast_logical, forecast_physical) AS monthly_savings_pct,
FROM storage_prices_compare
),
storage_ddl AS (
SELECT
 *,
 IF(time_travel_hours IS NULL,
  CONCAT("ALTER SCHEMA `", project_name, ".", dataset_name, "` SET OPTIONS(storage_billing_model='", better_on, "' );" ),
  CONCAT("ALTER SCHEMA `", project_name, ".", dataset_name, "` SET OPTIONS(storage_billing_model='", better_on, "', max_time_travel_hours=", time_travel_hours, ");" )
 ) AS ddl,
FROM storage_prices_compare2
WHERE better_on != currently_on
AND monthly_savings > min_monthly_saving
AND monthly_savings_pct > min_monthly_saving_pct
ORDER BY monthly_savings DESC
)
-- REMEMBER: (optional) Change here the name of the pseudo-table (CTE) you want to check
SELECT * FROM storage_ddl;