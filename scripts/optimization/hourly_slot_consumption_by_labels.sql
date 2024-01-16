DECLARE num_days_to_scan INT64 DEFAULT 30;
DECLARE my_reservation_id STRING DEFAULT "your_reservation_id";


CREATE OR REPLACE TABLE optimization_workshop.hourly_slot_consumption_by_labels AS
SELECT * EXCEPT(jobs_per_hour),
-- Get the top 10 query patterns by slot hours
ARRAY(
  SELECT AS STRUCT
    -- since period is hourly, period_slot_hours/(1hr duration) = num_period_slots
    ROUND(period_slot_hours, 2) AS num_period_slots,
    ROUND(period_slot_hours/period_total_slot_hours * 100, 2) || "%" AS pct_of_total_period,
    COALESCE(slot_hunger,0) AS slot_hunger,
    ROUND(avg_period_estimated_runnable_units,2) AS avg_period_estimated_runnable_units,
    ROUND(max_duration_minutes, 2)     AS max_duration_minutes,
    ROUND(p90_duration_minutes, 2)     AS p90_duration_minutes,
    ROUND(median_duration_minutes, 2)  AS median_duration_minutes,
    ROUND(max_p90_period_slots, 2)     AS max_p90_period_slots,
    ROUND(max_period_slots, 2)         AS max_period_slots,
    ROUND(p90_avg_job_period_slots, 2) AS p90_avg_job_period_slots,
    labels,
    num_jobs_per_period,
    job_url,
    parent_job_url,
  FROM(
    SELECT
      FORMAT("%p", JSON_ARRAY(labels))                 AS labels,
      MAX(duration_ms)/1000/60                         AS max_duration_minutes,
      SUM(slot_hunger)                                 AS slot_hunger,
      AVG(avg_period_estimated_runnable_units)         AS avg_period_estimated_runnable_units,
      MAX(p90_job_period_slots)                        AS max_p90_period_slots,
      MAX(max_job_period_slots)                        AS max_period_slots,
      ANY_VALUE(p90_avg_job_period_slots)              AS p90_avg_job_period_slots,
      ANY_VALUE(median_duration_minutes)               AS median_duration_minutes,
      ANY_VALUE(p90_duration_minutes)                  AS p90_duration_minutes,
      SUM(period_slot_hours)                           AS period_slot_hours,
      COUNT(1)                                         AS num_jobs_per_period,
      ANY_VALUE(job_url        HAVING MAX duration_ms) AS job_url,
      ANY_VALUE(parent_job_url HAVING MAX duration_ms) AS parent_job_url,
      ANY_VALUE(user_email     HAVING MAX duration_ms) AS user_email,
    FROM (
      SELECT *,
        PERCENTILE_CONT(duration_ms, 0.5) OVER(
          PARTITION BY FORMAT("%p", JSON_ARRAY(labels))
        )/1000/60 AS median_duration_minutes,
        PERCENTILE_CONT(duration_ms, 0.9) OVER(
          PARTITION BY FORMAT("%p", JSON_ARRAY(labels))
        )/1000/60 AS p90_duration_minutes,
        PERCENTILE_CONT(avg_job_period_slots, 0.9) OVER(
          PARTITION BY FORMAT("%p", JSON_ARRAY(labels))
        ) AS p90_avg_job_period_slots,
      FROM UNNEST(jobs_per_hour)
    )
    GROUP BY 1
    -- HAVING user_email LIKE '%g serviceaccount.com'
    ORDER BY period_slot_hours DESC
    LIMIT 10
  )
) AS top_job_patterns_per_hour
FROM(
  SELECT
    FORMAT_TIMESTAMP("%F %H", period_start_hour, "America/New_York") AS period_start_hour,
    COUNT(DISTINCT job_id) AS num_jobs_in_period,
    SUM(period_slot_ms)/1000/60/60 AS period_total_slot_hours,
    MAX(p90_job_period_slots) AS max_p90_job_period_slots,
    ARRAY_AGG(STRUCT(
      period_slot_ms/1000/60/60 AS period_slot_hours,
      p90_job_period_slots,
      avg_job_period_slots,
      max_job_period_slots,
      duration_ms,
      avg_period_estimated_runnable_units,
      slot_hunger,
      labels,
      bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url,
      COALESCE(bqutil.fn.job_url(project_id || ':us.' || parent_job_id), '') AS parent_job_url,
      user_email
    )) AS jobs_per_hour,
  FROM(
    SELECT
      TIMESTAMP_TRUNC(period_start, HOUR, "America/New_York") AS period_start_hour,
      project_id,
      parent_job_id,
      job_id,
      user_email,
      AVG(COALESCE(period_estimated_runnable_units,0)) AS avg_period_estimated_runnable_units,
      SUM(IF(COALESCE(period_estimated_runnable_units,0) >= 1000,1,0)) AS slot_hunger,
      SUM(period_slot_ms) AS period_slot_ms,
      ANY_VALUE(p90_job_period_slots) AS p90_job_period_slots,
      MAX(period_slot_ms/1000) AS max_job_period_slots,
      ANY_VALUE(avg_job_period_slots) AS avg_job_period_slots,
    FROM(
      SELECT *,
        PERCENTILE_CONT(period_slot_ms/1000, 0.9) OVER(
            PARTITION BY TIMESTAMP_TRUNC(period_start, HOUR, "America/New_York"), job_id
          ) AS p90_job_period_slots,
        AVG(period_slot_ms/1000) OVER(
          PARTITION BY TIMESTAMP_TRUNC(period_start, HOUR, "America/New_York"), job_id
        ) AS avg_job_period_slots
      FROM `region-us`.INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
      WHERE
        DATE(job_creation_time) >= CURRENT_DATE - num_days_to_scan
        AND reservation_id = my_reservation_id
        AND state IN ('RUNNING','DONE')
        AND statement_type != 'SCRIPT'
        AND period_slot_ms > 0
        -- Uncomment below to ignore slots consumed by failed queries
        -- AND error_result IS NULL
    )
    GROUP BY 1,2,3,4,5
  )
  JOIN (
    SELECT
      job_id,
      labels,
      TIMESTAMP_DIFF(end_time, start_time, MILLISECOND) AS duration_ms,
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE
      DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
      AND reservation_id = my_reservation_id
      AND statement_type != 'SCRIPT'
      AND total_slot_ms > 0
      -- Uncomment below to ignore slots consumed by failed queries
      -- AND error_result IS NULL
  ) USING(job_id)
  GROUP BY period_start_hour
);