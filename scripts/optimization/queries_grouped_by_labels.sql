DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE TEMP FUNCTION num_stages_with_perf_insights(query_info ANY TYPE) AS (
  COALESCE((
    SELECT SUM(IF(i.slot_contention, 1, 0) + IF(i.insufficient_shuffle_quota, 1, 0))
    FROM UNNEST(query_info.performance_insights.stage_performance_standalone_insights) i), 0)
  + COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_change_insights), 0)
);

CREATE OR REPLACE TABLE optimization_workshop.queries_grouped_by_labels AS
SELECT
  FORMAT("%p", JSON_ARRAY(labels))                                         AS labels,
  COUNT(DISTINCT DATE(start_time))                                         AS days_active,
  ARRAY_AGG(DISTINCT project_id IGNORE NULLS)                              AS project_ids,
  ARRAY_AGG(DISTINCT reservation_id IGNORE NULLS)                          AS reservation_ids,
  SUM(num_stages_with_perf_insights(query_info))                 AS num_stages_with_perf_insights,
  COUNT(DISTINCT (project_id || ':us.' || job_id))                         AS job_count,
  ARRAY_AGG(
    STRUCT(
      bqutil.fn.job_url(project_id || ':us.' || parent_job_id) AS parent_job_url,
      bqutil.fn.job_url(project_id || ':us.' || job_id) AS job_url
    )
    ORDER BY total_slot_ms
    DESC LIMIT 10)                                                         AS top_10_job_urls,
  ARRAY_AGG(DISTINCT user_email)                                           AS user_emails,
  SUM(total_bytes_processed) / POW(1024, 3)                                AS total_gigabytes_processed,
  AVG(total_bytes_processed) / POW(1024, 3)                                AS avg_gigabytes_processed,
  SUM(total_slot_ms) / (1000 * 60 * 60)                                    AS total_slot_hours,
  SUM(total_slot_ms) / (1000 * 60 * 60) / COUNT(DISTINCT DATE(start_time)) AS avg_total_slot_hours_per_active_day,
  AVG(TIMESTAMP_DIFF(end_time, start_time, SECOND) )                       AS avg_job_duration_seconds,
  SUM(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS total_slots,
  AVG(total_slot_ms / TIMESTAMP_DIFF(end_time, start_time, MILLISECOND))   AS avg_total_slots,
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  DATE(creation_time) >= CURRENT_DATE - num_days_to_scan
  AND state = 'DONE'
  AND error_result IS NULL
  AND job_type = 'QUERY'
  AND statement_type != 'SCRIPT'
  AND user_email LIKE '%gserviceaccount.com'
GROUP BY labels;