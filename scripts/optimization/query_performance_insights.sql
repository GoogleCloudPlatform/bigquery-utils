SELECT
  `bigquery-public-data`.persistent_udfs.job_url(project_id || ':us.' || job_id) AS job_url,
  query_info.performance_insights.stage_performance_standalone_insights,
  query_info.performance_insights.stage_performance_change_insights
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION jbo
WHERE
  DATE(jbo.creation_time,"US/Central") >= "2023-01-01"
  AND jbo.job_type = 'QUERY'
  AND jbo.end_time > jbo.start_time
  AND jbo.error_result IS NULL
  AND jbo.statement_type != 'SCRIPT'
  AND (
    COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_standalone_insights),0)
    + COALESCE(ARRAY_LENGTH(query_info.performance_insights.stage_performance_change_insights),0) > 0
  )
  AND EXISTS ( -- Only include queries which had performance insights
    SELECT
      1
    FROM
      UNNEST(query_info.performance_insights.stage_performance_standalone_insights) AS perf_insight
    WHERE
      perf_insight.slot_contention OR perf_insight.insufficient_shuffle_quota
  )