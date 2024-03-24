CREATE OR REPLACE TABLE optimization_workshop.antipattern_output_table (
  job_id STRING,
  user_email STRING,
  query STRING,
  recommendation ARRAY<STRUCT<name STRING, description STRING>>,
  slot_hours FLOAT64,
  optimized_sql STRING,
  process_timestamp TIMESTAMP
);

CREATE OR REPLACE VIEW optimization_workshop.antipattern_tool_input_view AS
SELECT 
  query_hash id, 
  top_10_jobs[SAFE_OFFSET(0)].query_text query 
FROM 
  optimization_workshop.viewable_queries_grouped_by_hash 
ORDER BY 
  avg_total_slots desc
LIMIT 
  1000
;
