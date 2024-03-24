ALTER TABLE optimization_workshop.viewable_queries_grouped_by_hash
ADD COLUMN recommendation ARRAY<STRUCT<name STRING, description STRING>>;

UPDATE optimization_workshop.viewable_queries_grouped_by_hash t1
SET t1.recommendation = t2.recommendation
FROM optimization_workshop.antipattern_output_table t2
WHERE t1.query_hash = t2.job_id;
