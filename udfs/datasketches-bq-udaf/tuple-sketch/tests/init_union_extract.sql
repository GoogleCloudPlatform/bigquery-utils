create table tuple_sketch_us.sample_data_100M AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as user_id,
    X2 as clicks
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,99)) as x2;

CREATE OR REPLACE TABLE tuple_sketch_us.agg_sample_data_100M AS
select
    group_key,
    count(distinct user_id) as exact_uniq_users_ct,
    sum(clicks) as exact_clicks_ct,
    tuple_sketch_us.tuple_sketch_int64(user_id,clicks,18) as tuple_sketch
from tuple_sketch_us.sample_data_100M
group by group_key;

select
    group_key,
    exact_uniq_users_ct,
    exact_clicks_ct,
    FLOOR(exact_clicks_ct/exact_uniq_users_ct) as clicks_avg,
    tuple_sketch_us.tuple_sketch_extract_summary(tuple_sketch) as summary
from tuple_sketch_us.agg_sample_data_100M;

WITH
    agg_data AS (
        SELECT
            tuple_sketch_us.tuple_sketch_union(tuple_sketch,14) as merged_tuple_sketch,
            SUM(exact_uniq_users_ct) as total_uniq_users_ct,
        from tuple_sketch_us.agg_sample_data_100M
    )
Select
    total_uniq_users_ct,
    tuple_sketch_us.tuple_sketch_extract_summary(merged_tuple_sketch) as summary,
    tuple_sketch_us.tuple_sketch_extract_count(merged_tuple_sketch) as approx_dist_user_ct,
    tuple_sketch_us.tuple_sketch_extract_sum(merged_tuple_sketch) as approx_clicks_ct,
    tuple_sketch_us.tuple_sketch_extract_avg(merged_tuple_sketch) as approx_clicks_avg,
    merged_tuple_sketch
FROM agg_data;