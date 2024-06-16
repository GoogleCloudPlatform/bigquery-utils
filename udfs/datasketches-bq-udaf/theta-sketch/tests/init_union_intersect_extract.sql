CREATE OR REPLACE TABLE udaf_us_east7.set_a AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,1)) as x2;


CREATE TABLE IF NOT EXISTS udaf_us_east7.set_b AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    x
from UNNEST(GENERATE_ARRAY(1,500000)) as x;


CREATE TABLE IF NOT EXISTS udaf_us_east7.agg_data_set_a_by_key_theta AS
select
    group_key,
    count(*) as total_count,
    udaf_us_east7.theta_sketch_int64(x,14) as theta_sketch
from udaf_us_east7.set_a
group by group_key;


CREATE TABLE IF NOT EXISTS udaf_us_east7.agg_data_set_b_by_key_theta AS
select
    group_key,
    count(*) as total_count,
    udaf_us_east7.theta_sketch_int64(x,14) as theta_sketch
from udaf_us_east7.set_b
group by group_key;


WITH
    union_data_set_a AS (
        SELECT
            udaf_us_east7.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from udaf_us_east7.agg_data_set_a_by_key_theta
    ),
    union_data_set_b AS (
        SELECT
            udaf_us_east7.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from udaf_us_east7.agg_data_set_b_by_key_theta
    ),
    intersect_data_set_a_b AS (
        SELECT
            udaf_us_east7.theta_sketch_intersection(merged_theta_sketch) as intersected_theta_sketch,
        from (
                 SELECT
                     merged_theta_sketch,
                     total_count
                 FROM union_data_set_b
                 UNION ALL
                 SELECT
                     merged_theta_sketch,
                     total_count
                 FROM union_data_set_a
             )
    )
select
    udaf_us_east7.theta_sketch_extract(intersected_theta_sketch) as intersect_uniq_count,
    intersected_theta_sketch
FROM intersect_data_set_a_b;