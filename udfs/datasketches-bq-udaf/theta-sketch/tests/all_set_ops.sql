-- Generate ints from [1, 5M]
CREATE OR REPLACE TABLE udaf_us_east7.set_a AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,4)) as x2;

-- Generate ints from [4M, 7M]
CREATE OR REPLACE TABLE  udaf_us_east7.set_b AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(4,6)) as x2;


CREATE OR REPLACE TABLE udaf_us_east7.theta_agg_by_key_set_a AS
select
    group_key,
    count(*) as total_count,
    udaf_us_east7.theta_sketch_int64(x,14) as theta_sketch
from udaf_us_east7.set_a
group by group_key;


CREATE OR REPLACE TABLE udaf_us_east7.theta_agg_by_key_set_b AS
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
        from udaf_us_east7.theta_agg_by_key_set_a
    ),
    union_data_set_b AS (
        SELECT
            udaf_us_east7.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from udaf_us_east7.theta_agg_by_key_set_b
    ),
    agg_data_set_a_b AS (
        SELECT
            udaf_us_east7.theta_sketch_intersection(merged_theta_sketch) as intersected_theta_sketch,
            sum(total_count) as total_count,
            udaf_us_east7.theta_sketch_union(merged_theta_sketch, 14) as union_theta_sketch
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
    ),
    set_difference AS (
        SELECT
            udaf_us_east7.theta_sketch_a_not_b(a.merged_theta_sketch, b.merged_theta_sketch) as a_not_b_theta_sketch,
            udaf_us_east7.theta_sketch_a_not_b(b.merged_theta_sketch, a.merged_theta_sketch) as b_not_a_theta_sketch,
            a.total_count as set_a_count,
            b.total_count as set_b_count
        from  union_data_set_a a, union_data_set_b b
    )
select
    "SetA = [1, 5M] \nSetB = (4M, 7M]" as set_info,
    udaf_us_east7.theta_sketch_extract(intersected_theta_sketch) as intersect_uniq_count,
    udaf_us_east7.theta_sketch_extract(union_theta_sketch) as union_uniq_count,
    udaf_us_east7.theta_sketch_extract(a_not_b_theta_sketch) as a_not_b_uniq_count,
    udaf_us_east7.theta_sketch_extract(b_not_a_theta_sketch) as b_not_a_uniq_count,
    set_a_count,
    set_b_count,
    total_count,
    intersected_theta_sketch,
    union_theta_sketch,
    a_not_b_theta_sketch,
    b_not_a_theta_sketch,
FROM agg_data_set_a_b, set_difference;