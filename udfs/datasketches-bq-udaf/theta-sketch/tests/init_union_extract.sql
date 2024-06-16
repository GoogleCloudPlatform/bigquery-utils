-- Sample data creation ( 10B records, split into 100 groups of ~100M each )
CREATE OR REPLACE TABLE udaf_us_east7.sample_data AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,9)) as x2;



-- Init + Union + Extract
-- Query runtime: 8 secs ( depends on value of lg_k )
with
    agg_data_by_key AS (
        select
            group_key,
            count(*) as total_count,
            udaf_us_east7.theta_sketch_int64(x,14) as theta_sketch
        from udaf_us_east7.sample_data
        group by group_key
    ),
    agg_data AS (
        SELECT
            udaf_us_east7.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from agg_data_by_key
    )
select
    udaf_us_east7.theta_sketch_extract(merged_theta_sketch) as uniq_count,
    merged_theta_sketch,
    total_count
FROM agg_data;

-- Output:
-- uniq_ct = 1009073919.5415539
-- total_count = 1000000000
-- Error: 0.9%

-- https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html
-- https://datasketches.apache.org/docs/Theta/ThetaSize.html
-- lg_k = 14 -> k = 16384
-- Size/sketch: 1.3 to 2.5 KB




-- Using lg_k = 16

-- Query time: 25 sec
CREATE OR REPLACE TABLE udaf_us_east7.agg_data_by_key_theta AS
select
    group_key,
    count(*) as total_count,
    udaf_us_east7.theta_sketch_int64(x,16) as theta_sketch
from udaf_us_east7.sample_data
group by group_key;

-- Query time: 1 sec
WITH
    agg_data AS (
        SELECT
            udaf_us_east7.theta_sketch_union(theta_sketch,16) as merged_theta_sketch,
            SUM(total_count) as total_count
        from udaf_us_east7.agg_data_by_key_theta
    )
select
    udaf_us_east7.theta_sketch_extract(merged_theta_sketch) as uniq_count,
    merged_theta_sketch,
    total_count
FROM agg_data;
