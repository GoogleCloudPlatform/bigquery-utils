CREATE TABLE IF NOT EXISTS udaf_us_east7.sample_data AS
SELECT 1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,9)) as x2;


with agg_data AS (
    select
        count(*) as total_count,
        udaf_us_east7.kll_sketch_int64(x,250) as kll_sketch
    from udaf_us_east7.sample_data
)
select
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.0) as mininum,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.5) as p50,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.75) as p75,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.95) as p95,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 1.0) as maximum,
    kll_sketch,
    total_count
from agg_data;


-- // Testing merging

CREATE OR REPLACE TABLE udaf_us_east7.sample_data AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,9)) as x2;


with
    agg_data_by_key AS (
        select
            group_key,
            count(*) as total_count,
            udaf_us_east7.kll_sketch_int64(x,25) as kll_sketch
        from udaf_us_east7.sample_data
        group by group_key
    ),
    agg_data AS (
        SELECT
            udaf_us_east7.kll_sketch_merge(kll_sketch,250) as merged_kll_sketch,
            SUM(total_count) as total_count
        from agg_data_by_key
    )
select
    udaf_us_east7.kll_sketch_quantile(merged_kll_sketch, 0.0) as mininum,
    udaf_us_east7.kll_sketch_quantile(merged_kll_sketch, 0.5) as p50,
    udaf_us_east7.kll_sketch_quantile(merged_kll_sketch, 0.75) as p75,
    udaf_us_east7.kll_sketch_quantile(merged_kll_sketch, 0.95) as p95,
    udaf_us_east7.kll_sketch_quantile(merged_kll_sketch, 1.0) as maximum,
    merged_kll_sketch,
    total_count
FROM agg_data;



-- ===== Testing init using float64 values =====
CREATE TABLE IF NOT EXISTS udaf_us_east7.sample_data AS
SELECT 1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,0)) as x2;


with agg_data AS (
    select
        count(*) as total_count,
        udaf_us_east7.kll_sketch_float64(cast(x as float64),250) as kll_sketch
    from udaf_us_east7.sample_data
)
select
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.0) as mininum,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.5) as p50,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.75) as p75,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 0.95) as p95,
    udaf_us_east7.kll_sketch_quantile(kll_sketch, 1.0) as maximum,
    kll_sketch,
    total_count
from agg_data;

