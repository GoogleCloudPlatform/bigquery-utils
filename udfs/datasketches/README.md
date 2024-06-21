# Supporting Datasketches in BigQuery

## Contents
<!-- TOC -->
* [Introduction](#introduction)
* [Featured Sketches in this Integration](#featured-sketches-in-this-integration)
* [Solution Approach](#solution-approach)
* [Theta Sketch](#theta-sketch)
  * [Lg_k - Precision parameter](#lgk---precision-parameter)
  * [Examples](#examples)
* [KLL Sketch](#kll-sketch)
  * [K - Precision Parameter](#k---precision-parameter)
  * [Examples](#examples-1)
* [Tuple Sketch](#tuple-sketch)
  * [Lg_k - Precision parameter](#lgk---precision-parameter-1)
  * [Examples](#examples-2)
<!-- TOC -->

## Introduction
This project enhances Google BigQuery by integrating a suite of powerful sketch functions from [Apache DataSketches](https://datasketches.apache.org/), enabling efficient probabilistic data analysis on massive datasets.

Apache Datasketches is an open source, high-performance library of stochastic streaming algorithms commonly called "sketches" in the data sciences. Sketches are small, stateful programs that process massive data as a stream and can provide approximate answers, with mathematical guarantees, to computationally difficult queries orders-of-magnitude faster than traditional, exact methods.

## Featured Sketches in this Integration

This project focuses on incorporating the following sketch types

1. [**Theta Sketch**](#theta-sketch): A sketch ideal for cardinality estimation and set operations (union, intersection, difference).
2. [**KLL Sketch**](#kll-sketch): A sketch designed for quantile estimation.
3. [**Tuple Sketch**](#tuple-sketch): An extension of the Theta Sketch that supports associating values with the estimated unique items.


## Solution Approach 

Custom sketch C++ implementation using Apache Datasketches [C++ core library](https://github.com/apache/datasketches-cpp) is compiled to [WebAssembly](https://webassembly.org/) (WASM) libraries using [emscripten](https://emscripten.org/) toolchain and loaded in BigQuery [JS UDAFs](https://cloud.google.com/bigquery/docs/user-defined-aggregates) and [JS UDFs](https://cloud.google.com/bigquery/docs/user-defined-functions#javascript-udf-structure)

Note: 
- All the functions defined below are deployed in `bqutil.fn` and `bqutil.fn_<bq_region>` public datasets
- You can also deploy these functions in your own dataset. Refer to this [README](../README.md).

## Theta Sketch
A [Theta Sketch](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html) is a data structure and algorithm used to perform approximate count discount calculations on a large dataset without having to store them all individually. More details can be found in this [Apache Datasketch](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html) public doc. 

| Type      | Function Spec                                                                                                                                                                                                                                                                                                                                                                                                                                      |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Aggregate | **FunctionName**: [theta_sketch_int64(id_col, lg_k)](../community/theta_sketch_int64.sqlx) <br> **Input**: Id_col -> INT64,  lg_k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates id_col, log_k args and returns a theta sketch.                                                                                                                                                                                 |
| Aggregate | **FunctionName**: [theta_sketch_bytes(bytes_col, lg_k)](../community/theta_sketch_bytes.sqlx) <br> **Input**: Bytes_col -> BYTES,  lg_k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates bytes_col, log_k args and returns a theta sketch.  <br> **Note**: This function can also be used for initializing theta sketch for string cols. Just CAST( string_col AS BYTES FORMAT 'UTF-8') and pass to this function |
| Aggregate | **FunctionName**: [theta_sketch_union(theta_sketch, lg_k)](../community/theta_sketch_union.sqlx) <br> **Input**: Sketch Bytes, lg_k -> INT64 (constant) <br> **Output**: sketch Bytes<br> **Description**: Aggregates multiple theta sketches, performs a union op and returns a merged theta sketch                                                                                                                                               |
| Aggregate | **FunctionName**: [theta_sketch_intersection(theta_sketch)](../community/theta_sketch_intersection.sqlx) <br> **Input**: Sketch Bytes <br> **Output**: sketch Bytes<br> **Description**: Aggregates multiple theta sketches, performs an intersection op and returns a merged theta sketch                                                                                                                                                         |
| Scalar    | **FunctionName**: [theta_sketch_a_not_b(theta_sketch_a, theta_sketch_b)](../community/theta_sketch_a_not_b.sqlx) <br> **Input**: sketch_a -> BYTES, sketch_b -> BYTES <br> **Output**: sketch Bytes<br> **Description**: Takes in 2 theta sketches, performs a difference op / a_not_b op (i.e SetA - SetB)  and returns a theta_sketch                                                                                                            |
| Scalar    | **FunctionName**: [theta_sketch_extract(theta_sketch)](../community/theta_sketch_extract.sqlx) <br> **Input**: theta_sketch -> Bytes <br> **Output**: FLOAT64 <br> **Description**: Takes in a theta sketch, returns approx distinct count of entries of the id_col used to create the theta sketch                                                                                                                                                |

### Lg_k - Precision parameter

Lg_k is one of the parameters in select theta sketch functions.    
Choice of lg_k int64 parameter is important as it has direct correlation between your query slot usage, theta_sketch size and relative error.
- Lg_k vs K vs Relative error comparison can be found in [this public doc](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html)
- theta_sketch size vs K comparison can be found in [this public doc](https://datasketches.apache.org/docs/Theta/ThetaSize.html)

### Examples

1. Generates 2 tables with sample data ( treated as SetA, setB)

```sql
-- Generate ints from [1, 5M]
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.set_a AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(0,4)) as x2;

-- Generate ints from [4M, 7M]
CREATE OR REPLACE TABLE  `$BQ_PROJECT.$BQ_DATASET`.set_b AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
     UNNEST(GENERATE_ARRAY(4,6)) as x2;
```
2.Groups on a certain key and aggregates theta_sketches per key
```sql
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.theta_agg_by_key_set_a AS
select
    group_key,
    count(*) as total_count,
    bqutil.fn.theta_sketch_int64(x,14) as theta_sketch
from `$BQ_PROJECT.$BQ_DATASET`.set_a
group by group_key;

CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.theta_agg_by_key_set_b AS
select
    group_key,
    count(*) as total_count,
    bqutil.fn.theta_sketch_int64(x,14) as theta_sketch
from `$BQ_PROJECT.$BQ_DATASET`.set_b
group by group_key;
```

4. Merges those grouped theta_sketches, find unique counts for Union, Intersection, AnotB, BnotA set operations.
```sql
WITH
    union_data_set_a AS (
        SELECT
            bqutil.fn.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from `$BQ_PROJECT.$BQ_DATASET`.theta_agg_by_key_set_a
    ),
    union_data_set_b AS (
        SELECT
            bqutil.fn.theta_sketch_union(theta_sketch,14) as merged_theta_sketch,
            SUM(total_count) as total_count
        from `$BQ_PROJECT.$BQ_DATASET`.theta_agg_by_key_set_b
    ),
    agg_data_set_a_b AS (
        SELECT
            bqutil.fn.theta_sketch_intersection(merged_theta_sketch) as intersected_theta_sketch,
            sum(total_count) as total_count,
            bqutil.fn.theta_sketch_union(merged_theta_sketch, 14) as union_theta_sketch
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
            bqutil.fn.theta_sketch_a_not_b(a.merged_theta_sketch, b.merged_theta_sketch) as a_not_b_theta_sketch,
            bqutil.fn.theta_sketch_a_not_b(b.merged_theta_sketch, a.merged_theta_sketch) as b_not_a_theta_sketch,
            a.total_count as set_a_count,
            b.total_count as set_b_count
        from  union_data_set_a a, union_data_set_b b
    )
select
    "SetA = [1, 5M] \nSetB = (4M, 7M]" as set_info,
    bqutil.fn.theta_sketch_extract(intersected_theta_sketch) as intersect_uniq_count,
    bqutil.fn.theta_sketch_extract(union_theta_sketch) as union_uniq_count,
    bqutil.fn.theta_sketch_extract(a_not_b_theta_sketch) as a_not_b_uniq_count,
    bqutil.fn.theta_sketch_extract(b_not_a_theta_sketch) as b_not_a_uniq_count,
    set_a_count,
    set_b_count,
    total_count,
    intersected_theta_sketch,
    union_theta_sketch,
    a_not_b_theta_sketch,
    b_not_a_theta_sketch,
FROM agg_data_set_a_b, set_difference;
```

## KLL Sketch
[KLL sketch](https://datasketches.apache.org/docs/KLL/KLLSketch.html) is a quantile type mergeable streaming sketch algorithm to estimate the distribution of values, and approximately answer queries about quantiles (median, min, max, 95th percentile and such).

| Type      | Function Spec                                                                                                                                                                                                                                                                                                                          |
|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Aggregate | **FunctionName**: [kll_sketch_int64(id_col, k)](../community/kll_sketch_int64.sqlx) <br> **Input**: Id_col -> INT64,  k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates id_col, k args and returns a KLL sketch.                                                                                     |
| Aggregate | **FunctionName**: [kll_sketch_float64(id_col, k)](../community/kll_sketch_float64.sqlx) <br> **Input**: Id_col -> FLOAT64,  k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates id_col, k args and returns a KLL sketch.                                                                               |
| Aggregate | **FunctionName**: [kll_sketch_merge(kll_sketch, k)](../community/kll_sketch_merge.sqlx) <br> **Input**: Bytes_col -> BYTES,  k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates KLL sketches, k arg, performs a union op and returns a merged KLL sketch.                                             |
| Scalar    | **FunctionName**: [kll_sketch_quantile(kll_sketch, rank)](../community/kll_sketch_quantile.sqlx) <br> **Input**: Sketch Bytes, rank -> FLOAT64 in range [0,1] <br> **Output**: FLOAT64 <br> **Description**: Takes in KLL sketch and rank value and returns quantile value. eg. a rank of 0.5 will return median, rank = 1 returns max |

### K - Precision Parameter
k is one of the arguments in select KLL sketch functions. Choice of k int64 is important as it has direct correlation between your query runtime, slot usage, kll_sketch size and relative error.
- K vs Relative error vs KLL-sketch size comparison can be found in [this public doc](https://datasketches.apache.org/docs/KLL/KLLAccuracyAndSize.html)

### Examples

1. Creating sample data with 10B records ( 1 through 10B) split in 100 nearly equal sized groups of 100M values
Note: This sample data generation query might run for a long time ( > 30 mins ) to generate 10B records. Try reducing the size for a faster outcome.
```sql
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.sample_data_10B AS
SELECT
CONCAT("group_key_", CAST(RAND()*100 AS INT64)) as group_key,
1000000*x2 + x1 as x
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
UNNEST(GENERATE_ARRAY(0,9999)) as x2;
```

2. Creating KLL merge sketches for a group key

```sql
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.agg_sample_data_10B AS
select
group_key,
count(*) as total_count,
bqutil.fn.kll_sketch_int64(x,250) as kll_sketch
from `$BQ_PROJECT.$BQ_DATASET`.sample_data_10B
group by group_key;
```

3. Merge group based sketches into a single sketch and then get approx quantiles

```sql
WITH
agg_data AS (
SELECT
bqutil.fn.kll_sketch_merge(kll_sketch,250) as merged_kll_sketch,
SUM(total_count) as total_count
from `$BQ_PROJECT.$BQ_DATASET`.agg_sample_data_10B
)
select
bqutil.fn.kll_sketch_quantile(merged_kll_sketch, 0.0) as mininum,
bqutil.fn.kll_sketch_quantile(merged_kll_sketch, 0.5) as p50,
bqutil.fn.kll_sketch_quantile(merged_kll_sketch, 0.75) as p75,
bqutil.fn.kll_sketch_quantile(merged_kll_sketch, 0.95) as p95,
bqutil.fn.kll_sketch_quantile(merged_kll_sketch, 1.0) as maximum,
merged_kll_sketch,
total_count
FROM agg_data;
```

## Tuple Sketch 
A [Tuple Sketch](https://datasketches.apache.org/docs/Tuple/TupleOverview.html) is an extension of the [Theta Sketch](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html). Tuple sketches store an additional summary value with each retained entry which makes the sketch ideal for summarizing attributes such as impressions or clicks. Tuple sketches enable set operations over a stream of data, and can also be used for cardinality estimation.

| Type      | Function Spec                                                                                                                                                                                                                                                                                                                                                                                                                       |
|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Aggregate | **FunctionName**: [tuple_sketch_int64(id_col, value_col, lg_k)](../community/tuple_sketch_int64.sqlx) <br> **Input**: Id_col -> INT64, value_col -> INT64,  lg_k -> INT64(constant) <br> **Output:** Sketch Bytes <br> **Description:** Aggregates id_col, value_col and log_k args and returns a tuple sketch.                                                                                                                     | 
| Aggregate | **FunctionName**: [tuple_sketch_union(tuple_sketch, lg_k)](../community/tuple_sketch_union.sqlx) <br> **Input**: Sketch Bytes, lg_k -> INT64 (constant) <br> **Output**: sketch Bytes<br> **Description**: Aggregates multiple tuple sketches, performs a union op and returns a merged tuple sketch                                                                                                                                |
| Scalar    | **FunctionName**: [tuple_sketch_extract_count(tuple_sketch)](../community/tuple_sketch_extract_count.sqlx) <br> **Input**: tuple_sketch -> Bytes <br> **Output**: INT64 <br> **Description**: Takes in a tuple sketch, returns approx distinct count of entries of the id_col used to create the tuple sketch                                                                                                                       |
| Scalar    | **FunctionName**: [tuple_sketch_extract_sum(tuple_sketch)](../community/tuple_sketch_extract_sum.sqlx) <br> **Input**: tuple_sketch -> Bytes <br> **Output**: INT64 <br> **Description**: Takes in a tuple sketch, combines the summary values of the value_col (using sum) from the random sample of id_col stored within the Tuple sketch,  calculates an estimate that applies to the entire dataset and returns the sum.        |
| Scalar    | **FunctionName**: [tuple_sketch_extract_avg(tuple_sketch)](../community/tuple_sketch_extract_avg.sqlx) <br> **Input**: tuple_sketch -> Bytes <br> **Output**: INT64 <br> **Description**:  Takes in a tuple sketch, combines the summary values of the value_col (using sum) from the random sample of id_col stored within the Tuple sketch, calculates an estimate that applies to the entire dataset and returns average.        |
| Scalar    | **FunctionName**: [tuple_sketch_extract_summary(tuple_sketch)](../community/tuple_sketch_extract_summary.sqlx) <br> **Input**: tuple_sketch -> Bytes <br> **Output**: STRUCT<key_distinct_count INT64, value_sum INT64, value_avg INT64> <br> **Description**: Takes in a tuple sketch and returns summary of key and value cols i.e struct<uniq_count, sum, avg>. This function combines output of the other 3 scalar functions above. |

###  Lg_k - Precision parameter

Lg_k is one of the parameters in select tuple sketch functions.    
Choice of lg_k int64 parameter is important as it has direct correlation between your query slot usage, tuple_sketch size and relative error.
- Lg_k vs K vs Relative error comparison can be found in [this public doc](https://datasketches.apache.org/docs/Theta/ThetaErrorTable.html)
- theta_sketch size vs K comparison can be found in [this public doc](https://datasketches.apache.org/docs/Theta/ThetaSize.html)  

Note:  As there is no dedicated tuple_sketch size vs lg_k table, You can assume tuple_sketch size will be nearly double of the theta_sketch size due to additional summary col maintained per hash key in the sketch. 

### Examples

1. Creating sample data with 100M records ( 1 through 100M) split in 10 nearly equal sized groups of 10M values

```sql
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.sample_data_100M AS
SELECT
    CONCAT("group_key_", CAST(RAND()*10 AS INT64)) as group_key,
    1000000*x2 + x1 as user_id, 
    X2 as clicks
from UNNEST(GENERATE_ARRAY(1,1000000)) as x1,
    UNNEST(GENERATE_ARRAY(0,99)) as x2
```

2.  Creating Tuple sketches for a group key 

```sql
CREATE OR REPLACE TABLE `$BQ_PROJECT.$BQ_DATASET`.agg_sample_data_100M AS 
select
    group_key,
    count(distinct user_id) as exact_uniq_users_ct,
    sum(clicks) as exact_clicks_ct,
    bqutil.fn.tuple_sketch_int64(user_id,clicks,14) as tuple_sketch
from `$BQ_PROJECT.$BQ_DATASET`.sample_data_100M
group by group_key;
```

3. Calculating tuple_sketch_summary for every grouped tuple_sketch and comparing with exact metrics

```sql
select
    group_key,
    exact_uniq_users_ct,
    exact_clicks_ct,
    FLOOR(exact_clicks_ct/exact_uniq_users_ct) as clicks_avg,
    bqutil.fn.tuple_sketch_extract_summary(tuple_sketch) as tuple_sketch_summary
from `$BQ_PROJECT.$BQ_DATASET`.agg_sample_data_100M
```

4.  Merge group based sketches into a single sketch and then extract relevant metric aggregations like sum, avg and distinct count

```sql
WITH
agg_data AS (
    SELECT
         bqutil.fn.tuple_sketch_union(tuple_sketch,14) as merged_tuple_sketch,
         SUM(exact_uniq_users_ct) as total_uniq_users_ct, 
    from `$BQ_PROJECT.$BQ_DATASET`.agg_sample_data_100M
)
Select
    total_uniq_users_ct,
   bqutil.fn.tuple_sketch_extract_summary(merged_tuple_sketch) as summary,
   bqutil.fn.tuple_sketch_extract_count(merged_tuple_sketch) as approx_dist_user_ct,
   bqutil.fn.tuple_sketch_extract_sum(merged_tuple_sketch) as approx_clicks_ct,
   bqutil.fn.tuple_sketch_extract_avg(merged_tuple_sketch) as approx_clicks_avg,
   merged_tuple_sketch
FROM agg_data;
```