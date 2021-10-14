# Community UDFs

This directory contains community contributed [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
to extend BigQuery for more specialized usage patterns. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`fn` dataset for reference in queries.

For example, if you'd like to reference the `int` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.fn.int(1.684)
```

## UDFs

* [csv_to_struct](#csv_to_structstrlist-string)
* [day_occurrence_of_month](#day_occurrence_of_monthdate_expression-any-type)
* [degrees](#degreesx-any-type)
* [find_in_set](#find_in_setstr-string-strlist-string)
* [freq_table](#freq_tablearr-any-type)
* [from_binary](#from_binaryvalue-string)
* [from_hex](#from_hexvalue-string)
* [get_array_value](#get_array_valuek-string-arr-any-type)
* [getbit](#getbittarget_arg-int64-target_bit_arg-int64)
* [get_value](#get_valuek-string-arr-any-type)
* [int](#intv-any-type)
* [jaccard](#jaccard)
* [json_extract_keys](#json_extract_keys)
* [json_extract_values](#json_extract_values)
* [json_typeof](#json_typeofjson-string)
* [kruskal_wallis](#kruskal_wallisarraystructfactor-string-val-float64)
* [last_day](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day)
* [levenshtein](#levenshteinsource-string-target-string-returns-int64)
* [linear_interpolate](#linear_interpolatepos-int64-prev-structx-int64-y-float64-next-structx-int64-y-float64)
* [linear_regression](#linear_regressionarraystructstructx-float64-y-float64)
* [median](#medianarr-any-type)
* [nlp_compromise_number](#nlp_compromise_numberstr-string)
* [nlp_compromise_people](#nlp_compromise_peoplestr-string)
* [percentage_change](#percentage_changeval1-float64-val2-float64)
* [percentage_difference](#percentage_differenceval1-float64-val2-float64)
* [pi](#pi)
* [pvalue](#pvalueh-float64-dof-float64)
* [radians](#radiansx-any-type)
* [random_int](#random_intmin-any-type-max-any-type)
* [random_string](#random_stringlength-int64)
* [random_value](#random_valuearr-any-type)
* [to_binary](#to_binaryx-int64)
* [to_hex](#to_hexx-int64)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)
* [ts_gen_keyed_timestamps](#ts_gen_keyed_timestampskeys-arraystring-tumble_seconds-int64-min_ts-timestamp-max_ts-timestamp)
* [ts_linear_interpolate](#ts_linear_interpolatepos-timestamp-prev-structx-timestamp-y-float64-next-structx-timestamp-y-float64)
* [ts_session_group](#ts_session_grouprow_ts-timestamp-prev_ts-timestamp-session_gap-int64)
* [ts_slide](#ts_slidets-timestamp-period-int64-duration-int64)
* [ts_tumble](#ts_tumbleinput_ts-timestamp-tumble_seconds-int64)
* [typeof](#typeofinput-any-type)
* [url_keys](#url_keysquery-string)
* [url_param](#url_paramquery-string-p-string)
* [url_parse](#url_parseurlstring-string-parttoextract-string)
* [week_of_month](#week_of_monthdate_expression-any-type)
* [y4md_to_date](#y4md_to_datey4md-string)
* [zeronorm](#zeronormx-any-type-meanx-float64-stddevx-float64)

## Documentation

### [csv_to_struct(strList STRING)](csv_to_struct.sqlx)
Take a list of comma separated key-value pairs and creates a struct.
Input:
strList: string that has map in the format a:b,c:d....
Output: struct for the above map.
```sql
WITH test_cases AS (
  SELECT NULL as s
  UNION ALL
  SELECT '' as s
  UNION ALL
  SELECT ',' as s
  UNION ALL
  SELECT ':' as s
  UNION ALL
  SELECT 'a:b' as s
  UNION ALL
  SELECT 'a:b,c:d' as s
  UNION ALL
  SELECT 'a:b' as s
)
SELECT key, value from test_cases as t, UNNEST(bqutil.fn.csv_to_struct(t.s)) s;
```

results:

| key | value |
|-----|-------|
| a   | b     |
| a   | b     |
| c   | d     |
| a   | b     |


### [day_occurrence_of_month(date_expression ANY TYPE)](day_occurrence_of_month.sqlx)
Returns the nth occurrence of the weekday in the month for the specified date. The result is an INTEGER value between 1 and 5.
```sql
SELECT
  bqutil.fn.day_occurrence_of_month(DATE '2020-07-01'),
  bqutil.fn.day_occurrence_of_month(DATE '2020-07-08');

1 2
```

### [degrees(x ANY TYPE)](degrees.sqlx)
Convert radians values into degrees.

```sql
SELECT bqutil.fn.degrees(3.141592653589793) is_this_pi

180.0
```


### [find_in_set(str STRING, strList STRING)](find_in_set.sqlx)
Returns the first occurance of str in strList where strList is a comma-delimited string.
Returns null if either argument is null.
Returns 0 if the first argument contains any commas.
For example, find_in_set('ab', 'abc,b,ab,c,def') returns 3.
Input:
str: string to search for.
strList: string in which to search.
Output: Position of str in strList
```sql
WITH test_cases AS (
  SELECT 'ab' as str, 'abc,b,ab,c,def' as strList
  UNION ALL
  SELECT 'ab' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
  UNION ALL
  SELECT 'mobile' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
  UNION ALL
  SELECT 'mobile,' as str, 'mobile,tablet,mobile/tablet,phone,text' as strList
)
SELECT bqutil.fn.find_in_set(str, strList) from test_cases
```

results:

| f0_  |
|------|
|    3 |
| NULL |
|    1 |
|    0 |



### [freq_table(arr ANY TYPE)](freq_table.sqlx)
Construct a frequency table (histogram) of an array of elements.
Frequency table is represented as an array of STRUCT(value, freq)

```sql
SELECT bqutil.fn.freq_table([1,2,1,3,1,5,1000,5]) ft
```

results:

|   Row   |  ft.value  |  ft.freq  |
|---------|------------|-----------|
|    1    |       1    |     3     |
|         |       2    |     1     |
|         |       3    |     1     |
|         |       5    |     2     |
|         |    1000    |     1     |


### [from_binary(value STRING)](from_binary.sqlx)
Returns a number in decimal form from its binary representation.

```sql
SELECT
  bqutil.fn.to_binary(x) AS binary,
  bqutil.fn.from_binary(bqutil.fn.to_binary(x)) AS x
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|                              binary                              |     x      |
|------------------------------------------------------------------|------------|
| 0000000000000000000000000000000000000000000000000000000000000001 |          1 |
| 0000000000000000000000000000000000000000000000011110001001000000 |     123456 |
| 0000000000000000000000000000001001001100101100000001011011101010 | 9876543210 |
| 1111111111111111111111111111111111111111111111111111110000010111 |      -1001 |


### [from_hex(value STRING)](from_hex.sqlx)
Returns a number in decimal form from its hexadecimal representation.

```sql
SELECT
  bqutil.fn.to_hex(x) AS hex,
  bqutil.fn.from_hex(bqutil.fn.to_hex(x)) AS x
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|       hex        |     x      |
|------------------|------------|
| 0000000000000001 |          1 |
| 000000000001e240 |     123456 |
| 000000024cb016ea | 9876543210 |
| fffffffffffffc17 |      -1001 |


### [get_array_value(k STRING, arr ANY TYPE)](get_array_value.sqlx)
Given a key and a map, returns the ARRAY type value.
This is same as get_value except it returns an ARRAY type.
This can be used when the map has multiple values for a given key.
```sql
WITH test AS (
  SELECT ARRAY(
    SELECT STRUCT('a' AS key, 'aaa' AS value) AS s
    UNION ALL
    SELECT STRUCT('b' AS key, 'bbb' AS value) AS s
    UNION ALL
    SELECT STRUCT('a' AS key, 'AAA' AS value) AS s
    UNION ALL
    SELECT STRUCT('c' AS key, 'ccc' AS value) AS s
  ) AS a
)
SELECT bqutil.fn.get_array_value('b', a), bqutil.fn.get_array_value('a', a), bqutil.fn.get_array_value('c', a) from test;
```

results:

|   f0_   |      f1_      |   f2_   |
|---------|---------------|---------|
| ["bbb"] | ["aaa","AAA"] | ["ccc"] |


### [getbit(target_arg INT64, target_bit_arg INT64)](getbit.sqlx)
Given an INTEGER value, returns the value of a bit at a specified position. The position of the bit starts from 0.

```sql
SELECT bqutil.fn.getbit(23, 2), bqutil.fn.getbit(23, 3), bqutil.fn.getbit(null, 1)

1 0 NULL
```

### [get_value(k STRING, arr ANY TYPE)](get_value.sqlx)
Given a key and a list of key-value maps in the form [{'key': 'a', 'value': 'aaa'}], returns the SCALAR type value.

```sql
WITH test AS (
  SELECT ARRAY(
    SELECT STRUCT('a' AS key, 'aaa' AS value) AS s
    UNION ALL
    SELECT STRUCT('b' AS key, 'bbb' AS value) AS s
    UNION ALL
    SELECT STRUCT('c' AS key, 'ccc' AS value) AS s
  ) AS a
)
SELECT bqutil.fn.get_value('b', a), bqutil.fn.get_value('a', a), bqutil.fn.get_value('c', a) from test;
```

results:

| f0_ | f1_ | f2_ |
|-----|-----|-----|
| bbb | aaa | ccc |



### [int(v ANY TYPE)](int.sqlx)
Convience wrapper which can be used to convert values to integers in place of
the native `CAST(x AS INT64)`.

```sql
SELECT bqutil.fn.int(1) int1
  , bqutil.fn.int(2.5) int2
  , bqutil.fn.int('7') int3
  , bqutil.fn.int('7.8') int4

1, 2, 7, 7
```

Note that CAST(x AS INT64) rounds the number, while this function truncates it. In many cases, that's the behavior users expect.

### [jaccard()](jaccard.sqlx)
Accepts two string and returns the distance using Jaccard algorithm. 
```sql
SELECT
       bqutil.fn.jaccard('thanks', 'thaanks'),
       bqutil.fn.jaccard('thanks', 'thanxs'),
       bqutil.fn.jaccard('bad demo', 'abd demo'),
       bqutil.fn.jaccard('edge case', 'no match'),
       bqutil.fn.jaccard('Special. Character?', 'special character'),
       bqutil.fn.jaccard('', ''),
1, 0.71, 1.0, 0.25, 0.67, 0.0
```

### [json_extract_keys()](json_extract_keys.sqlx)
Returns all keys in the input JSON as an array of string

```sql
SELECT bqutil.fn.json_extract_keys(
  '{"foo" : "cat", "bar": "dog", "hat": "rat"}'
) AS keys_array

foo
bar
hat
```

### [json_extract_values()](json_extract_values.sqlx)
Returns all values in the input JSON as an array of string

```sql
SELECT bqutil.fn.json_extract_values(
  '{"foo" : "cat", "bar": "dog", "hat": "rat"}'
) AS keys_array

cat
dog
rat
```

### [json_typeof(json string)](json_typeof.sqlx)

Returns the type of JSON value. It emulates [`json_typeof` of PostgreSQL](https://www.postgresql.org/docs/12/functions-json.html).

```sql
SELECT
       bqutil.fn.json_typeof('{"foo": "bar"}'),
       bqutil.fn.json_typeof(TO_JSON_STRING(("foo", "bar"))),
       bqutil.fn.json_typeof(TO_JSON_STRING([1,2,3])),
       bqutil.fn.json_typeof(TO_JSON_STRING("test")),
       bqutil.fn.json_typeof(TO_JSON_STRING(123)),
       bqutil.fn.json_typeof(TO_JSON_STRING(TRUE)),
       bqutil.fn.json_typeof(TO_JSON_STRING(FALSE)),
       bqutil.fn.json_typeof(TO_JSON_STRING(NULL)),

object, array, string, number, boolean, boolean, null
```

### [levenshtein(source STRING, target STRING) RETURNS INT64](levenshtein.sqlx)
Returns an integer number indicating the degree of similarity between two strings (0=identical, 1=single character difference, etc.)

```sql
SELECT
  source,
  target,
  bqutil.fn.levenshtein(source, target) distance,
FROM UNNEST([
  STRUCT('analyze' AS source, 'analyse' AS target),
  STRUCT('opossum', 'possum'),
  STRUCT('potatoe', 'potatoe'),
  STRUCT('while', 'whilst'),
  STRUCT('aluminum', 'alumininium'),
  STRUCT('Connecticut', 'CT')
]);
```

Row | source      | target      | distance
--- | ----------- | ----------- | ---------
1   |	analyze     | analyse     | 1
2   | opossum     | possum      | 1
3   | potatoe     | potatoe     | 0
4   | while       | whilst      | 2
5   | aluminum    | alumininium | 3
6   | Connecticut | CT          | 10

> This function is based on the [Levenshtein distance algorithm](https://en.wikipedia.org/wiki/Levenshtein_distance) which determines the minimum number of single-character edits (insertions, deletions or substitutions) required to change one source string into another target one.


### [linear_interpolate(pos INT64, prev STRUCT<x INT64, y FLOAT64>, next STRUCT<x INT64, y FLOAT64>)](linear_interpolate.sqlx)
Interpolate the current positions value from the preceding and folllowing coordinates

```sql
SELECT
  bqutil.fn.linear_interpolate(2, STRUCT(0 AS x, 0.0 AS y), STRUCT(10 AS x, 10.0 AS y)),
  bqutil.fn.linear_interpolate(2, STRUCT(0 AS x, 0.0 AS y), STRUCT(20 AS x, 10.0 AS y))
```

results:

| f0_ | f1_ |
|-----|-----|
| 2.0 | 1.0 |


### [median(arr ANY TYPE)](median.sqlx)
Get the median of an array of numbers.

```sql
SELECT bqutil.fn.median([1,1,1,2,3,4,5,100,1000]) median_1
  , bqutil.fn.median([1,2,3]) median_2
  , bqutil.fn.median([1,2,3,4]) median_3

3.0, 2.0, 2.5
```


### [nlp_compromise_number(str STRING)](nlp_compromise_number.sqlx)
Parse numbers from text.

```sql
SELECT bqutil.fn.nlp_compromise_number('one hundred fifty seven')
  , bqutil.fn.nlp_compromise_number('three point 5')
  , bqutil.fn.nlp_compromise_number('2 hundred')
  , bqutil.fn.nlp_compromise_number('minus 8')
  , bqutil.fn.nlp_compromise_number('5 million 3 hundred 25 point zero 1')

157, 3.5, 200, -8, 5000325.01
```


### [nlp_compromise_people(str STRING)](nlp_compromise_people.sqlx)
Extract names out of text.

```sql
SELECT bqutil.fn.nlp_compromise_people(
  "hello, I'm Felipe Hoffa and I work with Elliott Brossard - who thinks Jordan Tigani will like this post?"
) names

["felipe hoffa", "elliott brossard", "jordan tigani"]
```


### [percentage_change(val1 FLOAT64, val2 FLOAT64)](percentage_change.sqlx)
Calculate the percentage change (increase/decrease) between two numbers.

```sql
SELECT bqutil.fn.percentage_change(0.2, 0.4)
  , bqutil.fn.percentage_change(5, 15)
  , bqutil.fn.percentage_change(100, 50)
  , bqutil.fn.percentage_change(-20, -45)
```

results:

| f0_ | f1_ |  f2_  |   f3_   |
|-----|-----|-------|---------|
| 1.0 | 2.0 |  -0.5 |  -1.125 |


### [percentage_difference(val1 FLOAT64, val2 FLOAT64)](percentage_difference.sqlx)
Calculate the percentage difference between two numbers.

```sql
SELECT bqutil.fn.percentage_difference(0.2, 0.8)
  , bqutil.fn.percentage_difference(4.0, 12.0)
  , bqutil.fn.percentage_difference(100, 200)
  , bqutil.fn.percentage_difference(1.0, 1000000000)
```

results:

| f0_ | f1_ |   f2_   | f3_ |
|-----|-----|---------|-----|
| 1.2 | 1.0 |  0.6667 | 2.0 |

### [pi()](pi.sqlx)
Returns the value of pi.

```sql
SELECT bqutil.fn.pi() this_is_pi

3.141592653589793
```

### [radians(x ANY TYPE)](radians.sqlx)
Convert degree values into radian.

```sql
SELECT bqutil.fn.radians(180) is_this_pi

3.141592653589793
```


### [random_int(min ANY TYPE, max ANY TYPE)](random_int.sqlx)
Generate random integers between the min and max values.

```sql
SELECT bqutil.fn.random_int(0,10) randint, COUNT(*) c
FROM UNNEST(GENERATE_ARRAY(1,1000))
GROUP BY 1
ORDER BY 1
```


### [random_value(arr ANY TYPE)](random_value.sqlx)
Returns a random value from an array.

```sql
SELECT
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe'])

'tino', 'julie', 'jordan'
```

### [to_binary(x INT64)](to_binary.sqlx)
Returns a binary representation of a number.

```sql
SELECT
  x,
  bqutil.fn.to_binary(x) AS binary
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:

|     x      |                              binary                              |
|------------|------------------------------------------------------------------|
|          1 | 0000000000000000000000000000000000000000000000000000000000000001 |
|     123456 | 0000000000000000000000000000000000000000000000011110001001000000 |
| 9876543210 | 0000000000000000000000000000001001001100101100000001011011101010 |
|      -1001 | 1111111111111111111111111111111111111111111111111111110000010111 |


### [to_hex(x INT64)](to_hex.sqlx)
Returns a hexadecimal representation of a number.

```sql
SELECT
  x,
  bqutil.fn.to_hex(x) AS hex
FROM
  UNNEST([1, 123456, 9876543210, -1001]) AS x;
```

results:
|     x      |       hex        |
|------------|------------------|
|          1 | 0000000000000001 |
|     123456 | 000000000001e240 |
| 9876543210 | 000000024cb016ea |
|      -1001 | fffffffffffffc17 |


### [random_string(length INT64)](random_string.sqlx)
Returns a random string of specified length. Individual characters are chosen uniformly at random from the following pool of characters: 0-9, a-z, A-Z.

```sql
SELECT
  bqutil.fn.random_string(5),
  bqutil.fn.random_string(7),
  bqutil.fn.random_string(10)

'mb3AP' 'aQG5XYB' '0D5WFVQuq6'
```


### [translate(expression STRING, characters_to_replace STRING, characters_to_substitute STRING)](translate.sqlx)
For a given expression, replaces all occurrences of specified characters with specified substitutes. Existing characters are mapped to replacement characters by their positions in the `characters_to_replace` and `characters_to_substitute` arguments. If more characters are specified in the `characters_to_replace` argument than in the `characters_to_substitute` argument, the extra characters from the `characters_to_replace` argument are omitted in the return value.
```sql
SELECT bqutil.fn.translate('mint tea', 'inea', 'osin')

most tin
```

### [ts_gen_keyed_timestamps(keys ARRAY<STRING>, tumble_seconds INT64, min_ts TIMESTAMP, max_ts TIMESTAMP)](ts_gen_keyed_timestamps.sqlx)
Generate a timestamp array associated with each key

```sql
SELECT *
FROM
  UNNEST(bqutil.fn.ts_gen_keyed_timestamps(['abc', 'def'], 60, TIMESTAMP '2020-01-01 00:30:00', TIMESTAMP '2020-01-01 00:31:00))
```

| series_key | tumble_val
|------------|-------------------------|
| abc        | 2020-01-01 00:30:00 UTC |
| def        | 2020-01-01 00:30:00 UTC |
| abc        | 2020-01-01 00:31:00 UTC |
| def        | 2020-01-01 00:31:00 UTC |


### [ts_linear_interpolate(pos TIMESTAMP, prev STRUCT<x TIMESTAMP, y FLOAT64>, next STRUCT<x TIMESTAMP, y FLOAT64>)](ts_linear_interpolation.sqlx)
Interpolate the positions value using timestamp seconds as the x-axis

```sql
select bqutil.fn.ts_linear_interpolate(
  TIMESTAMP '2020-01-01 00:30:00',
  STRUCT(TIMESTAMP '2020-01-01 00:29:00' AS x, 1.0 AS y),
  STRUCT(TIMESTAMP '2020-01-01 00:31:00' AS x, 3.0 AS y)
)
```

| f0_ |
|-----|
| 2.0 |

### [ts_session_group(row_ts TIMESTAMP, prev_ts TIMESTAMP, session_gap INT64)](ts_session_group.sqlx)
Function to compare two timestamp as being within the same session window. A timestamp in the same session window as its previous timestamp will evaluate as NULL, otherwise the current row's timestamp is returned.  The "LAST_VALUE(ts IGNORE NULLS)" window function can then be used to stamp all rows with the starting timestamp for the session window.

```sql
--5 minute (300 seconds) session window
WITH ticks AS (
  SELECT 'abc' as key, 1.0 AS price, CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP) AS ts
  UNION ALL
  SELECT 'abc', 2.0, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 3.0, CAST('2020-01-01 01:05:01 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 4.0, CAST('2020-01-01 01:09:01 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 'abc', 5.0, CAST('2020-01-01 01:24:01 UTC' AS TIMESTAMP)
)
SELECT
  * EXCEPT(session_group),
  LAST_VALUE(session_group IGNORE NULLS)
    OVER (PARTITION BY key ORDER BY ts ASC) AS session_group
FROM (
  SELECT
    *,
    bqutil.fn.ts_session_group(
      ts,
      LAG(ts) OVER (PARTITION BY key ORDER BY ts ASC),
      300
    ) AS session_group
  FROM ticks
)
```

| key | price | ts                      |  sesssion_group         |
|-----|-------|-------------------------|-------------------------|
| abc | 1.0   | 2020-01-01 01:04:59 UTC | 2020-01-01 01:04:59 UTC |
| abc | 2.0   | 2020-01-01 01:05:00 UTC | 2020-01-01 01:04:59 UTC |
| abc | 3.0   | 2020-01-01 01:05:01 UTC | 2020-01-01 01:04:59 UTC |
| abc | 4.0   | 2020-01-01 01:09:01 UTC | 2020-01-01 01:04:59 UTC |
| abc | 5.0   | 2020-01-01 01:24:01 UTC | 2020-01-01 01:24:01 UTC |


### [ts_slide(ts TIMESTAMP, period INT64, duration INT64)](ts_slide.sqlx)
Calculate the sliding windows the ts parameter belongs to.

```sql
-- show a 15 minute window every 5 minutes and a 15 minute window every 10 minutes
WITH ticks AS (
  SELECT 1.0 AS price, CAST('2020-01-01 01:04:59 UTC' AS TIMESTAMP) AS ts
  UNION ALL
  SELECT 2.0, CAST('2020-01-01 01:05:00 UTC' AS TIMESTAMP)
  UNION ALL
  SELECT 3.0, CAST('2020-01-01 01:05:01 UTC' AS TIMESTAMP)
)
SELECT
  price,
  ts,
  bqutil.fn.ts_slide(ts, 300, 900) as _5_15,
  bqutil.fn.ts_slide(ts, 600, 900) as _10_15,
FROM ticks
```

| price | ts                      | _5_15.window_start      | _5_15.window_end        | _5_15.window_start      | _5_15.window_end        |
|-------|-------------------------|-------------------------|-------------------------|-------------------------|-------------------------|
| 1.0   | 2020-01-01 01:04:59 UTC | 2020-01-01 00:50:00 UTC | 2020-01-01 01:05:00 UTC | 2020-01-01 00:50:00 UTC | 2020-01-01 01:05:00 UTC |
|       |                         | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
| 2.0   | 2020-01-01 01:05:00 UTC | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
|       |                         | 2020-01-01 01:05:00 UTC | 2020-01-01 01:20:00 UTC |                         |                         |
| 3.0   | 2020-01-01 01:05:01 UTC | 2020-01-01 00:55:00 UTC | 2020-01-01 01:10:00 UTC | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |
|       |                         | 2020-01-01 01:00:00 UTC | 2020-01-01 01:15:00 UTC |                         |                         |
|       |                         | 2020-01-01 01:05:00 UTC | 2020-01-01 01:20:00 UTC |                         |                         |



### [ts_tumble(input_ts TIMESTAMP, tumble_seconds INT64)](ts_tumble.sqlx)
Calculate the [tumbling window](https://cloud.google.com/dataflow/docs/concepts/streaming-pipelines#tumbling-windows) the input_ts belongs in

```sql
SELECT
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 900) AS min_15,
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 600) AS min_10,
  fn.ts_tumble(TIMESTAMP '2020-01-01 00:17:30', 60) As min_1
```

| min_15                  | min_10                  |                         |
|-------------------------|-------------------------|-------------------------|
| 2020-01-01 00:15:00 UTC | 2020-01-01 00:10:00 UTC | 2020-01-01 00:17:00 UTC |


### [typeof(input ANY TYPE)](typeof.sqlx)

Return the type of input or 'UNKNOWN' if input is unknown typed value.

```sql
SELECT
  bqutil.fn.typeof(""),
  bqutil.fn.typeof(b""),
  bqutil.fn.typeof(1.0),
  bqutil.fn.typeof(STRUCT()),

STRING, BINARY, FLOAT64, STRUCT
```


### [url_keys(query STRING)](url_keys.sqlx)
Get an array of url param keys.

```sql
SELECT bqutil.fn.url_keys(
  'https://www.google.com/search?q=bigquery+udf&client=chrome')

["q", "client"]
```


### [url_param(query STRING, p STRING)](url_param.sqlx)
Get the value of a url param key.

```sql
SELECT bqutil.fn.url_param(
  'https://www.google.com/search?q=bigquery+udf&client=chrome', 'client')

"chrome"
```


### [url_parse(urlString STRING, partToExtract STRING)](url_parse_udf.sqlx)

Returns the specified part from the URL. Valid values for partToExtract include HOST, PATH, QUERY, REF, PROTOCOL
For example, url_parse('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') returns 'facebook.com'.
```sql
WITH urls AS (
  SELECT 'http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1' as url
  UNION ALL
  SELECT 'rpc://facebook.com/' as url
)
SELECT bqutil.fn.url_parse(url, 'HOST'), bqutil.fn.url_parse(url, 'PATH'), bqutil.fn.url_parse(url, 'QUERY'), bqutil.fn.url_parse(url, 'REF'), bqutil.fn.url_parse(url, 'PROTOCOL') from urls
```

results:

|     f0_      |     f1_     |       f2_        | f3_  | f4_  |
|--------------|-------------|------------------|------|------|
| facebook.com | path1/p.php | k1=v1&k2=v2#Ref1 | Ref1 | http |
| facebook.com | NULL        | NULL             | NULL | rpc  |


### [week_of_month(date_expression ANY TYPE)](week_of_month.sqlx)
Returns the number of weeks from the beginning of the month to the specified date. The result is an INTEGER value between 1 and 5, representing the nth occurrence of the week in the month. The value 0 means the partial week.

```sql
SELECT
  bqutil.fn.week_of_month(DATE '2020-07-01'),
  bqutil.fn.week_of_month(DATE '2020-07-08');

0 1
```

### [y4md_to_date(y4md STRING)](y4md_to_date.sqlx)
Convert a STRING formatted as a YYYYMMDD to a DATE

```sql
SELECT bqutil.fn.y4md_to_date('20201220')

"2020-12-20"
```


### [zeronorm(x ANY TYPE, meanx FLOAT64, stddevx FLOAT64)](zeronorm.sqlx)
Normalize a variable so that it has zero mean and unit variance.

```sql
with r AS (
  SELECT 10 AS x
  UNION ALL SELECT 20
  UNION ALL SELECT 30
  UNION ALL SELECT 40
  UNION ALL SELECT 50
),
stats AS (
  SELECT AVG(x) AS meanx, STDDEV(x) AS stddevx
  FROM r
)
SELECT x, bqutil.fn.zeronorm(x, meanx, stddevx) AS zeronorm
FROM r, stats;
```

returns:

| Row | x | zeronorm |
| --- | -- | ------- |
| 1	| 10 | -12.649110640673518 |
| 2	| 20 | -6.324555320336759 |
| 3	| 30 | 0.0 |
| 4	| 40 | 6.324555320336759 |
| 5	| 50 | 12.649110640673518 |


<br/>
<br/>
<br/>

# StatsLib: Statistical UDFs

This section details the subset of community contributed [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
that extend BigQuery and enable more specialized Statistical Analysis usage patterns.
Each UDF detailed below will be automatically synchronized to the `fn` dataset
within the `bqutil` project for reference in your queries.

For example, if you'd like to reference the `int` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.fn.int(1.684)
```

## UDFs

* [kruskal_wallis](#kruskal_wallisarrstructfactor-string-val-float64)

## Documentation

### [kruskal_wallis(ARRAY(STRUCT(factor STRING, val FLOAT64))](kruskal_wallis.sqlx)
Takes an array of struct where each struct (point) represents a measurement, with a group label and a measurement value

The [Kruskal–Wallis test by ranks](https://en.wikipedia.org/wiki/Kruskal%E2%80%93Wallis_one-way_analysis_of_variance), Kruskal–Wallis H test (named after William Kruskal and W. Allen Wallis), or one-way ANOVA on ranks is a non-parametric method for testing whether samples originate from the same distribution. It is used for comparing two or more independent samples of equal or different sample sizes. It extends the Mann–Whitney U test, which is used for comparing only two groups. The parametric equivalent of the Kruskal–Wallis test is the one-way analysis of variance (ANOVA).

* Input: array: struct <factor STRING, val FLOAT64>
* Output: struct<H FLOAT64, p-value FLOAT64, DOF FLOAT64>
```sql
DECLARE data ARRAY<STRUCT<factor STRING, val FLOAT64>>;

set data = [
('a',1.0),
('b',2.0),
('c',2.3),
('a',1.4),
('b',2.2),
('c',5.5),
('a',1.0),
('b',2.3),
('c',2.3),
('a',1.1),
('b',7.2),
('c',2.8)
];


SELECT `bqutil.fn.kruskal_wallis`(data) AS results;
```

results:

| results.H	| results.p	| results.DoF	|
|-----------|-----------|-------------|
| 3.4230769 | 0.1805877 | 2           |



### [linear_regression(ARRAY(STRUCT(STRUCT(X FLOAT64, Y FLOAT64))](linear_regression.sqlx)
Takes an array of STRUCT X, Y and returns _a, b, r_ where _Y = a*X + b_, and _r_ is the "goodness of fit measure.

The [Linear Regression](https://en.wikipedia.org/wiki/Linear_regression), is a linear approach to modelling the relationship between a scalar response and one or more explanatory variables (also known as dependent and independent variables).

* Input: array: struct <X FLOAT64, Y FLOAT64>
* Output: struct<a FLOAT64,b FLOAT64, r FLOAT64>
*
```sql
DECLARE data ARRAY<STRUCT<X STRING, Y FLOAT64>>;
set data = [ (5.1,2.5), (5.0,2.0), (5.7,2.6), (6.0,2.2), (5.8,2.6), (5.5,2.3), (6.1,2.8), (5.5,2.5), (6.4,3.2), (5.6,3.0)];
SELECT `bqutils.fn.linear_regression`(data) AS results;
```

results:


| results.a          	| results.b	         | results.r	       |
|---------------------|--------------------|-------------------|
| -0.4353361094588436 | 0.5300416418798544 | 0.632366563565354 |




### [pvalue(H FLOAT64, dof FLOAT64)](pvalue.sqlx)
Takes _H_ and _dof_ and returns _p_ probability value.

The [pvalue](https://jstat.github.io/distributions.html#jStat.chisquare.cdf) is NULL Hypothesis probability of the Kruskal-Wallis (KW) test. This is obtained to be the CDF of the chisquare with the _H_ value and the Degrees of Freedom (_dof_) of the KW problem.

* Input: H FLOAT64, dof FLOAT64
* Output: p FLOAT64
*
```sql
SELECT `bqutils.fn.pvalue`(.3,2) AS results;
```

results:


| results         	|
|-------------------|
|0.8607079764250578 |
