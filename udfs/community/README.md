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
* [find_in_set](#find_in_setstr-string-strlist-string)
* [freq_table](#freq_tablearr-any-type)
* [get_array_value](#get_array_valuek-string-arr-any-type)
* [get_value](#get_valuek-string-arr-any-type)
* [int](#intv-any-type)
* [json_typeof](#json_typeofjson-string)
* [last_day](#lastdaydt-date)
* [median](#medianarr-any-type)
* [nlp_compromise_number](#nlp_compromise_numberstr-string)
* [nlp_compromise_people](#nlp_compromise_peoplestr-string)
* [percentage_change](#percentage_changeval1-float64-val2-float64)
* [percentage_difference](#percentage_differenceval1-float64-val2-float64)
* [radians](#radiansx-any-type)
* [random_int](#random_intmin-any-type-max-any-type)
* [random_value](#random_valuearr-any-type)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)
* [typeof](#typeofinput-any-type)
* [url_keys](#url_keysquery-string)
* [url_param](#url_paramquery-string-p-string)
* [url_parse](#url_parseurlstring-string-parttoextract-string)
* [zeronorm](#zeronormx-any-type-meanx-float64-stddevx-float64)

## Documentation

### [csv_to_struct(strList STRING)](csv_to_struct.sql)
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



### [find_in_set(str STRING, strList STRING)](find_in_set.sql)
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



### [freq_table(arr ANY TYPE)](freq_table.sql)
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



### [get_array_value(k STRING, arr ANY TYPE)](get_array_value.sql)
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



### [get_value(k STRING, arr ANY TYPE)](get_value.sql)
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



### [int(v ANY TYPE)](int.sql)
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


### [json_typeof(json string)](json_typeof.sql)

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


### [last_day(dt DATE)](last_day.sql)

Get the date representing the last day of the month.

```sql
SELECT bqutil.fn.last_day(DATE("1987-12-25"))
  , bqutil.fn.last_day(DATE("1998-09-04"))
  , bqutil.fn.last_day(DATE("2020-02-21")) -- leap year
  , bqutil.fn.last_day(DATE("2019-02-21")) -- non-leap year
```

results:

|     f0_    |     f1_    |     f2_    |     f3_    |
|------------|------------|------------|------------|
| 1987-12-31 | 1998-09-30 | 2020-02-29 | 2019-02-28 |


### [median(arr ANY TYPE)](median.sql)
Get the median of an array of numbers.

```sql
SELECT bqutil.fn.median([1,1,1,2,3,4,5,100,1000]) median_1
  , bqutil.fn.median([1,2,3]) median_2
  , bqutil.fn.median([1,2,3,4]) median_3

3.0, 2.0, 2.5
```


### [nlp_compromise_number(str STRING)](nlp_compromise_number.sql)
Parse numbers from text.

```sql
SELECT bqutil.fn.nlp_compromise_number('one hundred fifty seven')
  , bqutil.fn.nlp_compromise_number('three point 5')
  , bqutil.fn.nlp_compromise_number('2 hundred')
  , bqutil.fn.nlp_compromise_number('minus 8')
  , bqutil.fn.nlp_compromise_number('5 million 3 hundred 25 point zero 1')

157, 3.5, 200, -8, 5000325.01
```


### [nlp_compromise_people(str STRING)](nlp_compromise_people.sql)
Extract names out of text.

```sql
SELECT bqutil.fn.nlp_compromise_people(
  "hello, I'm Felipe Hoffa and I work with Elliott Brossard - who thinks Jordan Tigani will like this post?"
) names

["felipe hoffa", "elliott brossard", "jordan tigani"]
```


### [percentage_change(val1 FLOAT64, val2 FLOAT64)](percentage_change.sql)
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


### [percentage_difference(val1 FLOAT64, val2 FLOAT64)](percentage_difference.sql)
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


### [radians(x ANY TYPE)](radians.sql)
Convert values into radian.

```sql
SELECT bqutil.fn.radians(180) is_this_pi

3.141592653589793
```


### [random_int(min ANY TYPE, max ANY TYPE)](random_int.sql)
Generate random integers between the min and max values.

```sql
SELECT bqutil.fn.random_int(0,10) randint, COUNT(*) c
FROM UNNEST(GENERATE_ARRAY(1,1000))
GROUP BY 1
ORDER BY 1
```


### [random_value(arr ANY TYPE)](random_value.sql)
Returns a random value from an array.

```sql
SELECT
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe']),
  bqutil.fn.random_value(['tino', 'jordan', 'julie', 'elliott', 'felipe'])

'tino', 'julie', 'jordan'
```


### [translate(expression STRING, characters_to_replace STRING, characters_to_substitute STRING)](translate.sql)
For a given expression, replaces all occurrences of specified characters with specified substitutes. Existing characters are mapped to replacement characters by their positions in the `characters_to_replace` and `characters_to_substitute` arguments. If more characters are specified in the `characters_to_replace` argument than in the `characters_to_substitute` argument, the extra characters from the `characters_to_replace` argument are omitted in the return value. 
```sql
SELECT bqutil.fn.translate('mint tea', 'inea', 'osin')

most tin
```


### [typeof(input ANY TYPE)](typeof.sql)

Return the type of input or 'UNKNOWN' if input is unknown typed value.

```sql
SELECT
  bqutil.fn.typeof(""),
  bqutil.fn.typeof(b""),
  bqutil.fn.typeof(1.0),
  bqutil.fn.typeof(STRUCT()),

STRING, BINARY, FLOAT64, STRUCT
```


### [url_keys(query STRING)](url_keys.sql)
Get an array of url param keys.

```sql
SELECT bqutil.fn.url_keys(
  'https://www.google.com/search?q=bigquery+udf&client=chrome')

["q", "client"]
```


### [url_param(query STRING, p STRING)](url_param.sql)
Get the value of a url param key.

```sql
SELECT bqutil.fn.url_param(
  'https://www.google.com/search?q=bigquery+udf&client=chrome', 'client')

"chrome"
```


### [url_parse(urlString STRING, partToExtract STRING)](url_parse_udf.sql)

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



### [y4md_to_date(y4md STRING)](y4md_to_date.sql)
Convert a STRING formatted as a YYYYMMDD to a DATE

```sql
SELECT bqutil.fn.y4md_to_date('20201220')

"2020-12-20"
```


### [zeronorm(x ANY TYPE, meanx FLOAT64, stddevx FLOAT64)](zeronorm.sql)
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

