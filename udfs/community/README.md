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

* [int](#intv-any-type)
* [median](#medianarr-any-type)
* [nlp_compromise_number](#nlp_compromise_numberstr-string)
* [nlp_compromise_people](#nlp_compromise_peoplestr-string)
* [radians](#radiansx-any-type)
* [random_int](#random_intmin-any-type-max-any-type)
* [url_keys](#url_keysquery-string)
* [url_param](#url_paramquery-string-p-string)

## Documentation

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
