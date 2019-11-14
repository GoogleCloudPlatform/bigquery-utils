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
* [random_value](#random_valuearr-any-type)
* [translate](#translateexpression-string-characters_to_replace-string-characters_to_substitute-string)
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

Note that CAST(x AS INT64) rounds the number, while this function truncates it. In many cases, that's the behavior users expect.


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


### [y4md_to_date(y4md STRING)](y4md_to_date.sql)
Convert a STRING formatted as a YYYYMMDD to a DATE

```sql
SELECT bqutil.fn.y4md_to_date('20201220')

"2020-12-20"
```
