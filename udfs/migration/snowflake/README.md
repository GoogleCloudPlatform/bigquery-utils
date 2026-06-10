# Snowflake UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Snowflake. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`sf` dataset for reference in queries.

For example, if you'd like to reference the `factorial` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.sf.factorial(0)
```

## UDFs

* [array_equal](#array_equal)
* [bitmap_bit_position](#bitmap_bit_position)
* [bitmap_bucket_number](#bitmap_bucket_number)
* [factorial](#factorial)
* [flatten](#flatten)
* [json_ilike](#json_ilike)
* [object_agg](#object_agg)


## Documentation

### [array_equal(a ARRAY<JSON>, b ARRAY<JSON>)](array_equal.sqlx)
Compares two arrays of JSON for equality, emulating Snowflake's `=` operator for untyped arrays.
*   Returns `true` if arrays are of equal length and all corresponding elements are equal.
*   Returns `false` if arrays are of different lengths or any corresponding elements are not equal.
*   Returns `null` if either input array is `null`.
*   Objects are compared recursively, ensuring they have the same keys and equal values (order of keys does not matter).
*   Nested arrays are compared recursively.
*   Null elements within arrays are treated as equal to other null elements.

```sql
SELECT bqutil.sf.array_equal([JSON '1', JSON '2'], [JSON '1', JSON '2']) as eq1,
       bqutil.sf.array_equal([JSON '{"a": 1}'], [JSON '{"a": 1}']) as eq2,
       bqutil.sf.array_equal([JSON '[1, 2]'], [JSON '[1, 3]']) as eq3;
```

eq1|eq2|eq3
---|---|---
true|true|false

### [bitmap_bit_position(value INT64)](bitmap_bit_position.sqlx)
Emulates the `bitmap_bit_position` function present in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/bitmap_bit_position)

```sql
SELECT bqutil.sf.bitmap_bit_position(1) as pos1,
       bqutil.sf.bitmap_bit_position(32768) as pos2,
       bqutil.sf.bitmap_bit_position(32769) as pos3;
```

pos1|pos2|pos3
---|---|---
0|32767|0

### [bitmap_bucket_number(value INT64)](bitmap_bucket_number.sqlx)
Emulates the `bitmap_bucket_number` function present in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/bitmap_bucket_number)

```sql
SELECT bqutil.sf.bitmap_bucket_number(1) as bucket1,
       bqutil.sf.bitmap_bucket_number(32768) as bucket2,
       bqutil.sf.bitmap_bucket_number(32769) as bucket3;
```

bucket1|bucket2|bucket3
---|---|---
1|1|2

### [factorial(integer_expr INT64)](factorial.sqlx)
Computes the factorial of its input. The input argument must be an integer expression in the range of `0` to `27`. Due to data type differences, the maximum input value in BigQuery is smaller than in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/factorial.html)
```sql
SELECT bqutil.sf.factorial(10)

3628800
```

### [flatten(input JSON, path STRING, outer_ BOOL, recursive_ BOOL, mode STRING)](flatten.sqlx)
Emulates the 'flatten' function present in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/flatten)
```sql
SELECT bqutil.sf.flatten(json_object('a', 1 , 'b', json_array(77, 88), 'c', json_object('d', 'X')), '', false, true, 'both');
```
SEQ|KEY|PATH|INDEX|VALUE|THIS
---|---|----|-----|-----|----
1|a|a|null|1|{"a":1,"b":[77,88],"c":{"d":"X"}}
2|b|b|null|[77,88]|{"a":1,"b":[77,88],"c":{"d":"X"}}
1|null|b[0]|0|77|[77,88]
2|null|b[1]|1|88|[77,88]
3|c|c|null|{"d":"X"}|{"a":1,"b":[77,88],"c":{"d":"X"}}
1|d|c.d|null|"X"|{"d":"X"}

### [json_ilike(input JSON, pattern STRING)](json_ilike.sqlx)
Helps to emulate the `SELECT {* ILIKE '%pattern%'}` syntax. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/data-types-semistructured#object-constants)
```sql
select
    bqutil.sf.json_ilike(to_json((select as struct subselect.*)), '%ra%') as j
  from
  (
    select 1 as xray, 2 as frame, 3 as id
  ) as subselect;
```

Row|j
---|-
1|{"frame":2,"xray":1}	

### [object_agg(key STRING, value JSON)](object_agg.sqlx)
Emulates the `OBJECT_AGG` function in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/object_agg)
```sql
SELECT object_agg(k, v)
FROM
  (
    SELECT 'a' AS k, json '1' AS v
    UNION ALL
    SELECT 'b' as k, json '2' as v);
```

Row|f0_
---|---
1|{"b":2,"a":1}

```sql
SELECT object_agg(k, v)
FROM
  (
    SELECT 'a' AS k, json '1' AS v
    UNION ALL
    SELECT 'a' AS k, json '3' AS b
    UNION ALL
    SELECT 'b' as k, json '2' as v);
```

Error: Duplicate field key 'a' at object_agg(STRING, JSON) line 7, columns 6-7


