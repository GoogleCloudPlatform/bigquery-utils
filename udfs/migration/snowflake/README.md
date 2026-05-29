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

* [factorial](#factorial)
* [flatten](#flatten)


## Documentation

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


