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
