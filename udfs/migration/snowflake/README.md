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


## Documentation

### [factorial(integer_expr INT64)](factorial.sqlx)
Computes the factorial of its input. The input argument must be an integer expression in the range of `0` to `27`. Due to data type differences, the maximum input value in BigQuery is smaller than in Snowflake. [Snowflake docs](https://docs.snowflake.com/en/sql-reference/functions/factorial.html)
```sql
SELECT bqutil.sf.factorial(10)

3628800
```
