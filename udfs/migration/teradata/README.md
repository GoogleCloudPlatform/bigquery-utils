# Teradata UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Teradata. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`td` dataset for reference in queries.

For example, if you'd like to reference the `nullifzero` function within your query,
you can reference it like the following:
```sql
SELECT bqutil.td.nullifzero(0)
```

## UDFs

* [ascii (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#ascii)
* [chr (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#chr)
* [decode](#decode-function)
* [index](#indexstring_expr1-string-string_expr2-string)
* [initcap (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#initcap)
* [instr (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#instr)
* [last_day (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/date_functions#last_day)
* [left (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#left)
* [months_between](#months_betweendate_expr1-date-date_expr2-date)
* [nullifzero](#nullifzeroexpr-any-type)
* [nvl](#nvlexpr1-any-type-expr2-any-type)
* [nvl2](#nvl2expr1-any-type-expr2-any-type-expr3-any-type)
* [otranslate](#otranslatesource_string-string-from_string-string-to_string-string)
* [right (now a native function)](https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#right)
* [zeroifnull](#zeroifnullexpr-any-type)

## Documentation

### Decode function
Decode function compares expression `expr` with search parameters (`s1`,`s2`,...,`sN`) and returns n-th match from result parameters (`r1`,`r2`,...,`rN`).
Decode supports up to 10 search parameters.

More details can be found in [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ).

To match this functionality in BigQuery, we can define a UDF for each number of search parameters. Note the `def` can be set to `NULL` but the type must match the type of the result parameters. If `NULL` is passed, it should be casted to the proper type.

#### [decode1(expr  ANY TYPE, s1  ANY TYPE, r1  ANY TYPE, def  ANY TYPE)](decode1.sqlx)
Returns `r1` if the `expr` is equal to `s1`, else `def` is returned.
```sql
SELECT bqutil.td.decode1(1, 1, 'One', CAST(NULL as STRING))
  , bqutil.td.decode1(0, 1, 'One', CAST(NULL as STRING))
  , bqutil.td.decode1('True', 'True', 1, 0)
  , bqutil.td.decode1('False', 'True', 1, 0)
  , bqutil.td.decode1(1, 1, 'One', 'Not One')
  , bqutil.td.decode1(0, 1, 'One', 'Not One')


'One', null, 1, 0, 'One', 'Not One'
```


#### [decode2(expr ANY TYPE, s1 ANY TYPE, r1 ANY TYPE, ..., [sn, rn], def ANY TYPE)](decode2.sqlx)
Returns `r1` if the `expr` is equal to `s1`, `r2` if the `expr` is equal to `s2`, else `def` is returned.
```sql
SELECT bqutil.td.decode2(1, 1, 'True', 0, 'False', '')
  , bqutil.td.decode2(0, 1, 'True', 0, 'False', 'def')
  , bqutil.td.decode2(3, 1, 'True', 0, 'False', CAST(NULL as STRING))

'True', 'False', null
```


#### [decode3(expr ANY TYPE, s1 ANY TYPE, r1 ANY TYPE, ..., [sn, rn], def ANY TYPE)](decode3.sqlx)
Returns `r1` if the `expr` is equal to `s1`, `r2` if the `expr` is equal to `s2`, `r3` if the `expr` is equal to `s3`, else `def` is returned.
```sql
SELECT bqutil.td.decode3(1, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3(0, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3(100, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3('F', 'F', 'Female', 'M', 'Male', 'O', 'Other', CAST(NULL as STRING))
  , bqutil.td.decode3('True', 'True', True, 'False', False, '', False, CAST(NULL as BOOLEAN))

'True', 'False', 'Invalid', 'Female' ,true
```


### [index(string_expr1 STRING, string_expr2 STRING)](index.sqlx)
Returns the 1-based index of the first occurrence of `string_expr2` inside `string_expr1`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/lYkmnMUSl7umkauHRSSITQ)
```sql
SELECT bqutil.td.index('BigQuery', 'Query')

4
```


### [months_between(date_expr1 DATE, date_expr2 DATE)](months_between.sqlx)
Returns the number of months between `date_expr1` and `date_expr2`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/ZrhSoO_oe_0dW9lkeueH1Q)
```sql
SELECT bqutil.td.months_between('2019-01-01', '2019-07-31')
  , bqutil.td.months_between('2019-07-31', '2019-01-01')

-6, 6
```


### [nullifzero(expr ANY TYPE)](nullifzero.sqlx)
Returns `NUll` if the `expr` evaluates to `0`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/WydeQxu0SJWrkTyxvekB7g)
```sql
SELECT bqutil.td.nullifzero(NULL)
  , bqutil.td.nullifzero(0)
  , bqutil.td.nullifzero(1)

NULL, NULL, 1
```


### [nvl(expr1 ANY TYPE, expr2 ANY TYPE)](nvl.sqlx)
Returns `expr2` if `expr1` evaluates to `NULL`, else `expr1`. [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/A3B8KYGf9EJhU2iCgqqrzw)
```sql
SELECT bqutil.td.nvl(NULL, 2.0)
  , bqutil.td.nvl(1.0, 2.0)

2.0, 1.0
```


### [nvl2(expr1 ANY TYPE, expr2 ANY TYPE, expr3 ANY TYPE)](nvl2.sqlx)
Returns `expr3` if `expr1` evaluates to `NULL`, else `expr2`. [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/_77jzgn34QLLsZDslWefpw)
```sql
SELECT bqutil.td.nvl2(NULL, 2.0, 3.0)
  , bqutil.td.nvl2(1.0, 2.0, 3.0)

3.0, 2.0
```


### [otranslate(source_string STRING, from_string STRING, to_string STRING)](otranslate.sqlx)
Returns `source_string` with every occurrence of each character in `from_string` replaced with the corresponding character in `to_string`. [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/UqxeIKN2A5LF~HyiovshLg)
```sql
SELECT bqutil.td.otranslate('Thin and Thick', 'Thk', 'Sp')

'Spin and Spic'
```


### [zeroifnull(expr ANY TYPE)](zeroifnull.sqlx)
Returns `0` if the `expr` evaluates to `NULL`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/4e57e7Mq4VCe5YtLMKoY4g)
```sql
SELECT bqutil.td.zeroifnull(NULL)
  , bqutil.td.zeroifnull(0)
  , bqutil.td.zeroifnull(1)

0, 0, 1
```
