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

* [ascii](#asciistring_expr-string)
* [decode](#decode-function)
* [index](#indexstring_expr1-string-string_expr2-string)
* [last_day](#last_daydate_expr-date)
* [months_between](#months_betweendate_expr1-date-date_expr2-date)
* [nullifzero](#nullifzeroexpr-any-type)
* [zeroifnull](#zeroifnullexpr-any-type)

## Documentation

### [ascii(string_expr STRING)](ascii.sql)
Returns the decimal representation of the first character in the `string_expr`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/qSvGNudIWmkd0nY_HkZ8~w)
```sql
SELECT bqutil.td.ascii('y')

121
```


### Decode function
Decode function compares expression `expr` with search parameters (`s1`,`s2`,...,`sN`) and returns n-th match from result parameters (`r1`,`r2`,...,`rN`).
Decode supports up to 10 search parameters.

More details can be found in [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ).

To match this functionality in BigQuery, we can define a UDF for each number of search parameters. Note the `def` can be set to `NULL` but the type must match the type of the result parameters. If `NULL` is passed, it should be casted to the proper type.

#### [decode1(expr  ANY TYPE, s1  ANY TYPE, r1  ANY TYPE, def  ANY TYPE)](decode1.sql)
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


#### [decode2(expr ANY TYPE, s1 ANY TYPE, r1 ANY TYPE, ..., [sn, rn], def ANY TYPE)](decode2.sql)
Returns `r1` if the `expr` is equal to `s1`, `r2` if the `expr` is equal to `s2`, else `def` is returned.
```sql
SELECT bqutil.td.decode2(1, 1, 'True', 0, 'False', '')
  , bqutil.td.decode2(0, 1, 'True', 0, 'False', 'def')
  , bqutil.td.decode2(3, 1, 'True', 0, 'False', CAST(NULL as STRING))

'True', 'False', null
```


#### [decode3(expr ANY TYPE, s1 ANY TYPE, r1 ANY TYPE, ..., [sn, rn], def ANY TYPE)](decode3.sql)
Returns `r1` if the `expr` is equal to `s1`, `r2` if the `expr` is equal to `s2`, `r3` if the `expr` is equal to `s3`, else `def` is returned.
```sql
SELECT bqutil.td.decode3(1, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3(0, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3(100, 1, 'True', 0, 'False', NULL, 'False', 'Invalid')
  , bqutil.td.decode3('F', 'F', 'Female', 'M', 'Male', 'O', 'Other', CAST(NULL as STRING))
  , bqutil.td.decode3('True', 'True', True, 'False', False, '', False, CAST(NULL as BOOLEAN))

'True', 'False', 'Invalid', 'Female' ,true
```


### [index(string_expr1 STRING, string_expr2 STRING)](index.sql)
Returns the 1-based index of the first occurrence of `string_expr2` inside `string_expr1`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/lYkmnMUSl7umkauHRSSITQ)
```sql
SELECT bqutil.td.index('BigQuery', 'Query')

4
```


### [last_day(date_expr DATE)](last_day.sql)
Returns the date of the last day in the month of the `date_expr`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/UYSHIofb6DaOFRBng8e3mQ)
```sql
SELECT bqutil.td.last_day('2019-07-05')

2019-07-31
```


### [months_between(date_expr1 DATE, date_expr2 DATE)](months_between.sql)
Returns the number of months between `date_expr1` and `date_expr2`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/ZrhSoO_oe_0dW9lkeueH1Q)
```sql
SELECT bqutil.td.months_between('2019-01-01', '2019-07-31')
  , bqutil.td.months_between('2019-07-31', '2019-01-01')

-6, 6
```


### [nullifzero(expr ANY TYPE)](nullifzero.sql)
Returns `NUll` if the `expr` evaluates to `0`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/WydeQxu0SJWrkTyxvekB7g)
```sql
SELECT bqutil.td.nullifzero(NULL)
  , bqutil.td.nullifzero(0)
  , bqutil.td.nullifzero(1)

NULL, NULL, 1
```


### [zeroifnull(expr ANY TYPE)](zeroifnull.sql)
Returns `0` if the `expr` evaluates to `NULL`. [Teradata docs](https://docs.teradata.com/reader/1DcoER_KpnGTfgPinRAFUw/4e57e7Mq4VCe5YtLMKoY4g)
```sql
SELECT bqutil.td.zeroifnull(NULL)
  , bqutil.td.zeroifnull(0)
  , bqutil.td.zeroifnull(1)

0, 0, 1
```
