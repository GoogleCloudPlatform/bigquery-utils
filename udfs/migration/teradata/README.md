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
### Decode function
Decode function compares expression against search parameters and gives as a return value result parameter - [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ).

Decode in Teradata supports 1 to 10 search parameters (with 1 to 10 result parameters). Search parameters can be NULL and default is optional. 

In BigQuery UDF there is method for different amount o search parameters and `def` parameter is required but can be set to `NULL` - have in mind that def type need to match r1, r2 etc, so if NULL is passed it should be casted to proper type.

#### [decode1(exp  ANY TYPE, s1  ANY TYPE, r1  ANY TYPE, def  ANY TYPE)](decode1.sql)
Returns r1 if the `expr` is equal to s1, else def is returned. [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ)
```sql
SELECT bqutil.td.decode1(1,1,'One', CAST(NULL as STRING))
  , bqutil.td.decode1(0,1,'One', CAST(NULL as STRING))
  , bqutil.td.decode1('True','True',1, 0)
  , bqutil.td.decode1('False','True',1, 0)
  , bqutil.td.decode1(1,1,'One', 'Not One')
  , bqutil.td.decode1(0,1,'One', 'Not One')

	
'One', null, 1, 0, 'One', 'Not One'
```

#### [decode2(exp  ANY TYPE, s1  ANY TYPE, r1  ANY TYPE, s2  ANY TYPE, r2  ANY TYPE, def  ANY TYPE)](decode2.sql)
Returns r1 if the `expr` is equal to s1, r2 if the `expr` is equal to s2, else def is returned.  [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ)
```sql
SELECT bqutil.td.decode2(1,1,'True',0,'False','')
  , bqutil.td.decode2(0,1,'True',0,'False', 'def')
  , bqutil.td.decode2(3,1,'True',0,'False', cast(NULL as STRING))

'True', 'False', null
```

#### [decode3(exp  ANY TYPE, s1  ANY TYPE, r1  ANY TYPE, s2  ANY TYPE, r2  ANY TYPE, s3  ANY TYPE, r3  ANY TYPE, def  ANY TYPE)](decode3.sql)
Returns r1 if the `expr` is equal to s1, r2 if the `expr` is equal to s2, r3 if the `expr` is equal to s3, else def is returned. [Teradata docs](https://docs.teradata.com/reader/kmuOwjp1zEYg98JsB8fu_A/8Jial4oyTcTU94YzVNRWIQ)
```sql
SELECT bqutil.td.decode3(1,1,'True',0,'False',NULL, 'False', 'Invalid')
  , bqutil.td.decode3(0,1,'True',0,'False',NULL, 'False', 'Invalid')
  , bqutil.td.decode3(100,1,'True',0,'False',NULL, 'False', 'Invalid')
  , bqutil.td.decode3('F','F','Female','M','Male', 'O', 'Other', cast(NULL as STRING))
  , bqutil.td.decode3('True','True',True,'False',False, '', False, cast(NULL as boolean))

'True', 'False', 'Invalid', 'Female' ,true
```

