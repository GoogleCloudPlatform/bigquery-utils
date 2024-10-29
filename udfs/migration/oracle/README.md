# Oracle UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in Oracle. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`or` dataset for reference in queries.

## UDFs

* [round_datetime](#round_datetimedt-datetime-format-string)

## Documentation

### [round_datetime(d DATETIME, format STRING)](round_datetime.sqlx)
Emulates the `ROUND(DATE, VARCHAR)` function from Oracle as [documented here](https://docs.oracle.com/cd/B14117_01/server.101/b10759/functions121.htm).
