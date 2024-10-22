# SQL Server UDFs

This directory contains [user-defined functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/user-defined-functions)
which mimic the behavior of proprietary functions in SQL Server. Each UDF within this
directory will be automatically synchronized to the `bqutil` project within the
`ss` dataset for reference in queries.

## UDFs

* [convert_datetime_string](#convert_datetime_stringt-datetime-mode-int64)
* [convert_timestamp_string](#convert_timestamp_stringt-timestamp-mode-int64)
* [convert_numeric_string](#convert_numeric_stringn-any-type-mode-int64)
* [convert_bytes_string](#convert_bytes_stringt-bytes-mode-int64)
* [convert_string_bytes](#convert_string_bytest-string-mode-int64)

## Documentation

### [convert_datetime_string(t DATETIME, mode INT64)](convert_datetime_string.sqlx)
Emulates the `CONVERT(VARCHAR, datetime_expression, style)` expression from SQL Server as [documented here](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#date-and-time-styles).

### [convert_timestamp_string(t TIMESTAMP, mode INT64)](convert_timestamp_string.sqlx)
Emulates the `CONVERT(VARCHAR, datetimeoffset_expression, style)` expression from SQL Server as [documented here](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#date-and-time-styles).

### [convert_numeric_string(n ANY TYPE, mode INT64)](convert_numeric_string.sqlx)
Emulates the `CONVERT(VARCHAR, numeric_expression, style)` expression from SQL Server as [documented here](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#float-and-real-styles).

### [convert_bytes_string(t BYTES, mode INT64)](convert_bytes_string.sqlx)
Emulates the `CONVERT(VARCHAR, varbinary_expression, style)` expression from SQL Server as [documented here](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#binary-styles).

### [convert_string_bytes(t STRING, mode INT64)](convert_string_bytes.sqlx)
Emulates the `CONVERT(VARBINARY, varchar_expression, style)` expression from SQL Server as [documented here](https://learn.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql?view=sql-server-ver16#binary-styles).
