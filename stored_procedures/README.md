# Stored Procedures

This directory contains [scripting](https://cloud.google.com/bigquery/docs/reference/standard-sql/scripting)
examples which mimic the behavior of features in a traditional database. Each stored procedure within this
directory will be automatically synchronized to the `bqutil` project within the
`procedure` dataset for reference in queries.

For example, if you'd like to reference the `GetNextIds` function within your query,
you can reference it like the following:
```sql
DECLARE next_ids ARRAY<INT64> DEFAULT [];
CALL bqutil.procedure.GetNextIds(10, next_ids);
```

## Stored Procedures

* [GetNextIds](#GetNextIds)

## Documentation

### [GetNextIds(id_count INT64, OUT next_ids ARRAY<INT64>)](GetNextIds.sql)
Generates next ids and inserts them into a sample table. This implementation prevents against race condition.
```sql
BEGIN
  DECLARE next_ids ARRAY<INT64> DEFAULT [];
  CALL bqutil.procedure.GetNextIds(10, next_ids);
  SELECT FORMAT('IDs are: %t', next_ids);
END;

IDs are: [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
```
