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

* [cancel_jobs](#cancel_jobscancel_jobssql)

* [delete_datasets](#delete_datasetsproject_id-string-dataset_like_filter-stringdelete_datasetssql)

* [GetNextIds](#getnextidsid_count-int64-out-next_ids-arrayint64get_next_idsql)

## Documentation

### [cancel_jobs(project_id STRING, job_id_like_filter STRING)](cancel_jobs.sql)

Cancels all running or pending jobs which match the input argument, job_id_like_filter.

```sql
# Cancel jobs which match a job_id prefix
CALL bqutil.procedure.cancel_jobs("YOUR_PROJECT_ID", "job_id_prefix%");
# Cancel jobs which match a job_id suffix
CALL bqutil.procedure.cancel_jobs("YOUR_PROJECT_ID", "%job_id_suffix");
```

### [delete_datasets(project_id STRING, dataset_like_filter STRING)](delete_datasets.sql)

Deletes all BigQuery datasets which match the input argument, dataset_like_filter.
```sql
# Delete datasets which match a dataset_id prefix
CALL bqutil.procedure.delete_datasets("YOUR_PROJECT_ID", "dataset_id_prefix%");
# Delete datasets which match a dataset_id suffix
CALL bqutil.procedure.delete_datasets("YOUR_PROJECT_ID", "%dataset_id_suffix");
```

### [GetNextIds(id_count INT64, OUT next_ids ARRAY<INT64>)](get_next_id.sql)
Generates next ids and inserts them into a sample table. This implementation prevents against race condition.

```sql
BEGIN
  DECLARE next_ids ARRAY<INT64> DEFAULT [];
  CALL bqutil.procedure.GetNextIds(10, next_ids);
  SELECT FORMAT('IDs are: %t', next_ids);
END;

IDs are: [99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109]
```
