This directory contains helper SELECT statements to query BigQuery audit logs \
More information regarding each is detailed below:


### [big_query_elt_audit_log_v2.sql](/views/audit/big_query_elt_audit_log_v2.sql)

Customers who run scripts in legacy data warehouses such as Teradata, understandably want to track statement results and metrics. For example, tracking the number of rows affected after a set of DML queries are executed. In order to collect this information for analysis, users would have to set system variables and make logging calls after each DML statement. Essentially, users are in charge of managing monitoring. This can become very tedious for users who run thousands, if not more, scripts in a day.  With BigQuery, you no longer have to log your SQL statement results because Cloud Logging allows you to store, search, analyze, monitor, and set alerts on all your BigQuery scripting activity. The new version of Cloud Logging logs for BigQuery, BigQueryAuditMetadata, provides rich insights into the execution of your scripts. This data can give you insight into your script performance, modifications of your data, and more. This is a BigQuery SELECT statement that has extracted and formatted BigQueryMetaData events, allowing you to write simple queries to monitor your BigQuery jobs.

#### Prerequisites

[BigQueryAuditMetadata](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata)

1.  Define a BigQuery log sink using any of the following methods:
    *   [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)
        ```
        gcloud alpha logging sinks create my-example-sink \ 
        bigquery.googleapis.com/projects/my-project-id/datasets/auditlog_dataset \
        --log-filter='protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobInsertion.job.jobConfig.queryConfig.statementType="SCRIPT" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason="QUERY")' \ 
        --use-partitioned-tables
        ``` 
        Note: gcloud **alpha** is needed in order to use the parameter `--use-partitioned-tables` 
    *   [Cloud Console Logs Viewer](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create)
        Use this filter:
        #### protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobInsertion.job.jobConfig.queryConfig.statementType="SCRIPT" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason!="" OR protoPayload.metadata.tableDataRead.reason!=""  OR protoPayload.metadata.tableDeletion.reason!="" )
        *   [Partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)
            is not required, but it is strongly recommended to select it for your BigQuery destination
            
    Note: You can create a log sink at the folder, billing account, or organization level using an 
    [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).
1.  The log sink will immediately create the BigQuery dataset but the table will
    be created once you run a BigQuery job post log sink creation.
1.  To use the SELECT statement in
    [big_query_elt_audit_log_v2.sql](/views/audit/big_query_elt_audit_log_v2.sql), change
    all occurrences of
    `project_id.dataset_id.cloudaudit_googleapis_com_data_access` to be the full
    table path you created in step 1.
    *   `sed
        's/project_id.dataset_id.cloudaudit_googleapis_com_data_access/YOUR_PROJECT.YOUR_DATASET.YOUR_TABLE/'
        big_query_elt_audit_log_v2.sql`
1.  From here, you can do further analysis in BigQuery by querying the view, or
    you can connect it to a BI tool such as DataStudio as a data source and
    build dashboards.
    
#### Usage Examples
Change all occurrences of `YOUR_VIEW` to the full path to the view. 

* Run this query to see job name, query, create time, start time, end time, job runtime, count of inserted rows and deleted rows, and total billed bytes
  
  
  ```  
  SELECT
   jobChange.jobStats.parentJobName,
   ARRAY_AGG(jobChange.jobConfig.queryConfig.statementType IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as statementType,
   ARRAY_AGG(tableDataChange.jobName IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobName,
   ARRAY_AGG(jobChange.jobConfig.queryConfig.query IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as query,
   ARRAY_AGG(jobChange.jobStats.createTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as createTime,
   ARRAY_AGG(jobChange.jobStats.startTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as startTime,
   ARRAY_AGG(jobChange.jobStats.endTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as endTime,
   ARRAY_AGG(jobRuntimeMs IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobRuntimeMs,
   ARRAY_AGG(tableDataChange.deletedRowsCount IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as deletedRowsCount,
   ARRAY_AGG(tableDataChange.insertedRowsCount IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as insertedRowsCount,
   ARRAY_AGG(jobChange.jobStats.queryStats.totalBilledBytes IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as totalBilledBytes,
  FROM YOUR_VIEW
  WHERE
  (jobChange.jobConfig.queryConfig.statementType="INSERT" OR
  jobChange.jobConfig.queryConfig.statementType="DELETE" OR
  jobChange.jobConfig.queryConfig.statementType="UPDATE" OR
  jobChange.jobConfig.queryConfig.statementType="MERGE")
  AND jobChange.jobStats.parentJobName IS NOT NULL
  GROUP BY 1

  ``` 
* Run this query to see job name, query, job create time, job start time, job end time, query, job runtime, and total billed bytes for SELECT queries. 
  
  ```
  SELECT
   jobChange.jobStats.parentJobName,
   ARRAY_AGG(jobChange.jobConfig.queryConfig.statementType IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as statementType,
   ARRAY_AGG(tableDataRead.jobName IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobName,
   ARRAY_AGG(jobChange.jobConfig.queryConfig.query IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as query,
   ARRAY_AGG(jobChange.jobStats.createTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as createTime,
   ARRAY_AGG(jobChange.jobStats.startTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as startTime,
   ARRAY_AGG(jobChange.jobStats.endTime IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as endTime,
   ARRAY_AGG(jobRuntimeMs IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobRuntimeMs,
   ARRAY_AGG(jobChange.jobStats.queryStats.totalBilledBytes IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as totalBilledBytes,
  FROM YOUR_VIEW
  WHERE
  (jobChange.jobConfig.queryConfig.statementType="SELECT") AND
  jobChange.jobStats.parentJobName IS NOT NULL
  GROUP BY 1

  ```
* Run this query to see reservation usage and runtime for scripts.
  
  ```
  SELECT 
   jobChange.jobStats.parentJobName,
   ARRAY_AGG(tableDataChange.jobName IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobName,
   ARRAY_CONCAT_AGG(jobChange.jobStats.reservationUsage.name ORDER BY jobChange.jobStats.startTime) as reservationName,
   ARRAY_CONCAT_AGG(jobChange.jobStats.reservationUsage.slotMs ORDER BY jobChange.jobStats.startTime) as reservationSlotMs,
   ARRAY_AGG(jobRuntimeMs IGNORE NULLS ORDER BY jobChange.jobStats.startTime) as jobRuntimeMs,
  FROM YOUR_VIEW
  WHERE jobChange.jobStats.reservationUsage.slotMs IS NOT NULL AND
  jobChange.jobStats.parentJobName IS NOT NULL
  GROUP BY 1
  
  ```
  
