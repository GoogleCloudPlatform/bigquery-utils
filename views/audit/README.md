This directory contains helper SELECT statements to query BigQuery audit logs \
More information regarding each is detailed below:


### [big_query_elt_script_logging.sql](/views/audit/big_query_elt_script_logging.sql)

A common pattern in data warehousing for tracking results of DML statements is to collect system variable values after each DML statement and write them to a separate logging table. With BigQuery, you no longer have to log your SQL statement results because Cloud Logging allows you to store, search, analyze, monitor, and set alerts on all your BigQuery scripting activity. The new version of Cloud Logging logs for BigQuery, BigQueryAuditMetadata, provides rich insights into the execution of your scripts. This data can give you insight into your script performance, modifications of your data, and more. This is a BigQuery SELECT statement that has extracted and formatted BigQueryMetaData events, allowing you to write simple queries to monitor your BigQuery jobs.

#### Prerequisites

[BigQueryAuditMetadata](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata)

1.  Define a BigQuery log sink using any of the following methods:
    *   [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)
        ```
        gcloud alpha logging sinks create my-example-sink \ 
        bigquery.googleapis.com/projects/my-project-id/datasets/auditlog_dataset \
        --log-filter='protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType="SCRIPT" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason="QUERY")' \ 
        --use-partitioned-tables
        ``` 
        Note: gcloud **alpha** is needed in order to use the parameter `--use-partitioned-tables` 
    *   [Cloud Console Logs Viewer](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create)
        Use this filter:
        #### protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType="SCRIPT" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason!="" OR protoPayload.metadata.tableDataRead.reason!=""  OR protoPayload.metadata.tableDeletion.reason!="" )
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

* Run this query to see job name, query, total number of billed bytes, job creation time, job start time, job end time, job runtime, and count of inserted rows and deleted rows for DML queries in a script.
  
  
  ```  
  SELECT 
   COALESCE(jobChange.jobStats.parentJobName, jobId) AS common_script_job_id,
   jobChange.jobConfig.queryConfig.query,
   jobChange.jobStats.queryStats.totalBilledBytes,
   jobChange.jobConfig.queryConfig.statementType,
   jobChange.jobStats.createTime,
   jobChange.jobStats.startTime,
   jobChange.jobStats.endTime,
   jobRuntimeMs,
   tableDataChange.deletedRowsCount,
   tableDataChange.insertedRowsCount,
  FROM YOUR_VIEW 
  WHERE 
  hasJobChangeEvent AND
  hasTableDataChangeEvent AND
  (jobChange.jobStats.parentJobName IS NOT NULL OR jobChange.jobConfig.queryConfig.statementType = 'SCRIPT')
  ORDER BY 
   jobChange.jobStats.startTime DESC,
   common_script_job_id
   
  ```

* Run this query to see job name, query, total number of billed bytes, job creation time, job start time, job end time, and job runtime table read queries in a script.

```  
  SELECT 
   COALESCE(jobChange.jobStats.parentJobName, jobId) AS common_script_job_id,
   jobChange.jobConfig.queryConfig.query,
   jobChange.jobStats.queryStats.totalBilledBytes,
   jobChange.jobConfig.queryConfig.statementType,
   jobChange.jobStats.createTime,
   jobChange.jobStats.startTime,
   jobChange.jobStats.endTime,
   jobRuntimeMs,
  FROM YOUR_VIEW 
  WHERE 
  hasJobChangeEvent AND 
  hasTableDataReadEvent AND
  (jobChange.jobStats.parentJobName IS NOT NULL OR jobChange.jobConfig.queryConfig.statementType = 'SCRIPT')
  ORDER BY 
   jobChange.jobStats.startTime DESC,
   common_script_job_id
   
  ```

* Run this query to see slot usage for query that uses reservations.

  ```
  
  SELECT 
   COALESCE(jobChange.jobStats.parentJobName, jobId) AS common_script_job_id,
   jobChange.jobStats.reservationUsage.name,
   jobChange.jobStats.reservationUsage.slotMs,
   jobChange.jobConfig.queryConfig.statementType,
   jobChange.jobStats.createTime,
   jobChange.jobStats.startTime,
   jobChange.jobStats.endTime,
   jobRuntimeMs,
  FROM YOUR_VIEW 
  WHERE 
  hasJobChangeEvent AND 
  jobChange.jobStats.reservationUsage.slotMs IS NOT NULL AND
  (jobChange.jobStats.parentJobName IS NOT NULL OR jobChange.jobConfig.queryConfig.statementType = 'SCRIPT')
  ORDER BY 
   jobChange.jobStats.startTime DESC,
   common_script_job_id
  
  ```
  
