This directory contains helper SELECT statements to query BigQuery audit logs \
More information regarding each is detailed below:


### [big_query_elt_audit_log_v2.sql](/views/audit/big_query_elt_audit_log_v2.sql)

BigQuery SELECT statement to help you extract and format BigQueryMetaData
events.

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
        #### protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobInsertion.job.jobConfig.queryConfig.statementType="SCRIPT" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason!="" OR protoPayload.metadata.tableDataRead.reason!=""  OR protoPayload.metadata.tableDeletion.reason!="" OR protoPayload.metadata.modelDataChange.reason!="" )
        *   Make sure to select
            [partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)
            for your BigQuery destination
            
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
Change all occurrences of `project_id.dataset_id.table_id` to the full path to the view. 

* Run this query to see job name, query, create time, start time, end time, job runtime, count of inserted rows and deleted rows, and total billed bytes
  
  
  ```  
  SELECT 
    tableDataChange.jobName,
    jobChange.jobConfig.queryConfig.query,
    jobChange.jobStats.createTime,
    jobChange.jobStats.startTime,
    jobChange.jobStats.endTime,
    jobRuntimeMs,
    tableDataChange.deletedRowsCount,
    tableDataChange.insertedRowsCount,
    jobChange.jobStats.queryStats.totalBilledBytes
  FROM `project_id.dataset_id.table_id`  
  GROUP BY jobChange.jobConfig.queryConfig.statementType
  
  ``` 
* Run this query to see job name, query, job create time, job start time, job end time, query, job runtime, and total billed bytes for SELECT queries. 
  
  ```
  SELECT 
   tableDataRead.jobName,
   jobChange.jobConfig.queryConfig.query,
   jobChange.jobStats.createTime,
   jobChange.jobStats.startTime,
   jobChange.jobStats.endTime,
   jobRuntimeMs,
   jobChange.jobStats.queryStats.totalBilledBytes
  FROM `project_id.dataset_id.table_id`
  WHERE jobChange.jobConfig.queryConfig.statementType="SELECT"

  ```

