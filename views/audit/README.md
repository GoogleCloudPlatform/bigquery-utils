This directory contains helper views to query BigQuery audit logs \
More information regarding each is detailed below:


### [big_query_elt_script_logging.sql](/views/audit/big_query_elt_script_logging.sql)

A common pattern in data warehousing for tracking results of DML statements is to collect system variable values after each DML statement and write them to a separate logging table. With BigQuery, you no longer have to log your SQL statement results because Cloud Logging allows you to store, search, analyze, monitor, and set alerts on all your BigQuery scripting activity. The new version of BigQuery metadata logs, [BigQueryAuditMetadata](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata), provides rich insights into the execution of your scripts. This data can give you insight into your script performance, modifications of your data, and more. [big_query_elt_script_logging.sql](/views/audit/big_query_elt_script_logging.sql) is a BigQuery view that handles extracting and formatting BigQueryMetaData events so that you can focus on writing simple queries on top of this view to monitor your BigQuery jobs.

#### Prerequisites

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
        Use this advanced filter:
        #### protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ( (protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType="SCRIPT" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE" ) OR ( protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason!="" OR protoPayload.metadata.tableDataRead.reason!=""  OR protoPayload.metadata.tableDeletion.reason!="" )
        *   [Partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)
            is not required, but it is strongly recommended to select it for your BigQuery destination
            
    Note: You can create a log sink at the folder, billing account, or organization level using an 
    [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).
1.  The BigQuery audit log tables will be created in your dataset sink destination once you run a BigQuery job post sink creation.
1.  To use the [big_query_elt_audit_log_v2.sql](/views/audit/big_query_elt_audit_log_v2.sql) view, simply change
    all occurrences of `project_id.dataset_id` to your own project id and dataset name you used when creating the logging sink. 
    Run the following sed command with your own project and dataset IDs to perform this replacement:
    *   `sed
        's/project_id.dataset_id/YOUR_PROJECT_ID.YOUR_DATASET_ID/'
        big_query_elt_audit_log_v2.sql`
1.  Execute the [big_query_elt_audit_log_v2.sql](/views/audit/big_query_elt_audit_log_v2.sql) SQL in your BigQuery console or command line to
    create your view. Once created, you can do further analysis in BigQuery by querying the view, or
    you can connect it to a BI tool such as DataStudio to build dashboards.
    
#### Usage Examples
In the following examples, change all occurrences of `project_id.dataset_id` to your own values. 

* Run the following query to see the 100 most recent BigQuery scripting statements. The results are ordered with the most recent script statement first, and then further ordering is applied using the script's job id and statement start time.
  
  
  ```  
  SELECT 
    COALESCE(parentJobId, jobId) AS common_script_job_id,
    jobChange.jobConfig.queryConfig.query,
    jobChange.jobConfig.queryConfig.destinationTable,
    jobChange.jobStats.queryStats.totalBilledBytes,
    jobChange.jobConfig.queryConfig.statementType,
    jobChange.jobStats.createTime,
    jobChange.jobStats.startTime,
    jobChange.jobStats.endTime,
    jobRuntimeMs,
    tableDataChange.deletedRowsCount,
    tableDataChange.insertedRowsCount,
  FROM
    project_id.dataset_id.bq_script_logs 
  WHERE 
    hasJobChangeEvent
    AND (
      jobChange.jobStats.parentJobName IS NOT NULL
      OR jobChange.jobConfig.queryConfig.statementType = 'SCRIPT'
    )
  ORDER BY 
    eventTimestamp DESC,
    common_script_job_id,
    jobChange.jobStats.startTime
  LIMIT 100
   
  ```

* Run the following query to see the 100 most recent BigQuery scripting statements that modify table data. The results are ordered with the most recent script statement first, and then further ordering is applied using the statement's job id and statement start time. 

```  
  SELECT 
    parentJobId,
    jobId,
    jobChange.jobConfig.queryConfig.query,
    jobChange.jobConfig.queryConfig.destinationTable,
    jobChange.jobStats.queryStats.totalBilledBytes,
    jobChange.jobConfig.queryConfig.statementType,
    jobChange.jobStats.createTime,
    jobChange.jobStats.startTime,
    jobChange.jobStats.endTime,
    jobRuntimeMs,
  FROM
    project_id.dataset_id.bq_script_logs 
  WHERE 
    hasJobChangeEvent
    AND hasTableDataChangeEvent 
    AND jobChange.jobStats.parentJobName IS NOT NULL
  ORDER BY 
    eventTimestamp DESC,
    jobId,
    jobChange.jobStats.startTime
  LIMIT 100
  ```

* Run the following query to see the 100 most recent BigQuery scripting statements which use slot reservations. The results are ordered with the most recent script statement first, and then further ordering is applied using the script's job id and statement start time.

  ```
  SELECT 
    COALESCE(parentJobId, jobId) AS common_script_job_id,
    jobChange.jobStats.reservationUsage.name,
    jobChange.jobStats.reservationUsage.slotMs,
    jobChange.jobConfig.queryConfig.statementType,
    jobChange.jobConfig.queryConfig.destinationTable,
    jobChange.jobStats.createTime,
    jobChange.jobStats.startTime,
    jobChange.jobStats.endTime,
    jobRuntimeMs,
  FROM 
    project_id.dataset_id.bq_script_logs 
  WHERE 
    hasJobChangeEvent 
    AND (
      (jobChange.jobStats.parentJobName IS NOT NULL AND jobChange.jobStats.reservationUsage.slotMs IS NOT NULL) 
      OR jobChange.jobConfig.queryConfig.statementType = 'SCRIPT' 
    )
  ORDER BY 
    eventTimestamp DESC,
    common_script_job_id,
    jobChange.jobStats.startTime
  LIMIT 100
  ```
  
