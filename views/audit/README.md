# BigQuery Audit Logging Views

This directory contains views which are designed to query audit log datasets
which **you must
[create via Cloud Logging sinks](https://cloud.google.com/logging/docs/export/bigquery)**
.

The following views are based on the newer v2 BigQueryAuditMetadata logs:

* [bigquery_audit_logs_v2.sql](bigquery_audit_logs_v2.sql) - Getting started
  details [here](#getting-started-with-bigquery_audit_logs_v2sql)
* [bigquery_script_logs_v2.sql](bigquery_script_logs_v2.sql) - Getting started
  details [here](#getting-started-with-bigquery_script_logs_v2sql)

The following views are based on the older v1 BigQUery AuditData logs:

* [bigquery_audit_logs_v1.sql](bigquery_audit_logs_v1.sql) - Getting started
  details [here](#getting-started-1)

> If you are looking to query INFORMATION_SCHEMA views which don't require
> setting up Cloud Logging sinks, please refer to the online
> documented examples:
>
> * [Dataset Metadata](https://cloud.google.com/bigquery/docs/information-schema-datasets)
> * [Job Metadata](https://cloud.google.com/bigquery/docs/information-schema-jobs)
> * [Job Timeline Metadata](https://cloud.google.com/bigquery/docs/information-schema-jobs-timeline)
> * [Reservation Metadata](https://cloud.google.com/bigquery/docs/information-schema-reservations)
> * [Streaming Metadata](https://cloud.google.com/bigquery/docs/information-schema-streaming)
> * [Routine Metadata](https://cloud.google.com/bigquery/docs/information-schema-routines)
> * [Table Metadata](https://cloud.google.com/bigquery/docs/information-schema-tables)
> * [View Metadata](https://cloud.google.com/bigquery/docs/information-schema-views)

## Which logs version should you use (v2 or v1)?

Google Cloud's audit log message system relies on structured logs, and the
BigQuery service provides three distinct kinds of structured log messages:

* [`BigQueryAuditMetadata`](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata):
  The new (v2) version of logs, which reports resource interactions such as
  which tables were read from and written to by a given query job and which
  tables expired due to having an expiration time configured.

  > Note: In general, you'll want to leverage these new
  > `BigQueryAuditMetadata` v2 logs.

* [`AuditData`](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData):
  The old (v1) version of logs, which reports API invocations.

* [`AuditLog`](https://cloud.google.com/logging/docs/reference/audit/auditlog/rest/Shared.Types/AuditLog):
  The logs that
  [BigQuery Reservations](https://cloud.google.com/bigquery/docs/reservations-intro)
  and
  [BigQuery Connections](https://cloud.google.com/bigquery/docs/reference/bigqueryconnection/rest)
  use when reporting requests.

## BigQueryAuditMetadata (v2 Logs)

The [bigquery_audit_logs_v2.sql](bigquery_audit_logs_v2.sql)
and [bigquery_script_logs_v2.sql](/views/audit/bigquery_script_logs_v2.sql)
views in this directory read the newer, more detailed, Version 2 BigQuery log
events,
[BigQueryAuditMetadata](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata)
. The logs include information such as which tables were read/written by a given
query job and which tables expired due to having an expiration time configured.

### Getting Started with bigquery_audit_logs_v2.sql

This view extracts and presents BigQuery audit log information into the
following organized structs which mimic the BigQuery API structs:

* [jobChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.jobchange)
* [tableDataChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.tabledatachange)
* [tableCreationEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.tablecreation)
* [tableChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.tablechange)
* [tableDeletionEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.tabledeletion)
* [tableDataReadEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.tabledataread)
* [modelDeletionEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.modeldeletion)
* [modelCreationEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.modelcreation)
* [modelMetadataChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.modelmetadatachange)
* [modelDataChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.modeldatachange)
* [routineCreationEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.routinecreation)
* [routineChangeEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.routinechange)
* [routineDeletionEvent](https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#bigqueryauditmetadata.routinedeletion)

1. Set your environment variables, replace \<your-project\> and \<your-dataset\>
   with the appropriate information

   ```shell
   export PROJECT_ID=<your-project-id>
   export DATASET_ID=<your-dataset-id>
   ```

1. Create a BigQuery dataset to store the audit logs, the logs tables will be
   created and filled with data once you run a BigQuery job post log sink
   creation.

   ```shell
   bq --project_id ${PROJECT_ID} mk ${DATASET_ID}
   ```
1. Define a log sink using any of the following methods:

   > Note: You can create a log sink at the folder, billing account, or
   > organization level using an
   > [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).

    * [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)

      ```shell
      gcloud logging sinks create bq-audit-logs-v2-sink \
      bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/${DATASET_ID} \
      --log-filter='protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata"' \
      --use-partitioned-tables
      ```

    * [Cloud Console Logs Viewer](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create)

      > Important: Make sure to select
      > [partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)
      > for your BigQuery destination.

1. Assign the log sink's service account the BigQuery Data Editor role

    * In the console, navigate to Logging >> Logs Router
    * Click the 3-dot menu for the sink you created above and select *View sink
      details*
    * Copy Writer identity, e.g.
      `serviceAccount:pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
    * IAM & Admin >> IAM >> ADD
    * For the member, input `pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
      (part of the Writer identity you copied in a previous step)
    * Assign the Bigquery Data Editor role and press Save

1. Create the [bigquery_audit_logs_v2.sql](bigquery_audit_logs_v2.sql) view

   ```shell
   bash create_view.sh bigquery_audit_logs_v2.sql
   ```

1. Congratulations! From here, you can do further analysis in BigQuery by saving
   and querying the view, or you can connect it to a BI tool such as DataStudio
   as a data source and build dashboards.

### Usage Examples for [bigquery_audit_logs_v2.sql](bigquery_audit_logs_v2.sql)

In the following examples, change all occurrences of `project_id.dataset_id` to
your own values.

#### Retrieve 100 Latest SELECT SQL Queries Executed

```sql
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.bigquery_audit_logs_v2`
  WHERE
    hasJobChangeEvent
    AND jobChange.jobConfig.queryConfig.statementType = 'SELECT'
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
```

#### Retrieve 100 Latest DML SQL Queries Executed

```sql
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.bigquery_audit_logs_v2`
  WHERE
    hasJobChangeEvent
    AND jobChange.jobConfig.queryConfig.statementType IN (
          'INSERT', 'DELETE', 'UPDATE', 'MERGE')
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
```

#### Retrieve 100 Latest BigQuery Load Jobs

```sql
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.bigquery_audit_logs_v2`
  WHERE
    hasJobChangeEvent AND
    jobChange.jobConfig.loadConfig.destinationTable IS NOT NULL
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
```

#### Retrieve 100 Latest BigQuery Table Deletion Events

```sql
  SELECT * EXCEPT(
    modelDeletion,
    modelCreation,
    modelMetadataChange,
    routineDeletion,
    routineCreation,
    routineChange)
  FROM `project_id.dataset_id.bigquery_audit_logs_v2`
  WHERE
    hasTableDeletionEvent
  ORDER BY jobChange.jobStats.startTime DESC
  LIMIT 100
```

### Getting Started with bigquery_script_logs_v2.sql

A common pattern in data warehousing for tracking results of DML statements is
to collect system variable values after each DML statement and write them to a
separate logging table. With BigQuery, you no longer have to log your SQL
statement results because Cloud Logging allows you to store, search, analyze,
monitor, and set alerts on all your BigQuery scripting activity.
[bigquery_script_logs_v2.sql](/views/audit/bigquery_script_logs_v2.sql)
is a BigQuery view that handles extracting and formatting BigQueryMetaData
events so that you can focus on writing simple queries on top of this view to
monitor your BigQuery script jobs.

> Note: This view is just a subset of the SQL
> that's present in the [bigquery_audit_logs_v2.sql](bigquery_audit_logs_v2.sql) view.
> It exists as a tactical solution to target only logs related to scripting in BigQuery.

#### Prerequisites

1. Set your environment variables, replace \<your-project\> and \<your-dataset\>
   with the appropriate information

   ```shell
   export PROJECT_ID=<your-project-id>
   export DATASET_ID=<your-dataset-id>
   ```

1. Create a BigQuery dataset to store the audit logs, the logs tables will be
   created and filled with data once you run a BigQuery job post log sink
   creation.

   ```shell
   bq --project_id ${PROJECT_ID} mk ${DATASET_ID}
   ```
1. Define a log sink using any of the following methods:

   > Note: You can create a log sink at the folder, billing account, or
   > organization level using an
   > [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).

    * [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)

      ```shell
      gcloud logging sinks create bq-audit-logs-v2-sink \
      bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/${DATASET_ID} \
      --log-filter='protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND ((protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType="SCRIPT" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR (protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE") OR protoPayload.metadata.tableDataChange.reason="QUERY" OR protoPayload.metadata.tableCreation.reason="QUERY" OR protoPayload.metadata.tableChangeEvent.reason="QUERY" OR protoPayload.metadata.tableDeletion.reason="QUERY" OR protoPayload.metadata.tableDataRead.reason="QUERY")' \
      --use-partitioned-tables
      ```

    * [Cloud Console Logs Viewer](https://cloud.google.com/logging/docs/export/configure_export_v2#dest-create)
      Use this advanced filter:
      ```
      protoPayload.metadata.@type="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" 
      AND ( 
        (
          protoPayload.metadata.jobChange.job.jobConfig.queryConfig.statementType="SCRIPT" 
          AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
        ) 
        OR ( 
          protoPayload.metadata.jobChange.job.jobStats.parentJobName!="" 
          AND protoPayload.metadata.jobChange.job.jobStatus.jobState="DONE"
        ) 
        OR protoPayload.metadata.tableDataChange.reason="QUERY"
        OR protoPayload.metadata.tableCreation.reason="QUERY"
        OR protoPayload.metadata.tableChangeEvent.reason="QUERY"
        OR protoPayload.metadata.tableDeletion.reason="QUERY" 
        OR protoPayload.metadata.tableDataRead.reason="QUERY"  
      )
      ```

      > Important: Make sure to select
      > [partitioning](https://cloud.google.com/logging/docs/export/bigquery#partition-tables)
      > for your BigQuery destination.

1. Assign the log sink's service account the BigQuery Data Editor role

    * In the console, navigate to Logging >> Logs Router
    * Click the 3-dot menu for the sink you created above and select *View sink
      details*
    * Copy Writer identity, e.g.
      `serviceAccount:pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
    * IAM & Admin >> IAM >> ADD
    * For the member, input `pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
      (part of the Writer identity you copied in a previous step)
    * Assign the Bigquery Data Editor role and press Save

1. Create
   the [bigquery_script_logs_v2.sql](/views/audit/bigquery_script_logs_v2.sql)
   view

   ```shell
   bash create_view.sh bigquery_script_logs_v2.sql
   ```

1. Congratulations! From here, you can do further analysis in BigQuery by saving
   and querying the view, or you can connect it to a BI tool such as DataStudio
   as a data source and build dashboards.

### Usage Examples for [bigquery_script_logs_v2.sql](/views/audit/bigquery_script_logs_v2.sql)

In the following examples, change all occurrences of `project_id.dataset_id` to
your own values.

* Run the following query to see the 100 most recent BigQuery scripting
  statements. The results are ordered with the most recent script statement
  first, and then further ordering is applied using the script's job id and
  statement start time.

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

* Run the following query to see the 100 most recent BigQuery scripting
  statements that modify table data. The results are ordered with the most
  recent script statement first, and then further ordering is applied using the
  statement's job id and statement start time.

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

* Run the following query to see the 100 most recent BigQuery scripting
  statements which use slot reservations. The results are ordered with the most
  recent script statement first, and then further ordering is applied using the
  script's job id and statement start time.

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

## AuditData (Logs v1)

### Getting Started

1. Set your environment variables, replace \<your-project\> and \<your-dataset\>
   with the appropriate information

   ```shell
   export PROJECT_ID=<your-project-id> export DATASET_ID=<your-dataset-id>
   ```

1. Create a BigQuery dataset to store the v1 audit logs, the logs tables will be
   created and filled with data once you run a BigQuery job post log sink
   creation.

   ```shell
   bq --project_id ${PROJECT_ID} mk ${DATASET_ID}
   ```

1. Define a log sink:

   > Note: You can create a log sink at the folder, billing account, or
   > organization level using an
   > [aggregated sink](https://cloud.google.com/logging/docs/export/aggregated_sinks#creating_an_aggregated_sink).

    * [gcloud command](https://cloud.google.com/bigquery/docs/reference/auditlogs#defining_a_bigquery_log_sink_using_gcloud)

      ```shell
      gcloud logging sinks create bq-audit-logs-v1-sink \
      bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/${DATASET_ID} \
      --log-filter='resource.type="bigquery_resource"'
      ```

1. Assign the log sink's service account the BigQuery Data Editor role
    * In the console, navigate to Logging >> Logs Router
    * Click the 3-dot menu for the sink you created above and select *View sink
      details*
    * Copy Writer identity, e.g.
      `serviceAccount:pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
    * IAM & Admin >> IAM >> ADD
    * For the member, input `pXXXXXX@gcp-sa-logging.iam.gserviceaccount.com`
      (part of the Writer identity you copied in a previous step)
    * Assign the Bigquery Data Editor role and press Save
1. Create the [bigquery_audit_logs_v1.sql](bigquery_audit_logs_v1.sql) view

   ```shell
   bash create_view.sh bigquery_audit_logs_v1.sql
   ```

1. Congratulations! From here, you can do further analysis in BigQuery by saving
   and querying the view, or you can connect it to a BI tool such as DataStudio
   as a data source and build dashboards.

