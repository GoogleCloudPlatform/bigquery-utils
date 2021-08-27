CREATE OR REPLACE VIEW `project_id.dataset_id.bq_script_logs` AS (
WITH jobChangeEvent AS (
  SELECT
    protopayload_auditlog.authenticationInfo.principalEmail,
    resource.labels.project_id AS projectId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.requestMetadata.callerIp') AS callerIp,
    timestamp,
    protopayload_auditlog.serviceName,
    protopayload_auditlog.methodName,
    CONCAT(
      SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(3)]
    ) AS jobId,
    CONCAT(
      SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.parentJobName'),
        "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.parentJobName'),
        "/")[SAFE_OFFSET(3)]
    ) AS parentJobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName') AS jobChangeJobName,
    /*
     * JobStatus: Running state of a job
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstatus
     */
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStatus.jobState') AS jobStatusJobState,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStatus.errorResult'),"/")[SAFE_OFFSET(1)],
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStatus.errorResult.code') AS jobStatusErrorResultCode,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStatus.errorResult.message') AS jobStatusErrorResultMessage,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStatus.errorResult.details') AS jobStatusErrorResultDetails,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errors'),r'"message":\"(.*?)\"}'
    ) AS jobStatusEncounteredErrorMessages,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errors'),r'"code":\"(.*?)\"}'
    ) AS jobStatusEncounteredErrorCodes,
    /*
     * JobStats: Job statistics that may change after job starts.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstats
     */
    JSON_EXTRACT_SCALAR(
      protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.parentJobName') AS jobStatsParentJobName,
    TIMESTAMP(JSON_EXTRACT_SCALAR(
      protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.createTime')) AS jobStatsCreateTime,
    TIMESTAMP(JSON_EXTRACT_SCALAR(
      protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.startTime')) AS jobStatsStartTime,
    TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.endTime')) AS jobStatsEndTime,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.totalSlotMs') AS INT64) AS jobStatsTotalSlotMs,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobStats.reservationUsage'),
      r'"name":\"(.*?)\"}') AS jobStatsReservationUsageName,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.reservationUsage'),
      r'"slotMs":\"(.*?)\"}') AS jobStatsReservationUsageSlotMs,
    /*
     * Query: Query job statistics
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query_1
     */
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.totalProcessedBytes') AS queryJobStatsTotalProcessedBytes,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.totalBilledBytes') AS INT64) AS queryJobStatsTotalBilledBytes,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.billingTier') AS queryJobStatsBillingTier,
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.referencedTables'),
      '["'), '"]'), '","') AS queryJobStatsReferencedTables,
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
       '$.jobChange.job.jobStats.queryStats.referencedViews'),
       '["'), '"]'), '","') AS queryJobStatsReferencedViews,
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
       '$.jobChange.job.jobStats.queryStats.referencedRoutines'),
       '["'), '"]'), '","') AS queryJobStatsReferencedRoutines,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.outputRowCount') AS queryJobStatsOutputRowCount,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.queryStats.cacheHit') AS BOOL) AS queryJobStatsCacheHit,
    /*
     * Load: Load job statistics
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load_1
     */
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobStats.loadStats.totalOutputBytes') AS INT64 ) AS loadJobStatsTotalOutputBytes,
    /*
     * JobStats convenience custom fields
     */
    TIMESTAMP_DIFF(
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
       '$.jobChange.job.jobStats.endTime')),
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
       '$.jobChange.job.jobStats.startTime')),
      MILLISECOND) AS jobStatsRuntimeMs,
    TIMESTAMP_DIFF(
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.endTime')),
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.startTime')),
      SECOND) AS jobStatsRuntimeSecs,
    CAST(CEILING(
      TIMESTAMP_DIFF(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.startTime')),
        SECOND) / 60 ) AS INT64) AS jobStatsExecutionMinuteBuckets,
    /*
     * Describes a query job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query
     */
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.query') AS queryConfigQuery,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.queryTruncated') AS BOOL) AS queryConfigQueryTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.destinationTable') AS queryConfigDestinationTable,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.createDisposition') AS queryConfigCreateDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.writeDisposition') AS queryConfigWriteDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.defaultDataset') AS queryConfigDefaultDataset,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobConfig.queryConfig.tableDefinitions'),
      r'"name":\"(.*?)\"}') AS tableDefinitionsName,
    REGEXP_EXTRACT_ALL(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,'$.jobChange.job.jobConfig.queryConfig.tableDefinitions'),
      r'"sourceUris":\"(.*?)\"}') AS tableDefinitionsSourceUris,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.priority') AS queryConfigPriority,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTableEncryption.kmsKeyName') AS queryConfigDestinationTableEncryptionKmsKeyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.statementType') AS queryConfigStatementType,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
      "/")[SAFE_OFFSET(1)] AS queryConfigDestinationTableProjectId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
      "/")[SAFE_OFFSET(3)] AS queryConfigDestinationTableDatasetId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
       '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
       "/")[SAFE_OFFSET(5)] AS queryConfigDestinationTableId,
    /*
     * Describes a load job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load
     */
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.sourceUris'),
      '["'), '"]'), '","') AS loadConfigSourceUris,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.extractConfig.sourceUrisTruncated') AS BOOL) AS loadConfigSourceUrisTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.schemaJson') AS loadConfigSchemaJson,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.schemaJsonUrisTruncated') AS BOOL) AS loadConfigSchemaJsonTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.destinationTable') AS loadConfigDestinationTable,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.createDisposition') AS loadConfigCreateDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.writeDisposition') AS loadConfigWriteDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.destinationTableEncryption.kmsKeyName') AS loadConfigDestinationTableEncryptionKmsKeyName,
    /*
     * Describes an extract job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#extract
     */
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.destinationUris'),
      '["'), '"]'), '","') AS extractConfigDestinationUris,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.destinationUrisTruncated') AS BOOL) AS extractConfigDestinationUrisTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.sourceTable') AS extractConfigSourceTable,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(1)] AS extractConfigSourceTableProjectId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(3)] AS extractConfigSourceTableDatasetId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(5)] AS extractConfigSourceTableId,
    /*
     * Describes a copy job, which copies an existing table to another table
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecopy
     */
    SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.sourceTables'),
      '["'),'"]') ,'","') AS tableCopySourceTables,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.sourceTablesTruncated') 
      AS BOOL) AS tableCopyConfigSourceTablesTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable') AS tableCopyConfigDestinationTable,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.createDisposition') AS tableCopyConfigCreateDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.writeDisposition') AS tableCopyConfigWriteDisposition,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTableEncryption.kmsKeyName') AS tableCopyConfigDestinationTableEncryptionKmsKeyName,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(1)] AS tableCopyConfigProjectId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(2)]  AS tableCopyConfigDatasetId,
    SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(3)] AS tableCopyConfigTableId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.before') AS jobChangeBefore,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.after') AS jobChangeAfter,
    REGEXP_EXTRACT(protopayload_auditlog.metadataJson, r'BigQueryAuditMetadata","(.*?)":') AS eventName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * TableDeletion: Table deletion event
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledeletion
 */
tableDeletionEvent AS (
  SELECT
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDeletion.jobName'),
        "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDeletion.jobName'),
        "/")[SAFE_OFFSET(3)]
      ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDeletion.jobName') AS tableDeletionJobName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDeletion.reason') AS tableDeletionReason,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_activity`
),
/*
 * TableDataRead: Data from tableDataRead audit logs
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledataread
 */
tableDataReadEvent AS (
  SELECT
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.tableDataRead.jobName'),
            "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.tableDataRead.jobName'),
            "/")[SAFE_OFFSET(3)]
    ) AS jobId,
    ARRAY_AGG(
      protopayload_auditlog.resourceName
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadTableName,
    ARRAY_AGG(STRUCT(
      SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.tableDataRead.fields'),
      '["'),'"]'),'","')
      AS fields) IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadFields,
    ARRAY_AGG(
      CAST(JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.tableDataRead.fieldsTruncated') AS BOOL)
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadFieldsTruncated,
    ARRAY_AGG(STRUCT(
      SPLIT(TRIM(TRIM(JSON_EXTRACT(protopayload_auditlog.metadataJson,'$.tableDataRead.categories'),
      '["'),'"]'),'","')
      AS categories) IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadCategories,
    ARRAY_AGG(
      CAST(JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.tableDataRead.categoriesTruncated') AS BOOL)
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadCategoriesTruncated,
    ARRAY_AGG(
      JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.tableDataRead.reason')
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadReason,
    ARRAY_AGG(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataRead.jobName')
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadJobName,
    ARRAY_AGG(
      JSON_EXTRACT(protopayload_auditlog.metadataJson, '$.tableDataRead.sessionName')
      IGNORE NULLS ORDER BY protopayload_auditlog.resourceName) AS tableDataReadSessionName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
  GROUP BY jobId
),
/*
 * TableDataChange: Table data change event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledatachange
 */
tableDataChangeEvent AS (
  SELECT
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableDataChange.jobName'),
      "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.tableDataChange.jobName'),
        "/")[SAFE_OFFSET(3)]
    ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.deletedRowsCount') AS tableDataChangeDeletedRowsCount,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.insertedRowsCount') AS tableDataChangeInsertedRowsCount,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.truncated') AS BOOL) AS tableDataChangeTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.reason') AS tableDataChangeReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.jobName') AS tableDataChangeJobName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
)
 -- End of WITH clauses
SELECT
  principalEmail,
  callerIp,
  timestamp as eventTimestamp,
  serviceName,
  methodName,
  eventName,
  projectId,
  jobId,
  parentJobId,
  IF(jobChangeJobName IS NULL, False, True) AS hasJobChangeEvent,
  IF(tableDeletionJobName is NULL, False, True) AS hasTableDeletionEvent,
  IF(tableDataReadJobName IS NULL, False, True) AS hasTableDataReadEvent,
  IF(tableDataChangeJobName IS NULL, False, True) AS hasTableDataChangeEvent,
  STRUCT(
    EXTRACT(MINUTE FROM jobStatsStartTime) AS minuteOfDay,
    EXTRACT(HOUR FROM jobStatsStartTime) AS hourOfDay,
    EXTRACT(DAYOFWEEK FROM jobStatsStartTime) - 1 AS dayOfWeek,
    EXTRACT(DAYOFYEAR FROM jobStatsStartTime) AS dayOfYear,
    EXTRACT(WEEK FROM jobStatsStartTime) AS week,
    EXTRACT(MONTH FROM jobStatsStartTime) AS month,
    EXTRACT(QUARTER FROM jobStatsStartTime) AS quarter,
    EXTRACT(YEAR FROM jobStatsStartTime) AS year
  ) AS jobStartDate,
  jobStatsRuntimeMs AS jobRuntimeMs,
  jobStatsRuntimeSecs AS jobRuntimeSec,
  REGEXP_CONTAINS(jobId, 'beam') AS isBeamJob,
  REGEXP_CONTAINS(queryConfigQuery, 'cloudaudit_googleapis_com_data_access') AS isAuditDashboardQuery,
  jobStatsTotalSlotMs / jobStatsRuntimeMs AS avgSlots,
  /*
   * The following statement breaks down the query into minute buckets
   * and provides the average slot usage within that minute. This is a
   * crude way of making it so you can retrieve the average slot utilization
   * for a particular minute across multiple queries.
   */
  ARRAY(
    SELECT
      STRUCT(
        TIMESTAMP_TRUNC(
          TIMESTAMP_ADD(jobStatsStartTime, INTERVAL bucket_num MINUTE), MINUTE
        ) AS time,
        jobStatsTotalSlotMs / jobStatsRuntimeMs AS avgSlotUsage
      )
    FROM UNNEST(GENERATE_ARRAY(1, jobStatsExecutionMinuteBuckets)) AS bucket_num
  ) AS jobExecutionTimeline,
  ARRAY_LENGTH(queryJobStatsReferencedTables) AS totalTablesProcessed,
  ARRAY_LENGTH(queryJobStatsReferencedViews) AS totalViewsProcessed,
  (queryJobStatsTotalBilledBytes / pow(2,30)) AS totalBilledGigabytes,
  (queryJobStatsTotalBilledBytes / pow(2,40)) AS totalBilledTerabytes,
  (queryJobStatsTotalBilledBytes / pow(2,40)) * 5 AS estimatedCostUsd,
  CONCAT(
    queryConfigDestinationTableDatasetId, '.',
    queryConfigDestinationTableId) AS queryDestinationTableRelativePath,
  CONCAT(
    queryConfigDestinationTableProjectId, '.',
    queryConfigDestinationTableDatasetId, '.',
    queryConfigDestinationTableId) AS queryDestinationTableAbsolutePath,
  queryConfigDestinationTableProjectId AS queryDestinationTableProjectId,
  queryConfigDestinationTableDatasetId AS queryDestinationTableDatasetId,
  queryConfigDestinationTableId AS queryDestinationTableId,
  /*
   * jobChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobchange
  */
  STRUCT(
    jobChangeJobName AS jobName,
    /*
     * jobConfig STRUCT
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobconfig
     */
    STRUCT(
      /*
       * queryConfig STRUCT
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query
       */
      STRUCT(
        queryConfigQuery AS query,
        queryConfigQueryTruncated AS queryTruncated,
        queryConfigDestinationTable AS destinationTable,
        queryConfigCreateDisposition AS createDisposition,
        queryConfigWriteDisposition AS writeDisposition,
        queryConfigDefaultDataset AS defaultDataset,
        STRUCT(
        tableDefinitionsName AS name,
        tableDefinitionsSourceUris AS sourceUris
        ) AS tableDefinitions,
        queryConfigPriority AS priority,
        STRUCT(
          queryConfigDestinationTableEncryptionKmsKeyName AS kmsKeyName
        ) AS destinationTableEncryption,
        queryConfigStatementType AS statementType
      ) AS queryConfig,
      /*
       * loadConfig STRUCT
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load
       */
      STRUCT(
        loadConfigSourceUris AS sourceUris,
        loadConfigSourceUrisTruncated AS sourceUrisTruncated,
        loadConfigSchemaJson AS schemaJson,
        loadConfigSchemaJsonTruncated AS schemaJsonTruncated,
        loadConfigDestinationTable AS destinationTable,
        loadConfigCreateDisposition AS createDisposition,
        loadConfigWriteDisposition AS writeDisposition,
        STRUCT(
          loadConfigDestinationTableEncryptionKmsKeyName AS kmsKeyName
        ) AS destinationTableEncryption
      ) AS loadConfig,
      /*
       * extractConfig STRUCT
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#extract
       */
      STRUCT(
        extractConfigDestinationUris AS destinationUris,
        extractConfigDestinationUrisTruncated AS destinationUrisTruncated,
        extractConfigSourceTable AS sourceTable
      ) AS extractConfig,
      /*
       * tableCopyConfig STRUCT
       * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecopy
       */
      STRUCT(
        tableCopySourceTables AS sourceTables,
        tableCopyConfigSourceTablesTruncated AS configSourceTablesTruncated,
        tableCopyConfigDestinationTable AS configDestinationTable,
        tableCopyConfigCreateDisposition AS configCreateDisposition,
        tableCopyConfigWriteDisposition AS configWriteDisposition,
        STRUCT(
          tableCopyConfigDestinationTableEncryptionKmsKeyName AS kmsKeyName
        ) AS destinationTableEncryption
      ) AS tableCopyConfig
    ) AS jobConfig,
    /*
     * JobStatus STRUCT
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#JobStatus
     */
    STRUCT(
      jobStatusJobState AS jobState,
      STRUCT(
        jobStatusErrorResultCode AS code,
        jobStatusErrorResultMessage AS message,
        jobStatusErrorResultDetails AS details
      ) AS errorResult,
      jobStatusEncounteredErrorMessages AS encounteredErrorMessages,
      jobStatusEncounteredErrorCodes AS encounteredErrorCodes
    ) AS jobStatus,
    /*
     * JobStats STRUCT
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstats
     */
    STRUCT(
      jobStatsCreateTime AS createTime,
      jobStatsStartTime AS startTime,
      jobStatsEndTime AS endTime,
      jobStatsTotalSlotMs AS totalSlotMs,
      jobStatsParentJobName as parentJobName,
      STRUCT(
        jobStatsReservationUsageName AS name,
        jobStatsReservationUsageSlotMs AS slotMs
      ) AS reservationUsage,
      STRUCT(
        queryJobStatsTotalProcessedBytes AS totalProcessedBytes,
        queryJobStatsTotalBilledBytes AS totalBilledBytes,
        queryJobStatsBillingTier AS billingTier,
        queryJobStatsReferencedTables AS referencedTables,
        queryJobStatsReferencedViews AS referencedViews,
        queryJobStatsReferencedRoutines AS referencedRoutines,
        queryJobStatsOutputRowCount AS outputRowCount,
        queryJobStatsCacheHit AS cacheHit
      ) AS queryStats,
      STRUCT(
        loadJobStatsTotalOutputBytes AS totalOutputBytes
      ) AS loadStats
    ) AS jobStats
  ) AS jobChange,
  /*
   * Load job statistics
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load_1
  */
  loadJobStatsTotalOutputBytes AS totalLoadOutputBytes,
  (loadJobStatsTotalOutputBytes / pow(2,30)) AS totalLoadOutputGigabytes,
  (loadJobStatsTotalOutputBytes / pow(2,40)) AS totalLoadOutputTerabytes,
  /*
   * TableDeletion STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledeletion
   */
   STRUCT(
     tableDeletionJobName as jobName,
     tableDeletionReason as reason
   ) AS tableDeletion,
  /*
   * TableDataRead STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledataread
   */
  STRUCT(
    tableDataReadFields AS fields,
    tableDataReadFieldsTruncated AS fieldsTruncated,
    tableDataReadCategories AS categories,
    tableDataReadCategoriesTruncated AS categoriesTruncated,
    tableDataReadReason AS reason,
    tableDataReadJobName AS jobName,
    tableDataReadSessionName AS sessionName
  ) AS tableDataRead,
  /*
   * TableDataChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledatachange
   */
  STRUCT(
    tableDataChangeDeletedRowsCount AS deletedRowsCount,
    tableDataChangeInsertedRowsCount AS insertedRowsCount,
    tableDataChangeTruncated AS truncated,
    tableDataChangeReason AS reason,
    tableDataChangeJobName AS jobName
  ) AS tableDataChange,
FROM jobChangeEvent
LEFT JOIN tableDataChangeEvent USING(jobId)
LEFT JOIN tableDeletionEvent USING(jobId)
LEFT JOIN tableDataReadEvent USING(jobId)
WHERE
  jobChangeAfter = "DONE"
)
