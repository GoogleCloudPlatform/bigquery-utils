*
 * Script: BQ Audit Version 2
 * Author: NamrataShah5, danieldeleo
 * Description:
 * This SQL script parses ETL job events from
 * the newer BigQueryAuditMetadata Stackdriver logs.
 * Reference for BigQueryAuditMetadata: https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata
 */

WITH jobChangeEvent AS (
  SELECT
    protopayload_auditlog.authenticationInfo.principalEmail,
    resource.labels.project_id AS projectId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.requestMetadata.callerIp') AS callerIp,
    protopayload_auditlog.serviceName,
    protopayload_auditlog.methodName,
    COALESCE(
      CONCAT(
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(1)],
        ":",
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),"/")[SAFE_OFFSET(3)]
      ),
      CONCAT(
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName'),"/")[SAFE_OFFSET(1)],
        ":",
        SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName'),"/")[SAFE_OFFSET(3)]
      )
    ) AS jobId,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.job.jobName'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobInsertion.job.jobName')) AS jobChangeJobName,
    /*
     * JobStatus: Running state of a job
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstatus
     */
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.jobState'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.jobState')
    ) AS jobStatusJobState,
    SPLIT(COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult'),JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errorResult.code')),"/")[SAFE_OFFSET(1)],
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult.code'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errorResult.code')
    ) AS jobStatusErrorResultCode,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult.message'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errorResult.message')
    ) AS jobStatusErrorResultMessage,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult.details'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errorResult.details')
    ) AS jobStatusErrorResultDetails,
    REGEXP_EXTRACT_ALL(COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errors'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errors')
      ),r'"message":\"(.*?)\"}'
    ) AS jobStatusEncounteredErrorMessages,
    REGEXP_EXTRACT_ALL(COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errors'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStatus.errors')
      ),r'"code":\"(.*?)\"}'
    ) AS jobStatusEncounteredErrorCodes,
    /*
     * JobStats: Job statistics that may change after job starts.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#jobstats
     */
    JSON_EXTRACT_SCALAR(
      protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.parentJobName') AS jobStatsParentJobName,
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(
       protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.createTime')),
      TIMESTAMP(JSON_EXTRACT_SCALAR(
       protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.createTime'))
    ) AS jobStatsCreateTime,
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(
       protopayload_auditlog.metadataJson,'$.jobInsertion.job.jobStats.startTime')),
      TIMESTAMP(JSON_EXTRACT_SCALAR(
       protopayload_auditlog.metadataJson,'$.jobChange.job.jobStats.startTime'))
    ) AS jobStatsStartTime,
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.endTime')),
      TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.endTime'))
    ) AS jobStatsEndTime,
    COALESCE(
      CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.totalSlotMs') AS INT64),
      CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.totalSlotMs') AS INT64)
    ) AS jobStatsTotalSlotMs,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.reservationUsage.name'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.reservationUsage.name')
    ) AS jobStatsReservationUsageName,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.reservationUsage.slotMs'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.reservationUsage.slotMs')
    ) AS jobStatsReservationUsageSlotMs,
    /*
     * Query: Query job statistics
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query_1
     */
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.queryStats.totalProcessedBytes'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.totalProcessedBytes')
    ) AS queryJobStatsTotalProcessedBytes,
    COALESCE(
      CAST(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.totalBilledBytes') AS INT64),
       CAST(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.totalBilledBytes') AS INT64)
    ) AS queryJobStatsTotalBilledBytes,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.queryStats.billingTier'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.billingTier')
    ) AS queryJobStatsBillingTier,
    SPLIT(TRIM(TRIM(
      COALESCE(
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedTables'),
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedTables')
      ), '["'), '"]'), '","') AS queryJobStatsReferencedTables,
    SPLIT(TRIM(TRIM(
      COALESCE(
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedViews'),
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedViews')
      ), '["'), '"]'), '","') AS queryJobStatsReferencedViews,
    SPLIT(TRIM(TRIM(
      COALESCE(
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.queryStats.referencedRoutines'),
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedRoutines')
      ), '["'), '"]'), '","') AS queryJobStatsReferencedRoutines,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.queryStats.outputRowCount'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.outputRowCount')
    ) AS queryJobStatsOutputRowCount,
    CAST(COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStats.queryStats.cacheHit'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.cacheHit')
    ) AS BOOL) AS queryJobStatsCacheHit,
    /*
     * Load: Load job statistics
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load_1
     */
    CAST(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.loadStats.totalOutputBytes') AS INT64 ) AS loadJobStatsTotalOutputBytes,
    /*
     * JobStats convenience custom fields
     */
    COALESCE(
      TIMESTAMP_DIFF(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.jobInsertion.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.jobInsertion.job.jobStats.startTime')),
        MILLISECOND),
      TIMESTAMP_DIFF(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.jobChange.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.jobChange.job.jobStats.startTime')),
        MILLISECOND)
      ) AS jobStatsRuntimeMs,
    COALESCE(
      TIMESTAMP_DIFF(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobStats.startTime')),
        SECOND),
      TIMESTAMP_DIFF(
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.startTime')),
        SECOND)
      ) AS jobStatsRuntimeSecs,
    COALESCE(
      CAST(CEILING(
        TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.endTime')),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.startTime')),
          SECOND) / 60 ) AS INT64),
      CAST(CEILING(
        TIMESTAMP_DIFF(
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.endTime')),
          TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.startTime')),
          SECOND) / 60) AS INT64)
      ) AS jobStatsExecutionMinuteBuckets,
    /*
     * Describes a query job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#query
     */
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.query'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.query')) AS queryConfigQuery,
    CAST(COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.queryTruncated'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.queryTruncated')) AS BOOL) AS queryConfigQueryTruncated,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.destinationTable'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTable')) AS queryConfigDestinationTable,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.createDisposition'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.createDisposition')) AS queryConfigCreateDisposition,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.writeDisposition'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.writeDisposition')) AS queryConfigWriteDisposition,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.defaultDataset'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.defaultDataset')) AS queryConfigDefaultDataset,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.priority'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.priority')) AS queryConfigPriority,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.destinationTableEncryption.kmsKeyName'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTableEncryption.kmsKeyName')
      ) AS queryConfigDestinationTableEncryptionKmsKeyName,
    COALESCE(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.statementType'),
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.statementType')) AS queryConfigStatementType,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
      "/")[SAFE_OFFSET(1)] AS queryConfigDestinationTableProjectId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
      "/")[SAFE_OFFSET(3)] AS queryConfigDestinationTableDatasetId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.queryConfig.destinationTable'),
      "/")[SAFE_OFFSET(5)] AS queryConfigDestinationTableId,
    /*
     * Describes a load job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#load
     */
    SPLIT(TRIM(TRIM(
      JSON_EXTRACT(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.sourceUris'),
      '["'), '"]'), '","') AS loadConfigSourceUris,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.loadConfig.sourceUrisTruncated') AS BOOL) AS loadConfigSourceUrisTruncated,
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
      '$.jobChange.job.jobConfig.loadConfig.destinationTableEncryption.kmsKeyName'
      ) AS loadConfigDestinationTableEncryptionKmsKeyName,
    /*
     * Describes an extract job.
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#extract
     */
    SPLIT(TRIM(TRIM(
      JSON_EXTRACT(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.extractConfig.destinationUris'),
      '["'), '"]'), '","') AS extractConfigDestinationUris,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.destinationUrisTruncated')
       AS BOOL) AS extractConfigDestinationUrisTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.jobChange.job.jobConfig.extractConfig.sourceTable') AS extractConfigSourceTable,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(1)] AS extractConfigSourceTableProjectId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(3)] AS extractConfigSourceTableDatasetId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.extractConfig.sourceTable'),
      ",")[SAFE_OFFSET(5)] AS extractConfigSourceTableId,
    /*
     * Describes a copy job, which copies an existing table to another table
     * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecopy
     */
    SPLIT(TRIM(TRIM(
      JSON_EXTRACT(protopayload_auditlog.metadataJson,
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
      '$.jobChange.job.jobConfig.tableCopyConfig.destinationTableEncryption.kmsKeyName'
    ) AS tableCopyConfigDestinationTableEncryptionKmsKeyName,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(1)] AS tableCopyConfigProjectId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(2)] AS tableCopyConfigDatasetId,
    SPLIT(
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobConfig.tableCopyConfig.destinationTable'),
      ".")[SAFE_OFFSET(3)] AS tableCopyConfigTableId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.before') AS jobChangeBefore,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.jobChange.after') AS jobChangeAfter,
    REGEXP_EXTRACT(protopayload_auditlog.metadataJson, r'BigQueryAuditMetadata","(.*?)":') AS eventName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * TableCreation: Table creation event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecreation
 */
tableCreationEvent AS (
  SELECT
    CONCAT(
     SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableCreation.jobName'),"/")[SAFE_OFFSET(1)],
       ":",
     SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableCreation.jobName'),"/")[SAFE_OFFSET(3)]
    ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
    '$.tableCreation.jobName') AS tableCreationJobName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.tableName') AS tableCreationTableName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.tableInfo.friendlyName') AS tableCreationTableFriendlyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.tableInfo.description') AS tableCreationTableDescription,
    JSON_EXTRACT(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.tableInfo.labels') AS tableCreationTableLabels,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.schemaJson') AS tableCreationTableSchemaJson,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableCreation.table.schemaJsonTruncated') AS BOOL) AS tableCreationTableSchemaJsonTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.view.query') AS tableCreationViewDefinitionQuery,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.view.queryTruncated') AS BOOL) AS tableCreationViewDefinitionTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.expireTime') AS tableCreationTableExpireTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.createTime') AS tableCreationTableCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCrhange.table.updateTime') AS tableCreationTableUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.truncateTime') AS tableCreationTableTruncateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.table.encryption.kmsKeyName') AS tableCreationTableKmsKeyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCreation.reason')  AS tableCreationReason,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access` ),
/*
 * TableChange: Table metadata change event
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#TableChange
 */
tableChangeEvent AS (
  SELECT
    CONCAT(
     SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableChange.jobName'),"/")[SAFE_OFFSET(1)],
       ":",
     SPLIT(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson, '$.tableChange.jobName'),"/")[SAFE_OFFSET(3)]
    ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
    '$.tableChange.jobName') AS tableChangeJobName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableChange.table.tableName') AS tableChangeTableName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableChange.table.tableInfo.friendlyName') AS tableChangeTableFriendlyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableChange.table.tableInfo.description') AS tableChangeTableDescription,
    JSON_EXTRACT(protopayload_auditlog.metadataJson,
     '$.tableChange.table.tableInfo.labels') AS tableChangeTableLabels,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableChange.table.schemaJson') AS tableChangeTableSchemaJson,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
     '$.tableChange.table.schemaJsonTruncated') AS BOOL) AS tableChangeTableSchemaJsonTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.view.query') AS tableChangeViewDefinitionQuery,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.view.queryTruncated') AS BOOL) AS tableChangeViewDefinitionTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.expireTime') AS tableChangeTableExpireTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.createTime') AS tableChangeTableCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableCrhange.table.updateTime') AS tableChangeTableUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.truncateTime') AS tableChangeTableTruncateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.table.encryption.kmsKeyName') AS tableChangeTableKmsKeyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.reason')  AS tableChangeReason,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableChange.truncated') AS BOOL) AS tableChangeTruncated
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access` ),
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
      '$.tableDeletion.table.reason') AS tableDeletionReason,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`),
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
    SPLIT(TRIM(TRIM(
      COALESCE(
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.fields'),
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.fields')),
        '["'),'"]'),'","') AS tableDataReadFields,
     CAST(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.fieldsTruncated') AS BOOL) AS tableDataReadFieldsTruncated,
     SPLIT(TRIM(TRIM(
        JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.categories'),
        '["'),'"]'),'","') AS tableDataReadCategories,
     CAST(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.categoriesTruncated') AS BOOL) AS tableDataReadCategoriesTruncated,
     JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.reason') AS tableDataReadReason,
     JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.tableDataRead.jobName') AS tableDataReadJobName,
     JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.tableDataRead.sessionName') AS tableDataReadSessionName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
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
      '$.datasetCreation.dataset.insertedRowsCount') AS tableDataChangeInsertedRowsCount,
    CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.truncated') AS BOOL) AS tableDataChangeTruncated,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.reason') AS tableDataChangeReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.tableDataChange.jobName') AS tableDataChangeJobName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * ModelDeletion: Model deletion event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldeletion
 */
modelDeletionEvent AS (
  SELECT
    JSON_EXTRACT(protopayload_auditlog.metadataJson,
         '$.modelDeletion.reason') AS modelDeletionReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelDeletion.jobName') AS modelDeletionJobName,
    CONCAT(
     SPLIT(
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
           '$.modelDeletion.jobName'),
           "/")[SAFE_OFFSET(1)],
     ":",
     SPLIT(
       JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
         '$.modelDeletion.jobName'),
           "/")[SAFE_OFFSET(3)]
   ) AS jobId,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * ModelCreation: Model creation event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelcreation
 */
modelCreationEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.reason')  AS modelCreationReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.jobName') AS modelCreationJobName,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.modelCreation.jobName'),
           "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.modelCreation.jobName'),
         "/")[SAFE_OFFSET(3)]
    ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.modelName')  AS modelCreationModelName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.modelInfo.entityInfo.friendlyName')  AS modelCreationEntityInfoFriendlyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.modelInfo.entityInfo.description')  AS modelCreationEntityInfoDescription,
    JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.modelInfo.entityInfo.labels')  AS modelCreationEntityInfoLabels,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.expireTime')  AS modelCreationModelExpireTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.createTime')  AS modelCreationModelCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.updateTime')  AS modelCreationModelUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelCreation.model.encryption.kmsKeyName')  AS modelCreationEncryptionKmsKeyName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * ModelMetadataChange: Model metadata change event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelmetadatachange
 */
modelMetadataChangeEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.reason') AS modelMetadataChangeReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.jobName') AS modelMetadataChangeJobName,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.modelMetadataChange.jobName'),
           "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.modelMetadataChange.jobName'),
         "/")[SAFE_OFFSET(3)]
    ) AS jobId,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.modelName') AS modelMetadataChangeModelName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.modelInfo.entityInfo.friendlyName') AS modelMetadataChangeEntityInfoFriendlyName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.modelInfo.entityInfo.description') AS modelMetadataChangeEntityInfoDescription,
    JSON_EXTRACT(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.modelInfo.entityInfo.labels') AS modelMetadataChangeEntityInfoLabels,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.expireTime') AS modelMetadataChangeModelExpireTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.createTime') AS modelMetadataChangeModelCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.updateTime') AS modelMetadataChangeModelUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.modelMetadataChange.model.encryption.kmsKeyName') AS modelMetadataChangeEncryptionKmsKeyName,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * ModelDataChange: Model data change event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldatachange
 */
modelDataChangeEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.modelDataChange.reason')
    AS modelDataChangeReason,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.modelDataChange.jobName'),
            "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.modelDataChange.jobName'),
            "/")[SAFE_OFFSET(3)]
    ) AS jobId,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * RoutineCreation: Routine creation event.
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinecreation
 */
routineCreationEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineCreation.routine.routineName') AS routineCreationName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineCreation.routine.createTime') AS routineCreationCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineCreation.routine.updateTime') AS routineCreationUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineCreation.reason') AS routineCreationReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineCreation.jobName') AS routineCreationJobName,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.routineCreation.jobName'),
            "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.routineCreation.jobName'),
            "/")[SAFE_OFFSET(3)]
    ) AS jobId,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * RoutineChange: Routine change event
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinechange
 */
routineChangeEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineChange.routine.routineName') AS routineChangeName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineChange.routine.createTime') AS routineChangeCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineChange.routine.updateTime') AS routineChangeUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineChange.reason') AS routineChangeReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineChange.jobName') AS routineChangeJobName,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.routineChange.jobName'),
            "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.routineChange.jobName'),
            "/")[SAFE_OFFSET(3)]
    ) AS jobId,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
),
/*
 * RoutineDeletion: Routine deletion event
 * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinedeletion
 */
routineDeletionEvent AS (
  SELECT
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineDeletion.routine.routineName') AS routineDeletionName,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineDeletion.routine.createTime') AS routineDeletionCreateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineDeletion.routine.updateTime') AS routineDeletionUpdateTime,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineDeletion.reason') AS routineDeletionReason,
    JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
      '$.routineDeletion.jobName') AS routineDeletionJobName,
    CONCAT(
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.routineDeletion.jobName'),
            "/")[SAFE_OFFSET(1)],
      ":",
      SPLIT(
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.routineDeletion.jobName'),
            "/")[SAFE_OFFSET(3)]
    ) AS jobId,
  FROM `project_id.dataset_id.cloudaudit_googleapis_com_data_access`
) -- End of WITH clauses
SELECT
  SHA1(principalEmail) AS principalEmail,
  callerIp,
  serviceName,
  methodName,
  eventName,
  projectId,
  jobId,
  IF(jobChangeJobName IS NULL, False, True) AS hasJobChangeEvent,
  IF(tableChangeJobName IS NULL, False, True) AS hasTableChangeEvent,
  IF(tableCreationJobName IS NULL, False, True) AS hasTableCreationEvent,
  IF(tableDeletionJobName IS NULL, False, True) AS hasTableDeletionEvent,
  IF(tableDataReadJobName IS NULL, False, True) AS hasTableDataReadEvent,
  IF(tableDataChangeJobName IS NULL, False, True) AS hasTableDataChangeEvent,
  IF(modelDeletionJobName IS NULL, False, True) AS hasModelDeletionEvent,
  IF(modelCreationJobName IS NULL, False, True) AS hasModelCreationEvent,
  IF(modelMetadataChangeJobName IS NULL, False, True) AS hasModelMetadataChangeEvent,
  IF(routineCreationJobName IS NULL, False, True) AS hasRoutineCreationEvent,
  IF(routineChangeJobName IS NULL, False, True) AS hasRoutineChangeEvent,
  IF(routineDeletionJobName IS NULL, False, True) AS hasRoutineDeletionEvent,
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
        --TODO Add tableDefinitions
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
   * tableChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#TableChange
   */
  STRUCT(
    STRUCT(
      tableChangeTableName,
      STRUCT(
        tableChangeTableFriendlyName AS friendlyName,
        tableChangeTableDescription AS description,
        tableChangeTableLabels AS labels
      ) AS tableInfo,
      tableChangeTableSchemaJson AS schemaJson,
      tableChangeTableSchemaJsonTruncated AS schemaJsonTruncated,
      STRUCT(
        tableChangeViewDefinitionQuery AS query,
        tableChangeViewDefinitionTruncated AS queryTruncated
      ) AS view,
      tableChangeTableExpireTime AS expireTime,
      tableChangeTableCreateTime AS createTime,
      tableChangeTableUpdateTime AS updateTime,
      tableChangeTableTruncateTime AS truncateTime,
      STRUCT(
        tableChangeTableKmsKeyName AS kmsKeyName
      ) AS encryption
    ) AS table,
    tableChangeReason AS reason,
    tableChangeJobName AS jobName
  ) AS tableChange,
  /*
   * tableCreation STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tablecreation
   */
  STRUCT(
    STRUCT(
      tableCreationTableName,
      STRUCT(
        tableCreationTableFriendlyName AS friendlyName,
        tableCreationTableDescription AS description,
        tableCreationTableLabels AS labels
      ) AS tableInfo,
      tableCreationTableSchemaJson AS schemaJson,
      tableCreationTableSchemaJsonTruncated AS schemaJsonTruncated,
      STRUCT(
        tableCreationViewDefinitionQuery AS query,
        tableCreationViewDefinitionTruncated AS queryTruncated
      ) AS view,
      tableCreationTableExpireTime AS expireTime,
      tableCreationTableCreateTime AS createTime,
      tableCreationTableUpdateTime AS updateTime,
      tableCreationTableTruncateTime AS truncateTime,
      STRUCT(
        tableCreationTableKmsKeyName AS kmsKeyName
      ) AS encryption
    ) AS table,
    tableCreationReason AS reason,
    tableCreationJobName AS jobName
  ) AS tableCreation,
  /*
   * tableDeletion STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#tabledeletion
   */
  STRUCT(
    tableDeletionReason AS reason,
    tableDeletionJobName AS jobName
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
   /*
   * ModelDeletion STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modeldeletion
   */
  STRUCT(
    modelDeletionReason AS reason,
    modelDeletionJobName AS jobName
  ) AS modelDeletion,
  /*
   * ModelCreation STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelcreation
   */
  STRUCT(
    STRUCT(
      modelCreationModelName AS modelName,
      STRUCT(
        modelCreationEntityInfoFriendlyName AS friendlyName,
        modelCreationEntityInfoDescription AS description,
        modelCreationEntityInfoLabels AS labels
      ) AS entityInfo,
      modelCreationModelExpireTime AS expireTime,
      modelCreationModelCreateTime AS createTime,
      modelCreationModelUpdateTime AS updateTime,
      STRUCT(modelCreationEncryptionKmsKeyName AS kmsKeyName) AS encryptionInfo
    ) AS model,
    modelCreationReason AS reason,
    modelCreationJobName AS jobName
  ) AS modelCreation,
  /*
   * ModelMetadataChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#modelmetadatachange
   */
  STRUCT(
    STRUCT(
      modelMetadataChangeModelName AS modelName,
      STRUCT(
        modelMetadataChangeEntityInfoFriendlyName AS friendlyName,
        modelMetadataChangeEntityInfoDescription AS description,
        modelMetadataChangeEntityInfoLabels AS labels
      ) AS entityInfo,
      modelMetadataChangeModelExpireTime AS expireTime,
      modelMetadataChangeModelCreateTime AS createTime,
      modelMetadataChangeModelUpdateTime AS updateTime,
      STRUCT(modelMetadataChangeEncryptionKmsKeyName AS kmsKeyName) AS encryptionInfo
    ) AS model,
    modelMetadataChangeReason AS reason,
    modelMetadataChangeJobName AS jobName
  ) AS modelMetadataChange,
  /*
   * RoutineCreation STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinecreation
   */
  STRUCT(
    STRUCT(
      routineCreationName AS name,
      routineCreationCreateTime AS createTime,
      routineCreationUpdateTime AS updateTime
    ) AS routine,
    routineCreationReason AS reason,
    routineCreationJobName AS jobName
  ) AS routineCreation,
  /*
   * RoutineChange STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinechange
   */
  STRUCT(
    STRUCT(
      routineChangeName AS name,
      routineChangeCreateTime AS createTime,
      routineChangeUpdateTime AS updateTime
    ) AS routine,
    routineChangeReason AS reason,
    routineChangeJobName AS jobName
  ) AS routineChange,
  /*
   * RoutineDeletion STRUCT
   * https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/BigQueryAuditMetadata#routinedeletion
   */
  STRUCT(
    STRUCT(
      routineDeletionName AS name,
      routineDeletionCreateTime AS createTime,
      routineDeletionUpdateTime AS updateTime
    ) AS routine,
    routineDeletionReason AS reason,
    routineDeletionJobName AS jobName
  ) AS routineDeletion

FROM jobChangeEvent
LEFT JOIN tableDataChangeEvent USING(jobId)
LEFT JOIN tableCreationEvent USING(jobId)
LEFT JOIN tableChangeEvent USING(jobId)
LEFT JOIN tableDeletionEvent USING(jobId)
LEFT JOIN tableDataReadEvent USING(jobId)
LEFT JOIN modelDeletionEvent USING(jobId)
LEFT JOIN modelMetadataChangeEvent USING(jobId)
LEFT JOIN modelCreationEvent USING(jobId)
LEFT JOIN modelDataChangeEvent USING(jobId)
LEFT JOIN routineCreationEvent USING(jobId)
LEFT JOIN routineChangeEvent USING(jobId)
LEFT JOIN routineDeletionEvent USING(jobId)
WHERE
  /*
   * Currently, BigQuery Scripting jobs do not emit jobChange events, they only produce a jobInsertion event.
   * It's therefore necessary to add the additional filter shown below to capture "SCRIPT" statementType queries.
   */
  queryConfigStatementType = "SCRIPT"
  OR jobChangeAfter = "DONE"
