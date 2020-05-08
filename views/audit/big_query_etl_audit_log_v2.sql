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
)
 -- End of WITH clauses
SELECT
  principalEmail,
  callerIp,
  serviceName,
  methodName,
  eventName,
  projectId,
  jobId,
  IF(jobChangeJobName IS NULL, False, True) AS hasJobChangeEvent,
  IF(tableDataReadJobName IS NULL, False, True) AS hasTableDataReadEvent,
  IF(tableDataChangeJobName IS NULL, False, True) AS hasTableDataChangeEvent,
  IF(modelMetadataChangeJobName IS NULL, False, True) AS hasModelMetadataChangeEvent,
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
LEFT JOIN tableDataReadEvent USING(jobId)
LEFT JOIN modelDataChangeEvent USING(jobId)
WHERE
  /*
   * Currently, BigQuery Scripting jobs do not emit jobChange events, they only produce a jobInsertion event.
   * It's therefore necessary to add the additional filter shown below to capture "SCRIPT" statementType queries.
   */
  queryConfigStatementType = "SCRIPT"
  OR jobChangeAfter = "DONE"
