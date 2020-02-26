/* Public BigQuery Audit View */
  WITH
    BQAudit2_job AS (
    SELECT
      protopayload_auditlog.authenticationInfo.principalEmail AS principalEmail,
      resource.labels.project_id,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.requestMetadata.callerIp') AS callerIp,
      protopayload_auditlog.serviceName AS serviceName,
      protopayload_auditlog.methodName AS methodName,
      resource.labels.project_id AS projectId,
      COALESCE( CONCAT(SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobName'),"/")[
        OFFSET
          (1)], ":", SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobName'),"/")[
        OFFSET
          (3)]),
        CONCAT(SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobName'),"/")[
        OFFSET
          (1)], ":", SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobName'),"/")[
        OFFSET
          (3)]) ) AS jobId,
      COALESCE(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.createTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.createTime')) ) AS createTime,
      COALESCE(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.startTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.startTime'))) AS startTime,
      COALESCE(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStats.endTime')),
        TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.endTime'))) AS endTime,
      COALESCE(TIMESTAMP_DIFF(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobStats.endTime')),TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobStats.startTime')),MILLISECOND),
        TIMESTAMP_DIFF(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobStats.endTime')),TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobStats.startTime')),MILLISECOND) )AS runtimeMs,
      /* This code extracts the column specific to the Copy operation in BQ */ /*Extract each field out of this struct*/ JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig') AS tableCopy,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.sourceTables') AS sourceTables,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.sourceTablesTruncated') AS sourceTablesTruncated,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.loadConfig.sourceUris') AS sourceUris,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.loadConfig.createDisposition') AS createDisposition,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.loadConfig.writeDisposition') AS writeDisposition,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.loadConfig.schemaJson') AS schemaJson,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTableEncryption.kmsKeyName') AS kmsKeyName,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable') AS destinationTable,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),".")[
    OFFSET
      (0)] AS project_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),".")[
    OFFSET
      (1)] AS dataset_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.tableCopyConfig.destinationTable'),".")[
    OFFSET
      (2)] AS table_id,
      /* This code extracts the column specific to the Extract operation in BQ */ JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.extract'),
      /* The following code extracts the columns specific to the Load operation in BQ */ CAST(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.loadStats.totalOutputBytes' ) AS INT64) AS totalLoadOutputBytes,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.loadConfig.load'),
      /* The following code extracts columns specific to Query operation in BQ */ COALESCE(TIMESTAMP_DIFF(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobStats.endTime')),TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobInsertion.job.jobStats.startTime')),SECOND),
        TIMESTAMP_DIFF(TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobStats.endTime')),TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
              '$.jobChange.job.jobStats.startTime')),SECOND)) AS runtimeSecs,
      COALESCE(CAST(CEILING((TIMESTAMP_DIFF( TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                    '$.jobInsertion.job.jobStats.endTime')), TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                    '$.jobInsertion.job.jobStats.startTime')), SECOND)) / 60) AS INT64),
        CAST(CEILING((TIMESTAMP_DIFF( TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                    '$.jobChange.job.jobStats.endTime')), TIMESTAMP(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
                    '$.jobChange.job.jobStats.startTime')), SECOND)) / 60) AS INT64)) AS executionMinuteBuckets,
    IF
      (COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.totalProcessedBytes' ),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.totalSlotMs'),
          JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
            '$.jobInsertion.job.jobStatus.errorResult.code' ) ) IS NULL,
        TRUE,
        FALSE ) AS isCached,
      CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.totalSlotMs') AS INT64 ) AS totalSlotMs,
      ARRAY_LENGTH(SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedTables' ),",")) AS totalTablesProcessed,
      ARRAY_LENGTH(SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.jobChange.job.jobStats.queryStats.referencedViews' ),",")) AS totalViewsProcessed,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.totalProcessedBytes' ) AS totalProcessedBytes,
      CAST(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.totalBilledBytes' ) AS INT64) AS totalBilledBytes,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.billingTier' ) AS billingTier,
      COALESCE( JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.query'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.query') ) AS query,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedTables'),"/")[
    OFFSET
      (0)] AS refTable_project_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedTables'),"/")[
    OFFSET
      (1)] AS refTable_dataset_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedTables'),"/")[
    OFFSET
      (2)] AS refTable_table_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedViews'),"/")[
    OFFSET
      (0)] AS refView_project_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedViews'),"/")[
    OFFSET
      (1)] AS refView_dataset_id,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobStats.queryStats.referencedViews'),"/")[
    OFFSET
      (2)] AS refView_table_id,
      JSON_EXTRACT(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.referencedViews') AS referencedViews,
      JSON_EXTRACT(protopayload_auditlog.metadataJson,
        '$.jobChange.job.jobStats.queryStats.referencedTables') AS referencedTables,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.destinationUris' ) AS destinationUris,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.extractConfig.sourceTable' ) AS sourceTable,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.extractConfig.destinationUris'),".")[
    OFFSET
      (0)] AS srctable_projectid,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.extractConfig.destinationUris'),".")[
    OFFSET
      (1)] AS srctable_datasetid,
      SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.extractConfig.destinationUris'),".")[
    OFFSET
      (2)] AS srctable_tableid,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobConfig.queryConfig.query.query' ) AS jobconfig_query,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult.code' ) AS errorCode,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.jobInsertion.job.jobStatus.errorResult.message' ) AS errorMessage,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        "$.jobChange.after") AS jobChangeAfter,
      COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.queryConfig.statementType'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.queryConfig.statementType') )AS statementType,
      REGEXP_EXTRACT(protopayload_auditlog.metadataJson, r'BigQueryAuditMetadata","(.*?)":') AS eventName,
      
      COALESCE(JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobInsertion.job.jobConfig.labels.querytype'),
        JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
          '$.jobChange.job.jobConfig.labels.querytype')) AS querytype
          
         
    FROM
      `project_id.dataset_id.table_id` ),
    BQAudit2_data AS(
    SELECT
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.tableDataChange.insertedRowsCount') AS insertRowCount,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.tableDataChange.deletedRowsCount') AS deleteRowCount,
      JSON_EXTRACT_SCALAR(protopayload_auditlog.metadataJson,
        '$.tableDataChange.reason') AS tableDataChangeReason,
      CONCAT(SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataChange.jobName'),"/")[
      OFFSET
        (1)], ":", SPLIT(JSON_EXTRACT(protopayload_auditlog.metadataJson,
            '$.tableDataChange.jobName'),"/")[
      OFFSET
        (3)]) AS data_jobid
    FROM
      `project_id.dataset_id.table_id`) /* This code queries BQAudit2 */
  SELECT
    principalEmail,
    callerIp,
    serviceName,
    methodName,
    eventName,
    tableDataChangeReason,
    projectId,
    jobId,
    querytype,
    insertRowCount,
    deleteRowCount,
    errorCode,
    errorMessage,
    statementType,
    STRUCT( EXTRACT(MINUTE
      FROM
        startTime) AS minuteOfDay,
      EXTRACT(HOUR
      FROM
        startTime) AS hourOfDay,
      EXTRACT(DAYOFWEEK
      FROM
        startTime) - 1 AS dayOfWeek,
      EXTRACT(DAYOFYEAR
      FROM
        startTime) AS dayOfYear,
      EXTRACT(WEEK
      FROM
        startTime) AS week,
      EXTRACT(MONTH
      FROM
        startTime) AS month,
      EXTRACT(QUARTER
      FROM
        startTime) AS quarter,
      EXTRACT(YEAR
      FROM
        startTime) AS year ) AS date,
    createTime,
    startTime,
    endTime,
    runtimeMs,
    runtimeSecs,
    tableCopy,
    /* This code queries data specific to the Copy operation */ CONCAT(dataset_id, '.', table_id) AS tableCopyDestinationTableRelativePath,
    CONCAT(projectId, '.', dataset_id, '.', table_id) AS tableCopyDestinationTableAbsolutePath,
  IF
    (eventName = "jobChange",
      1,
      0) AS numCopies,
    /* This code queries data specific to the Copy operation */ /* The following code queries data specific to the Load operation in BQ */ totalLoadOutputBytes,
    (totalLoadOutputBytes / 1000000000) AS totalLoadOutputGigabytes,
    (totalLoadOutputBytes / 1000000000) / 1000 AS totalLoadOutputTerabytes,
    STRUCT( sourceUris,
      STRUCT( projectId,
        dataset_id,
        table_id,
        CONCAT(dataset_id, '.', table_id) AS relativePath,
        CONCAT(projectId, '.', dataset_id, '.', table_id) AS absolutePath ) AS destinationTable,
      createDisposition,
      writeDisposition,
      schemaJson ) AS load,
  IF
    (eventName = "jobChange",
      1,
      0) AS numLoads,
    /* This ends the code snippet that queries columns specific to the Load operation in BQ */ /* The following code queries data specific to the Extract operation in BQ */ REGEXP_CONTAINS(jobId, 'beam') AS isBeamJob,
    STRUCT( destinationUris,
      STRUCT( srctable_projectid,
        srctable_datasetid,
        srctable_tableid,
        CONCAT(srctable_datasetid, '.', srctable_tableid) AS relativeTableRef,
        CONCAT(srctable_projectid, '.', srctable_datasetid, '.', srctable_tableid) AS absoluteTableRef ) AS sourceTable ) AS `extract`,
  IF
    (eventName = "jobChange",
      1,
      0) AS numExtracts,
    /* This ends the code snippet that 
 columns specific to the Extract operation in BQ */ /* The following code queries data specific to the Query operation in BQ */ REGEXP_CONTAINS(jobconfig_query, 'cloudaudit_googleapis_com_data_access_') AS isAuditDashboardQuery,
    errorCode IS NOT NULL AS isError,
    REGEXP_CONTAINS(errorMessage, 'timeout') AS isTimeout,
    isCached,
  IF
    (isCached,
      1,
      0) AS numCached,
    totalSlotMs,
    totalSlotMs / runtimeMs AS avgSlots,
    /* The following statement breaks down the query into minute buckets
   * and provides the average slot usage within that minute. This is a
   * crude way of making it so you can retrieve the average slot utilization
   * for a particular minute across multiple queries.
   */ ARRAY(
    SELECT
      STRUCT( TIMESTAMP_TRUNC(TIMESTAMP_ADD(startTime, INTERVAL bucket_num MINUTE), MINUTE) AS time,
        totalSlotMs / runtimeMs AS avgSlotUsage )
    FROM
      UNNEST(GENERATE_ARRAY(1, executionMinuteBuckets)) AS bucket_num ) AS executionTimeline,
    totalTablesProcessed,
    totalViewsProcessed,
    totalProcessedBytes,
    totalBilledBytes,
    (totalBilledBytes / 1000000000) AS totalBilledGigabytes,
    (totalBilledBytes / 1000000000) / 1000 AS totalBilledTerabytes,
    ((totalBilledBytes / 1000000000) / 1000) * 5 AS estimatedCostUsd,
    billingTier,
    query,
    CONCAT(dataset_id, '.', table_id) AS queryDestinationTableRelativePath,
    CONCAT(projectId, '.', dataset_id, '.', table_id) AS queryDestinationTableAbsolutePath,
    referencedViews,
    referencedTables,
    refTable_project_id,
    refTable_dataset_id,
    refTable_table_id,
    refView_project_id,
    refView_dataset_id,
    refView_table_id,
  IF
    (eventName = "jobChange",
      1,
      0) AS queries /* This ends the code snippet that queries columns specific to the Query operation in BQ */
  FROM
    BQAudit2_job
  LEFT JOIN
    BQAudit2_data
  ON
    (data_jobid=jobId)
  WHERE
    statementType = "SCRIPT"
    OR jobChangeAfter= "DONE"
    OR tableDataChangeReason ="QUERY" 
