/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * View: bigquery_audit_logs_v1
 * Author: ryanmcdowell, freedomofnet, mihirborkar
 * Description:
 * This is a user-friendly view over BigQuery job events based on
 * the legacy BigQuery audit data: https://cloud.google.com/bigquery/docs/reference/auditlogs/rest/Shared.Types/AuditData.
 */

CREATE OR REPLACE VIEW `project_id.dataset_id.bigquery_audit_logs_v1` AS
WITH BQAudit AS (
  SELECT
    protopayload_auditlog.authenticationInfo.principalEmail,
    protopayload_auditlog.requestMetadata.callerIp,
    protopayload_auditlog.serviceName,
    protopayload_auditlog.methodName,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.projectId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobName.jobId,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.createTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code
    AS errorCode,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.message
    AS errorMessage,
    TIMESTAMP_DIFF(
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, MILLISECOND)
      AS runtimeMs,
    /* This following code extracts the column specific to the Copy operation in BQ */
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.tableCopy,
    /* This following code extracts the column specific to the Extract operation in BQ */
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.extract,
    /* The following code extracts the columns specific to the Load operation in BQ */
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalLoadOutputBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.load,
    /* The following code extracts columns specific to Query operation in BQ */
    TIMESTAMP_DIFF(
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, SECOND)
      AS runtimeSecs,
    CAST(CEILING((TIMESTAMP_DIFF(
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.endTime,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.startTime, SECOND)) / 60) AS INT64)
      AS executionMinuteBuckets,
    IF(COALESCE(protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
      protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatus.error.code) IS NULL, TRUE, FALSE
    ) AS isCached,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalSlotMs,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalTablesProcessed,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalViewsProcessed,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalProcessedBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.totalBilledBytes,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.billingTier,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobConfiguration.query,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedTables,
    protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.job.jobStatistics.referencedViews
  FROM
    `project_id.dataset_id.cloudaudit_googleapis_com_data_access_*`
  WHERE
    protopayload_auditlog.serviceName = 'bigquery.googleapis.com'
    AND protopayload_auditlog.methodName = 'jobservice.jobcompleted'
    AND protopayload_auditlog.servicedata_v1_bigquery.jobCompletedEvent.eventName IN
    (
      'table_copy_job_completed',
      'query_job_completed',
      'extract_job_completed',
      'load_job_completed'
    )
)
/* The following builds a user-friendly projection of the audit data. */
SELECT
  principalEmail,
  callerIp,
  serviceName,
  methodName,
  eventName,
  projectId,
  jobId,
  errorCode,
  errorMessage,
  STRUCT(
    EXTRACT(MINUTE FROM startTime) AS minuteOfDay,
    EXTRACT(HOUR FROM startTime) AS hourOfDay,
    EXTRACT(DAYOFWEEK FROM startTime) - 1 AS dayOfWeek,
    EXTRACT(DAYOFYEAR FROM startTime) AS dayOfYear,
    EXTRACT(WEEK FROM startTime) AS week,
    EXTRACT(MONTH FROM startTime) AS month,
    EXTRACT(QUARTER FROM startTime) AS quarter,
    EXTRACT(YEAR FROM startTime) AS year
  ) AS date,
  createTime,
  startTime,
  endTime,
  runtimeMs,
  runtimeSecs,
  /* This code queries data specific to the Copy operation */
  STRUCT(
    tableCopy.sourceTables,
    STRUCT(
      tableCopy.destinationTable.projectId,
      tableCopy.destinationTable.datasetId,
      tableCopy.destinationTable.tableId,
      CONCAT(tableCopy.destinationTable.datasetId, '.', tableCopy.destinationTable.tableId) AS relativePath,
      CONCAT(tableCopy.destinationTable.projectId, '.', tableCopy.destinationTable.datasetId,
        '.', tableCopy.destinationTable.tableId) AS absolutePath
    ) AS destinationTable,
    tableCopy.createDisposition,
    tableCopy.writeDisposition
  ) AS tableCopy,
  IF(eventName = 'table_copy_job_completed', 1, 0) AS numCopies,
  /* The following code queries data specific to the Load operation in BQ */
  totalLoadOutputBytes,
  (totalLoadOutputBytes / pow(2,30)) AS totalLoadOutputGigabytes,
  (totalLoadOutputBytes / pow(2,40)) AS totalLoadOutputTerabytes,
  STRUCT(
    load.sourceUris,
    STRUCT(
      load.destinationTable.projectId,
      load.destinationTable.datasetId,
      load.destinationTable.tableId,
      CONCAT(load.destinationTable.datasetId, '.', load.destinationTable.tableId) AS relativePath,
      CONCAT(load.destinationTable.projectId, '.', load.destinationTable.datasetId,
        '.', load.destinationTable.tableId) AS absolutePath
    ) AS destinationTable,
    load.createDisposition,
    load.writeDisposition,
    load.schemaJson
  ) AS load,
  IF(eventName = 'load_job_completed', 1, 0) AS numLoads,
  /* The following code queries data specific to the Extract operation in BQ */
  REGEXP_CONTAINS(jobId, 'beam') AS isBeamJob,
  STRUCT(
    `extract`.destinationUris,
    STRUCT(
      `extract`.sourceTable.projectId,
      `extract`.sourceTable.datasetId,
      `extract`.sourceTable.tableId,
      CONCAT(`extract`.sourceTable.datasetId, '.', `extract`.sourceTable.tableId)
      AS relativeTableRef,
      CONCAT(`extract`.sourceTable.projectId, '.', `extract`.sourceTable.datasetId,
      '.', `extract`.sourceTable.tableId) AS absoluteTableRef
    ) AS sourceTable
  ) AS `extract`,
  IF(eventName = 'extract_job_completed', 1, 0) AS numExtracts,
  /* The following code queries data specific to the Query operation in BQ */
  REGEXP_CONTAINS(query.query, 'cloudaudit_googleapis_com_data_access_') AS isAuditDashboardQuery,
  errorCode IS NOT NULL AS isError,
  REGEXP_CONTAINS(errorMessage, 'timeout') AS isTimeout,
  isCached,
  IF(isCached, 1, 0) AS numCached,
  totalSlotMs,
  totalSlotMs / runtimeMs AS avgSlots,
  /* The following statement breaks down the query into minute buckets
   * and provides the average slot usage within that minute. This is a
   * crude way of making it so you can retrieve the average slot utilization
   * for a particular minute across multiple queries.
   */
  ARRAY(
    SELECT
      STRUCT(
        TIMESTAMP_TRUNC(TIMESTAMP_ADD(startTime, INTERVAL bucket_num MINUTE),
        MINUTE) AS time,
        totalSlotMs / runtimeMs AS avgSlotUsage
      )
    FROM
      UNNEST(GENERATE_ARRAY(1, executionMinuteBuckets)) AS bucket_num
  ) AS executionTimeline,
  totalTablesProcessed,
  totalViewsProcessed,
  totalProcessedBytes,
  (totalProcessedBytes / pow(2,30)) AS totalProcessedGigabytes,
  (totalProcessedBytes / pow(2,40)) AS totalProcessedTerabytes,
  totalBilledBytes,
  (totalBilledBytes / pow(2,30)) AS totalBilledGigabytes,
  (totalBilledBytes / pow(2,40)) AS totalBilledTerabytes,
  (totalBilledBytes / pow(2,40)) * 5 AS estimatedCostUsd,
  billingTier,
  query,
  CONCAT(query.destinationTable.datasetId, '.', query.destinationTable.tableId) AS queryDestinationTableRelativePath,
  CONCAT(query.destinationTable.projectId, '.', query.destinationTable.datasetId, '.',
    query.destinationTable.tableId) AS queryDestinationTableAbsolutePath,
  referencedTables,
  referencedViews,
  IF(eventName = 'query_job_completed', 1, 0) AS queries
FROM
  BQAudit
