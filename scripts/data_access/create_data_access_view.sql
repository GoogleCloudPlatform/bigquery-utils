#standardSQL

--
-- This is setup for storage.googleapis.com and bigquery.googleapis.com
--
-- This is for examining access to bigquery datasets and buckets.
-- 
-- Table and object-level is available in the logs (and more!), but
-- this creates confusion for a dashboard.
-- 
CREATE OR REPLACE VIEW data_access.audit_summary AS
WITH
  -- Pull out Data Access logs
  DataAccess AS (
    SELECT
      -- Hour truncated
      TIMESTAMP_TRUNC(d.timestamp, HOUR) AS hour,
      -- Project ID that the access method was called on
      d.resource.labels.project_id,
      -- Actor
      d.protopayload_auditlog.authenticationInfo.principalEmail AS actor,
      -- Permission used to access data
      SPLIT(i.permission,'.')[SAFE_OFFSET(0)] AS service,
      -- Permission used
      i.permission AS action,
      -- Whether granted or denied
      IFNULL(i.granted, FALSE) AS granted,
      -- Parts of the resource accessed
      SPLIT(i.resource, '/') AS parts
    FROM
      `${PROJECT_ID}.data_access.cloudaudit_googleapis_com_data_access_*` d
      CROSS JOIN d.protopayload_auditlog.authorizationInfo i
    WHERE
      i.resource IS NOT NULL AND
      d.protopayload_auditlog.serviceName IN ('storage.googleapis.com',
                                              'bigquery.googleapis.com')
  )
SELECT
  hour,
  service,
  actor,
  -- Translate the action into an operation (READ/WRITE/ADMIN)
  CASE
    WHEN service = 'storage' THEN
      CASE
      -- See granular permissions here: https://cloud.google.com/storage/docs/access-control/iam-permissions
      WHEN action IN ('storage.objects.create',
                      'storage.objects.delete') THEN
        'WRITE'
      WHEN action IN ('storage.objects.get') THEN
        'READ'
      WHEN action IN ('storage.objects.getIamPolicy',
                      'storage.objects.list',
                      'storage.objects.setIamPolicy',
                      'storage.objects.update',
                      'storage.buckets.create',
                      'storage.buckets.delete',
                      'storage.buckets.get',
                      'storage.buckets.getIamPolicy',
                      'storage.buckets.list',
                      'storage.buckets.setIamPolicy',
                      'storage.buckets.update') THEN
        'ADMIN'
      ELSE
        CONCAT('Unknown storage:', action)
      END
    -- See granular permissions here: https://cloud.google.com/bigquery/docs/access-control#bq-permissions
    WHEN service = 'bigquery' THEN
      CASE
      WHEN action IN ('bigquery.tables.delete',
                            'bigquery.datasets.delete',
                            'bigquery.jobs.update',
                            'bigquery.routines.delete',
                            'bigquery.tables.updateData') THEN
        'WRITE'
      WHEN action IN ('bigquery.tables.getData',
                            'bigquery.tables.export',
                            'bigquery.readsessions.create',
                            'bigquery.connections.use') THEN
        'READ'
      WHEN action IN ('bigquery.jobs.create',
                            'bigquery.jobs.listAll',
                            'bigquery.jobs.list',
                            'bigquery.jobs.get',
                            'bigquery.datasets.create',
                            'bigquery.datasets.get',
                            'bigquery.datasets.update',
                            'bigquery.tables.create',
                            'bigquery.tables.list',
                            'bigquery.tables.get',
                            'bigquery.tables.update',
                            'bigquery.routines.create',
                            'bigquery.routines.list',
                            'bigquery.routines.get',
                            'bigquery.routines.update',
                            'bigquery.transfers.get',
                            'bigquery.transfers.update',
                            'bigquery.savedqueries.create',
                            'bigquery.savedqueries.get',
                            'bigquery.savedqueries.list',
                            'bigquery.savedqueries.update',
                            'bigquery.savedqueries.delete',
                            'bigquery.connections.create',
                            'bigquery.connections.get',
                            'bigquery.connections.list',
                            'bigquery.connections.update',
                            'bigquery.connections.delete') THEN
        'ADMIN'
      ELSE
        CONCAT('Unknown bigquery:', action)
      END
    ELSE
      CONCAT('Unknown service:', service)
  END AS op,
  granted,
  -- Project is of the resource or, if not there,
  -- then for the method accessing it (eg for buckets)
  CASE
      -- BigQuery project.dataset
      WHEN service = 'bigquery' THEN
        CONCAT(parts[SAFE_OFFSET(1)], '.', parts[SAFE_OFFSET(3)])
      -- GCS project.bucket
      WHEN service = 'storage' THEN
        CONCAT(project_id, '.', parts[SAFE_OFFSET(3)])
  END AS entity
FROM
  DataAccess
WHERE
  -- Limit to BigQuery dataset / GCS bucket operations
  ARRAY_LENGTH(parts) >= 4
GROUP BY
  1,2,3,4,5,6;
