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
 * It is assumed that the following query will be run in the administration
 * project where the reservations were created. If this is not the case,
 * prepend the project id to the table name as follows:
 * `{project_id}`.`region-{region_name}`.INFORMATION_SCHEMA.{table}
 */

/*
 * Job Error Report: Returns information about jobs that resulted in an
 * error
 */

SELECT
  project_id,
  user_email,
  creation_time,
  job_type,
  CASE WHEN statement_type IS NULL THEN 'N/A' ELSE statement_type END AS statement_type,
  error_result
FROM
  `region-{region_name}`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
WHERE
  -- Jobs that resulted in an error will have the error_result.reason
  -- field populated
  error_result.reason IS NOT NULL
