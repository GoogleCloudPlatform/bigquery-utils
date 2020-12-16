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

SELECT
--  SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(1)] AS query_id,
 SPLIT(SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(1)], '_')[OFFSET(0)] AS complexity,
 COUNT(1)
FROM
  `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE
  DATE(creation_time) = CURRENT_DATE()  -- Partitioning column
  AND project_id = 'YOUR_PROJECT'       -- Clustering column
  AND SPLIT(TRIM(SPLIT(query, '*/')[OFFSET(0)],'/*'))[OFFSET(0)] = 'jmeter_jdbc_test'
GROUP BY 1
