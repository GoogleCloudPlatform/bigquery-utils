/*
 * Copyright 2026 Google LLC
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

DECLARE num_days_to_scan INT64 DEFAULT 30;

CREATE SCHEMA IF NOT EXISTS optimization_workshop;
CREATE OR REPLACE TABLE optimization_workshop.bi_engine_disabled_reasons AS
SELECT reasons.code, count(*) 
FROM `region-us`.INFORMATION_SCHEMA.JOBS as jbo, UNNEST(bi_engine_statistics.bi_engine_reasons) AS reasons 
WHERE DATE(jbo.creation_time) >= CURRENT_DATE - num_days_to_scan 
AND bi_engine_statistics.bi_engine_mode = 'DISABLED' 
GROUP BY reasons.code;
