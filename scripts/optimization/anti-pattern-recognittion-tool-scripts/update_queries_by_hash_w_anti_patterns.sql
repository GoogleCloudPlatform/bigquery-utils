/*
 * Copyright 2023 Google LLC
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

ALTER TABLE <input_table>
ADD COLUMN IF NOT EXISTS recommendation ARRAY<STRUCT<name STRING, description STRING>>;

UPDATE <input_table> t1
SET t1.recommendation = t2.recommendation
FROM optimization_workshop.antipattern_output_table t2
WHERE t1.<input_table_id_col_name> = t2.job_id;
