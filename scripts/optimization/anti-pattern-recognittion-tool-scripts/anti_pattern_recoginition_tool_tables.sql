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
 
CREATE OR REPLACE TABLE optimization_workshop.antipattern_output_table (
  job_id STRING,
  user_email STRING,
  query STRING,
  recommendation ARRAY<STRUCT<name STRING, description STRING>>,
  slot_hours FLOAT64,
  optimized_sql STRING,
  process_timestamp TIMESTAMP
);

CREATE OR REPLACE VIEW optimization_workshop.antipattern_tool_input_view AS
SELECT 
  <input_table_id_col_name> id, 
  ANY_VALUE(<input_table_query_text_col_name>) query
FROM 
  <input_table>
WHERE
  <input_table_id_col_name> is not null
GROUP BY 
  <input_table_id_col_name>
ORDER BY 
  ANY_VALUE(<input_table_slots_col_name>) desc
LIMIT 
  1000
;
