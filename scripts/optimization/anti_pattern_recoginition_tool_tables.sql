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
  Query_Hash id, 
  Query_Raw_Sample query 
FROM 
  optimization_workshop.viewable_queries_grouped_by_hash 
ORDER BY 
  Total_Slot_Hours desc
LIMIT 
  1000
;
