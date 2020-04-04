/*
 * Copyright 2019 Google LLC
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

-- typeof:
-- Input: Any
-- Output: Type of input or 'UNKNOWN' if input is unknown typed value
CREATE OR REPLACE FUNCTION fn.typeof(input ANY TYPE)
AS ((
    SELECT
      AS VALUE
      CASE
        WHEN REGEXP_CONTAINS(fmt, r'^[0-9]*$') THEN 'INT64'
        WHEN fmt LIKE 'NUMERIC "%' THEN 'NUMERIC'
        WHEN REGEXP_CONTAINS(fmt, r'^([0-9]*\.[0-9]*|CAST\("([^"]*)" AS FLOAT64\))$') THEN 'FLOAT64'
        WHEN fmt IN ('true', 'false') THEN 'BOOL'
        WHEN fmt LIKE '"%' THEN 'STRING'
        WHEN fmt LIKE 'b"%' THEN 'BYTES'
        WHEN fmt LIKE 'DATE "%' THEN 'DATE'
        WHEN fmt LIKE 'DATETIME "%' THEN 'DATETIME'
        WHEN fmt LIKE 'TIME "%' THEN 'TIME'
        WHEN fmt LIKE 'TIMESTAMP "%' THEN 'TIMESTAMP'
        WHEN fmt LIKE '[%' THEN 'ARRAY'
        WHEN REGEXP_CONTAINS(fmt, r'^(STRUCT)?\(') THEN 'STRUCT'
        WHEN fmt LIKE 'ST_%' THEN 'GEOGRAPHY'
        WHEN fmt = 'NULL' THEN 'NULL'
      ELSE
      'UNKNOWN'
    END
    FROM UNNEST([FORMAT('%T', input)]) AS fmt ));
