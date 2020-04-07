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

-- typeof:
-- Input: Any
-- Output: Type of input or 'UNKNOWN' if input is unknown typed value
CREATE OR REPLACE FUNCTION fn.typeof(input ANY TYPE)
AS ( (
    SELECT
      CASE
      -- Process NUMERIC, DATE, DATETIME, TIME, TIMESTAMP,
        WHEN REGEXP_CONTAINS(literal, r'^[A-Z]+ "') THEN REGEXP_EXTRACT(literal, r'^([A-Z]+) "')
        WHEN REGEXP_CONTAINS(literal, r'^-?[0-9]*$') THEN 'INT64'
        WHEN REGEXP_CONTAINS(literal, r'^(-?[0-9]+[.e].*|CAST\("([^"]*)" AS FLOAT64\))$') THEN 'FLOAT64'
        WHEN literal IN ('true', 'false') THEN 'BOOL'
        WHEN literal LIKE '"%' THEN 'STRING'
        WHEN literal LIKE 'b"%' THEN 'BYTES'
        WHEN literal LIKE '[%' THEN 'ARRAY'
        WHEN REGEXP_CONTAINS(literal, r'^(STRUCT)?\(') THEN 'STRUCT'
        WHEN literal LIKE 'ST_%' THEN 'GEOGRAPHY'
        WHEN literal = 'NULL' THEN 'NULL'
      ELSE
      'UNKNOWN'
    END
    FROM
      UNNEST([FORMAT('%T', input)]) AS literal));
