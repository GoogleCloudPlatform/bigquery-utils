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

-- json_typeof:
-- Input: JSON string
-- Output: The JSON value type of the argument or NULL if the argument is an unknown value
CREATE OR REPLACE FUNCTION fn.json_typeof(json STRING)
AS ( (
    SELECT AS VALUE
      CASE
        WHEN value LIKE '{%' THEN 'object'
        WHEN value LIKE '[%' THEN 'array'
        WHEN value LIKE '"%' THEN 'string'
        WHEN REGEXP_CONTAINS(value, r'^[-0-9]') THEN 'number'
        WHEN value IN ('true', 'false') THEN 'boolean'
        WHEN value = 'null' THEN 'null'
      ELSE NULL
    END
    FROM
      UNNEST([TRIM(json)]) AS value));
