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

-- string_to_struct:
-- helper function for parsing key-value data from a string to struct
-- Input:
-- strList: string that has map in the format 'a:b,c:d....'
-- entry_delimiter: string that has the delimiter e.g. ','
-- kv_delimiter: string that has the delimiter between key and value e.g. ':'
-- Output: struct for the above map.
CREATE OR REPLACE FUNCTION fn.string_to_struct(strList STRING, entry_delimiter STRING, kv_delimiter STRING)
AS (
  CASE
    WHEN REGEXP_CONTAINS(strList, entry_delimiter) OR REGEXP_CONTAINS(strList, kv_delimiter) THEN
      (ARRAY(
        WITH list AS (
          SELECT l FROM UNNEST(SPLIT(TRIM(strList), entry_delimiter)) l WHERE REGEXP_CONTAINS(l, kv_delimiter)
        )
        SELECT AS STRUCT
            TRIM(SPLIT(l, kv_delimiter)[OFFSET(0)]) AS key, TRIM(SPLIT(l, kv_delimiter)[OFFSET(1)]) as value
        FROM list
      ))
    ELSE NULL
  END
);