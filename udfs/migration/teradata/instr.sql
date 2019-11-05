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

CREATE OR REPLACE FUNCTION td.instr(
  haystack STRING, needle STRING, position INT64, occurrence INT64)
AS
(
  (
    SELECT
      IFNULL(
        NULLIF(
          SUM(LENGTH(str)) + ((occurrence - 1) * LENGTH(needle)) + position,
          LENGTH(haystack) + 1),
        0)  -- No match
    FROM
      UNNEST(SPLIT(SUBSTR(haystack, position), needle)) AS str WITH OFFSET off
    WHERE
      off < occurrence
  )
);
