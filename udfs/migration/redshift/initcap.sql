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

CREATE OR REPLACE FUNCTION rs.initcap(string_expr STRING) AS (
  (
    SELECT
      CASE 
        WHEN string_expr = '' THEN ''
        ELSE 
          STRING_AGG(
            CONCAT(
              UPPER(SUBSTR(word, 1, 1)),
              LOWER(SUBSTR(word, 2))),
            '' ORDER BY pos)
        END
    FROM UNNEST(REGEXP_EXTRACT_ALL(string_expr, r'\s+|\p{P}+|\p{S}+|.[^\s\p{P}\p{S}]*')) AS word
    WITH OFFSET AS pos
  )
);
