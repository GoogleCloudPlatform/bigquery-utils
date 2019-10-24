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

CREATE OR REPLACE FUNCTION td.otranslate(source_string STRING, from_string STRING, to_string STRING) AS (
  IF(LENGTH(from_string) < LENGTH(to_string) OR LENGTH(source_string) < LENGTH(from_string), source_string,
  (SELECT
     STRING_AGG(
       IFNULL(
         (SELECT ARRAY_CONCAT([c], SPLIT(to_string, ''))[SAFE_OFFSET((
            SELECT IFNULL(MIN(o2) + 1, 0) FROM UNNEST(SPLIT(from_string, '')) AS k WITH OFFSET o2
            WHERE k = c))]
         ),
         ''),
       '' ORDER BY o1)
   FROM UNNEST(SPLIT(source_string, '')) AS c WITH OFFSET o1
  ))
);
