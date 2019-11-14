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

CREATE OR REPLACE FUNCTION rs.interval_literal_to_seconds(interval_literal STRING) 
AS ( 
  /* For intervals in Redshift, there are 360 days in a year. */ 
  (
    SELECT
      SUM(
          CASE
            WHEN unit IN ('minutes', 'minute', 'm') THEN num * 60
            WHEN unit IN ('hours', 'hour', 'h') THEN num * 60 * 60
            WHEN unit IN ('days', 'day', 'd') THEN num * 60 * 60 * 24
            WHEN unit IN ('weeks','week', 'w') THEN num * 60 * 60 * 24 * 7
            WHEN unit IN ('months', 'month') THEN num * 60 * 60 * 24 * 30
            WHEN unit IN ('years', 'year') THEN num * 60 * 60 * 24 * 360
            ELSE num
            END)
    FROM 
      (
        SELECT
          CAST(REGEXP_EXTRACT(value, r'^[0-9]*\.?[0-9]+') AS numeric) num,
          SUBSTR(value, LENGTH(REGEXP_EXTRACT(value, r'^[0-9]*\.?[0-9]+')) + 1) unit
        FROM
          UNNEST(SPLIT(REPLACE(interval_literal, ' ', ''), ',')) value 
      )
  ) 
);