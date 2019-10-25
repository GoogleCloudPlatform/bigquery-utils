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

CREATE OR REPLACE FUNCTION rs.interval_literal_to_seconds(interval_literal STRING) AS (
/* For intervals in Redshift, there are 360 days in a year. */
(select sum(case
  when unit in ('minutes',  'minute', 'm'	) then num * 60
  when unit in ('hours', 'hour', 'h'	) then num * 60 * 60
  when unit in ('days', 'day', 'd'	) then num * 60 * 60 * 24
  when unit in ('weeks', 'week', 'w'	) then num * 60 * 60 * 24 * 7
  when unit in ('months', 'month' ) then num * 60 * 60 * 24 * 30 
  when unit in ('years', 'year') then num * 60 * 60 * 24 * 360
  else num
end)
from (
  select
    cast(regexp_extract(value, r'^[0-9]*\.?[0-9]+') as numeric) num,
    substr(value, length(regexp_extract(value, r'^[0-9]*\.?[0-9]+')) + 1) unit
  from 
    UNNEST(SPLIT(replace(interval_literal, ' ', ''), ',')) value
)));

