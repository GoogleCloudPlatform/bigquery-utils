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


CREATE OR REPLACE FUNCTION fn.url_parse(url STRING, part STRING)
AS (
  CASE
    -- Return HOST part of the URL.
    WHEN UPPER(part) = 'HOST' THEN SPLIT(fn.check_protocol(url), '/')[OFFSET(2)]
    WHEN UPPER(part) = 'PATH' THEN REGEXP_EXTRACT(url, r'^[a-zA-Z]+://[a-zA-Z0-9.-]+/([a-zA-Z0-9.-/]+)') 
    WHEN UPPER(part) = 'QUERY' THEN 
      IF(REGEXP_CONTAINS(url, r'\?'), SPLIT(fn.check_protocol(url), '?')[OFFSET(1)], NULL)
    WHEN UPPER(part) = 'REF' THEN
      IF(REGEXP_CONTAINS(url, '#'), SPLIT(fn.check_protocol(url), '#')[OFFSET(1)], NULL)
    WHEN UPPER(part) = 'PROTOCOL' THEN RTRIM(REGEXP_EXTRACT(url, '^[a-zA-Z]+://'), '://')
    ELSE ''
  END
);
