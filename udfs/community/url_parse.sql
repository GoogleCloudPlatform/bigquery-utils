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

--url_parse(string urlString, string partToExtract)
--Returns the specified part from the URL. Valid values for partToExtract include PROTOCOL, HOST, PATH, QUERY, and REF.
--For example, parse_url('http://facebook.com/path1/p.php?k1=v1&k2=v2#Ref1', 'HOST') returns 'facebook.com'.
CREATE OR REPLACE FUNCTION fn.url_parse(url STRING, part STRING)
AS (
  CASE
    WHEN UPPER(part) = 'HOST'  THEN REGEXP_EXTRACT(url, r'(?:[a-zA-Z]+://)?([a-zA-Z0-9-.]+)/?')
    WHEN UPPER(part) = 'PATH'  THEN REGEXP_EXTRACT(url, r'(?:[a-zA-Z]+://)?(?:[a-zA-Z0-9-.]+)/{1}([a-zA-Z0-9-./]+)')
    WHEN UPPER(part) = 'QUERY' THEN REGEXP_EXTRACT(url, r'\?(.*)')
    WHEN UPPER(part) = 'REF'   THEN REGEXP_EXTRACT(url, r'#(.*)')
    WHEN UPPER(part) = 'PROTOCOL' THEN REGEXP_EXTRACT(url, r'^([a-zA-Z]+)://')
    ELSE NULL
  END
);
