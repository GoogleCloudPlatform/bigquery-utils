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

-- Given a key and a list of key-value maps of the form [{'key': 'a', 'value': 'b'}, ...]
-- returns the SCALAR type value.
CREATE OR REPLACE FUNCTION fn.get_value(get_key STRING, arr ANY TYPE) AS
(
  (SELECT value FROM UNNEST(arr) WHERE key = get_key)
);
