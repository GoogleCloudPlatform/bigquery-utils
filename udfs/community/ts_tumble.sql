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

--divides a timestamp into a [tumble window](https://cloud.google.com/dataflow/docs/reference/sql/streaming-extensions#tumble)
--specified in seconds 
CREATE OR REPLACE FUNCTION fn.ts_tumble(val TIMESTAMP, tumble_seconds INT64)
AS (
 timestamp_seconds(div(unix_seconds(val), tumble_seconds) *  tumble_seconds))
);