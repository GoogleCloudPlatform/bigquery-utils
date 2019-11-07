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

/*
 * BigQuery supports a maximum precision / scale of (38,9) for NUMERIC. For some
 * applications (e.g financial) it may be necessary to multiply a high scale
 * number such as an FX Rate against another NUMERIC value. This function takes
 * in a STRING high_scale_value and multiplies against a multiplier to allow for
 * high scale multiplication while returning BigQuery's NUMERIC default of (38,9)
 */
CREATE OR REPLACE FUNCTION multiply_full_scale(high_scale_value STRING, multiplier NUMERIC) AS (
  CAST(SPLIT(high_scale_value, '.')[OFFSET(0)] AS NUMERIC) * multiplier +
  IFNULL(CAST(SPLIT(high_scale_value, '.')[SAFE_OFFSET(1)] AS NUMERIC) * multiplier /
    POW(NUMERIC '10', LENGTH(SPLIT(high_scale_value, '.')[SAFE_OFFSET(1)])), 0)
);
