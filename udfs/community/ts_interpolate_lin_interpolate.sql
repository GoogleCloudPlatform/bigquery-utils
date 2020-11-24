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

/*
* wrap fn.linear_interpolate to handle time series interpolation
* 
* Example usage: use value if exists, otherwise attempt linear interpolation, else fill with zero
* 
* WITH tbl AS (
*   SELECT 'abc' key, CAST('2021-01-01' AS TIMESTAMP) ts, 1 value, STRUCT(CAST('2021-01-01' AS TIMESTAMP) AS x, 1 AS y) coord
*   UNION ALL
*   SELECT 'abc', CAST('2021-01-02' AS TIMESTAMP), null, null
*   UNION ALL
*   SELECT 'abc', CAST('2021-01-03' AS TIMESTAMP), 3, STRUCT(CAST('2021-01-03' AS TIMESTAMP) AS x, 3 AS y)
*   UNION ALL
*   SELECT 'abc', CAST('2021-01-04' AS TIMESTAMP), null, null
* )
* SELECT 
*   *,
*   COALESCE(coord.y,
*     fn.ts_lin_interpolate(
*       ts,
*       LAST_VALUE(coord IGNORE NULLS)
*         OVER (PARTITION BY key
*           ORDER BY unix_seconds(ts) ASC
*           RANGE BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
*       FIRST_VALUE(coord IGNORE NULLS)
*         OVER (PARTITION BY key
*           ORDER BY unix_seconds(ts) ASC
*           RANGE BETWEEN 1 FOLLOWING AND UNBOUNDED FOLLOWING)
*       ),
*       0
*   ) AS intrp
* FROM tbl
*/

CREATE OR REPLACE FUNCTION fn.ts_lin_interpolate(
        pos TIMESTAMP,
        prev STRUCT<x TIMESTAMP, y FLOAT64>,
        next STRUCT<x TIMESTAMP, y FLOAT64>)
AS (
 CASE
    WHEN pos IS NULL OR prev IS NULL or next IS NULL THEN null
    ELSE
        bqutil.fn.linear_interpolate(
            unix_seconds(pos),
            STRUCT(unix_seconds(prev.x) AS x, prev.y AS y),
            STRUCT(unix_seconds(next.x) AS x, next.y AS y)
        )
 END
);