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


CREATE OR REPLACE FUNCTION st.linear_regression(data ARRAY<STRUCT<X FLOAT64, Y FLOAT64>>) AS (
    (
    with PRELIM AS(
        SELECT SUM(X) AS Sx, SUM(Y) AS Sy,
        SUM(X * X) AS Sxx,
        SUM(X * Y) AS Sxy,
        SUM(Y * Y) AS Syy,
        COUNT(*) AS N
        FROM (
            SELECT Los AS x, Charge AS Y FROM `Inpatient Confinement`
        )
    )


SELECT 
    ((Sy * Sxx) - (Sx * Sxy) / ((N * (Sxx)) - (Sx * Sx)) AS a,
    ((N * Sxy) - (Sx * Sy))  / ((N * Sxx) - (Sx * Sx)) AS b,
    ((N * Sxy) - (Sx * Sy))
    / SQRT(
        (((N * Sxx) - (Sx * Sx))
         * ((N * Syy - (Sy * Sy))))) AS r
    FROM
      (
  
      ) sums;