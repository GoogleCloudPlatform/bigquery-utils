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



CREATE OR REPLACE FUNCTION st.linear_regression(data ARRAY<STRUCT<X FLOAT64, Y FLOAT64>>) 
AS ((
     WITH results AS (
       WITH sums AS (
         with d as (
              select * from unnest(data)
         )
         select 
            SUM(d.X) as Sx,
            SUM(d.Y) as Sy,
            SUM(d.X * d.Y) as Sxy,
            SUM(d.X * d.X) as Sxx,
            SUM(d.Y * d.Y) as Syy,
            COUNT(*) as N
         from d
       )
       SELECT 
        ((Sy * Sxx) - (Sx * Sxy)) / ((N * (Sxx)) - (Sx * Sx)) AS a,
        ((N * Sxy) - (Sx * Sy))  / ((N * Sxx) - (Sx * Sx)) AS b,
        ((N * Sxy) - (Sx * Sy))/ SQRT(
            (((N * Sxx) - (Sx * Sx))* ((N * Syy - (Sy * Sy))))) AS r 
        from sums
      )
      select STRUCT(a, b, r) from results
));
