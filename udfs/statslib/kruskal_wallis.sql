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

CREATE OR REPLACE FUNCTION st.kruskal_wallis(data ARRAY<STRUCT<factor STRING, val FLOAT64>>) AS ((
    with H_raw AS(
        with sums AS 
        (
            with rank_data AS 
            (
                select d.factor AS f, d.val AS v, rank() over(order by d.val) AS r
                from unnest(data) AS d 
            ) #rank_data
            select     
                SUM(r) * SUM(r) / COUNT(*) AS sumranks, COUNT(*) AS n
            from rank_data
            GROUP BY f
        ) # sums
        SELECT 12.00 /(SUM(n) *(SUM(n) + 1)) * SUM(sumranks) -(3.00 *(SUM(n) + 1)) AS H, 
                      count(n) -1 AS DoF
        FROM sums
    ) # H_raw
    SELECT struct(H AS H, st.pvalue(H, DoF) AS p, DoF AS DoF) from H_raw
));