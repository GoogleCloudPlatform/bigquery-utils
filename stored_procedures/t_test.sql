/*
 * Copyright 2021 Google LLC
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

-- @param STRING pop1_table table name (or subquery) that contains the first population
-- @param STRING pop1_variable name of the measurement column in our first table
-- @param STRING pop2_table table name (or subquery) that contains the second population
-- @param STRING pop2_variable name of the measurement column in our second table
-- @return STRUCT<t_value FLOAT64, dof FLOAT64, p_value FLOAT64>

CREATE OR REPLACE PROCEDURE bqutil.procedure.t_test(pop1_table STRING, pop1_variable STRING, pop2_table STRING, pop2_variable STRING, OUT result STRUCT<t_value FLOAT64, dof FLOAT64, p_value FLOAT64> )
BEGIN
EXECUTE IMMEDIATE """
    WITH pop1 AS (
        SELECT `""" || pop1_variable || """` AS value
        FROM """ || pop1_table ||"""
    ), pop2 as (
        SELECT `""" || pop2_variable || """` AS value
        FROM """ || pop2_table || """
    )
    SELECT STRUCT(
        ABS(x1 - x2) / Sqrt((st1 * st1 / n1) + (st2 * st2 / n2)) AS t_value,
        n1 + n2 - 2 AS dof,
        bqutil.fn.pvalue(ABS(x1 - x2) / Sqrt((st1 * st1 / n1) + (st2 * st2 / n2)), n1 + n2 - 2) AS p_value
    )
    FROM (
        SELECT
            AVG(value) x1,
            STDDEV(value) st1,
            COUNT(value) AS n1
        FROM pop1
    )
    CROSS JOIN
    (
        SELECT
            AVG(value) x2,
            STDDEV(value) st2,
            COUNT(value) AS n2
        FROM pop2
    )
""" INTO result;
END;

-- a unit test of t_test_paired
BEGIN
  DECLARE result STRUCT<t_value FLOAT64, dof FLOAT64, p_value FLOAT64>;
  CREATE TEMP TABLE iris (sepal_length FLOAT64, sepal_width FLOAT64, petal_length FLOAT64, petal_width FLOAT64, species STRING)
  AS
  SELECT 5.1 AS sepal_length,
       3.5 AS sepal_width,
       1.4 AS petal_length,
       0.2 AS petal_width,
       'setosa' AS species
     UNION ALL SELECT 4.9,3.0,1.4,0.2,'setosa'
     UNION ALL SELECT 4.7,3.2,1.3,0.2,'setosa'
     UNION ALL SELECT 4.6,3.1,1.5,0.2,'setosa'
     UNION ALL SELECT 5.0,3.6,1.4,0.2,'setosa'
     UNION ALL SELECT 5.4,3.9,1.7,0.4,'setosa'
     UNION ALL SELECT 4.6,3.4,1.4,0.3,'setosa'
     UNION ALL SELECT 5.0,3.4,1.5,0.2,'setosa'
     UNION ALL SELECT 4.4,2.9,1.4,0.2,'setosa'
     UNION ALL SELECT 4.9,3.1,1.5,0.1,'setosa'
     UNION ALL SELECT 5.4,3.7,1.5,0.2,'setosa'
     UNION ALL SELECT 4.8,3.4,1.6,0.2,'setosa'
     UNION ALL SELECT 4.8,3.0,1.4,0.1,'setosa'
     UNION ALL SELECT 4.3,3.0,1.1,0.1,'setosa'
     UNION ALL SELECT 5.8,4.0,1.2,0.2,'setosa'
     UNION ALL SELECT 5.7,4.4,1.5,0.4,'setosa'
     UNION ALL SELECT 5.4,3.9,1.3,0.4,'setosa'
     UNION ALL SELECT 5.1,3.5,1.4,0.3,'setosa'
     UNION ALL SELECT 5.7,3.8,1.7,0.3,'setosa'
     UNION ALL SELECT 5.1,3.8,1.5,0.3,'setosa'
     UNION ALL SELECT 5.4,3.4,1.7,0.2,'setosa'
     UNION ALL SELECT 5.1,3.7,1.5,0.4,'setosa'
     UNION ALL SELECT 4.6,3.6,1.0,0.2,'setosa'
     UNION ALL SELECT 5.1,3.3,1.7,0.5,'setosa'
     UNION ALL SELECT 4.8,3.4,1.9,0.2,'setosa'
     UNION ALL SELECT 5.0,3.0,1.6,0.2,'setosa'
     UNION ALL SELECT 5.0,3.4,1.6,0.4,'setosa'
     UNION ALL SELECT 5.2,3.5,1.5,0.2,'setosa'
     UNION ALL SELECT 5.2,3.4,1.4,0.2,'setosa'
     UNION ALL SELECT 4.7,3.2,1.6,0.2,'setosa'
     UNION ALL SELECT 4.8,3.1,1.6,0.2,'setosa'
     UNION ALL SELECT 5.4,3.4,1.5,0.4,'setosa'
     UNION ALL SELECT 5.2,4.1,1.5,0.1,'setosa'
     UNION ALL SELECT 5.5,4.2,1.4,0.2,'setosa'
     UNION ALL SELECT 4.9,3.1,1.5,0.1,'setosa'
     UNION ALL SELECT 5.0,3.2,1.2,0.2,'setosa'
     UNION ALL SELECT 5.5,3.5,1.3,0.2,'setosa'
     UNION ALL SELECT 4.9,3.1,1.5,0.1,'setosa'
     UNION ALL SELECT 4.4,3.0,1.3,0.2,'setosa'
     UNION ALL SELECT 5.1,3.4,1.5,0.2,'setosa'
     UNION ALL SELECT 5.0,3.5,1.3,0.3,'setosa'
     UNION ALL SELECT 4.5,2.3,1.3,0.3,'setosa'
     UNION ALL SELECT 4.4,3.2,1.3,0.2,'setosa'
     UNION ALL SELECT 5.0,3.5,1.6,0.6,'setosa'
     UNION ALL SELECT 5.1,3.8,1.9,0.4,'setosa'
     UNION ALL SELECT 4.8,3.0,1.4,0.3,'setosa'
     UNION ALL SELECT 5.1,3.8,1.6,0.2,'setosa'
     UNION ALL SELECT 4.6,3.2,1.4,0.2,'setosa'
     UNION ALL SELECT 5.3,3.7,1.5,0.2,'setosa'
     UNION ALL SELECT 5.0,3.3,1.4,0.2,'setosa'
     UNION ALL SELECT 7.0,3.2,4.7,1.4,'versicolor'
     UNION ALL SELECT 6.4,3.2,4.5,1.5,'versicolor'
     UNION ALL SELECT 6.9,3.1,4.9,1.5,'versicolor'
     UNION ALL SELECT 5.5,2.3,4.0,1.3,'versicolor'
     UNION ALL SELECT 6.5,2.8,4.6,1.5,'versicolor'
     UNION ALL SELECT 5.7,2.8,4.5,1.3,'versicolor'
     UNION ALL SELECT 6.3,3.3,4.7,1.6,'versicolor'
     UNION ALL SELECT 4.9,2.4,3.3,1.0,'versicolor'
     UNION ALL SELECT 6.6,2.9,4.6,1.3,'versicolor'
     UNION ALL SELECT 5.2,2.7,3.9,1.4,'versicolor'
     UNION ALL SELECT 5.0,2.0,3.5,1.0,'versicolor'
     UNION ALL SELECT 5.9,3.0,4.2,1.5,'versicolor'
     UNION ALL SELECT 6.0,2.2,4.0,1.0,'versicolor'
     UNION ALL SELECT 6.1,2.9,4.7,1.4,'versicolor'
     UNION ALL SELECT 5.6,2.9,3.6,1.3,'versicolor'
     UNION ALL SELECT 6.7,3.1,4.4,1.4,'versicolor'
     UNION ALL SELECT 5.6,3.0,4.5,1.5,'versicolor'
     UNION ALL SELECT 5.8,2.7,4.1,1.0,'versicolor'
     UNION ALL SELECT 6.2,2.2,4.5,1.5,'versicolor'
     UNION ALL SELECT 5.6,2.5,3.9,1.1,'versicolor'
     UNION ALL SELECT 5.9,3.2,4.8,1.8,'versicolor'
     UNION ALL SELECT 6.1,2.8,4.0,1.3,'versicolor'
     UNION ALL SELECT 6.3,2.5,4.9,1.5,'versicolor'
     UNION ALL SELECT 6.1,2.8,4.7,1.2,'versicolor'
     UNION ALL SELECT 6.4,2.9,4.3,1.3,'versicolor'
     UNION ALL SELECT 6.6,3.0,4.4,1.4,'versicolor'
     UNION ALL SELECT 6.8,2.8,4.8,1.4,'versicolor'
     UNION ALL SELECT 6.7,3.0,5.0,1.7,'versicolor'
     UNION ALL SELECT 6.0,2.9,4.5,1.5,'versicolor'
     UNION ALL SELECT 5.7,2.6,3.5,1.0,'versicolor'
     UNION ALL SELECT 5.5,2.4,3.8,1.1,'versicolor'
     UNION ALL SELECT 5.5,2.4,3.7,1.0,'versicolor'
     UNION ALL SELECT 5.8,2.7,3.9,1.2,'versicolor'
     UNION ALL SELECT 6.0,2.7,5.1,1.6,'versicolor'
     UNION ALL SELECT 5.4,3.0,4.5,1.5,'versicolor'
     UNION ALL SELECT 6.0,3.4,4.5,1.6,'versicolor'
     UNION ALL SELECT 6.7,3.1,4.7,1.5,'versicolor'
     UNION ALL SELECT 6.3,2.3,4.4,1.3,'versicolor'
     UNION ALL SELECT 5.6,3.0,4.1,1.3,'versicolor'
     UNION ALL SELECT 5.5,2.5,4.0,1.3,'versicolor'
     UNION ALL SELECT 5.5,2.6,4.4,1.2,'versicolor'
     UNION ALL SELECT 6.1,3.0,4.6,1.4,'versicolor'
     UNION ALL SELECT 5.8,2.6,4.0,1.2,'versicolor'
     UNION ALL SELECT 5.0,2.3,3.3,1.0,'versicolor'
     UNION ALL SELECT 5.6,2.7,4.2,1.3,'versicolor'
     UNION ALL SELECT 5.7,3.0,4.2,1.2,'versicolor'
     UNION ALL SELECT 5.7,2.9,4.2,1.3,'versicolor'
     UNION ALL SELECT 6.2,2.9,4.3,1.3,'versicolor'
     UNION ALL SELECT 5.1,2.5,3.0,1.1,'versicolor'
     UNION ALL SELECT 5.7,2.8,4.1,1.3,'versicolor'
     UNION ALL SELECT 6.3,3.3,6.0,2.5,'virginica'
     UNION ALL SELECT 5.8,2.7,5.1,1.9,'virginica'
     UNION ALL SELECT 7.1,3.0,5.9,2.1,'virginica'
     UNION ALL SELECT 6.3,2.9,5.6,1.8,'virginica'
     UNION ALL SELECT 6.5,3.0,5.8,2.2,'virginica'
     UNION ALL SELECT 7.6,3.0,6.6,2.1,'virginica'
     UNION ALL SELECT 4.9,2.5,4.5,1.7,'virginica'
     UNION ALL SELECT 7.3,2.9,6.3,1.8,'virginica'
     UNION ALL SELECT 6.7,2.5,5.8,1.8,'virginica'
     UNION ALL SELECT 7.2,3.6,6.1,2.5,'virginica'
     UNION ALL SELECT 6.5,3.2,5.1,2.0,'virginica'
     UNION ALL SELECT 6.4,2.7,5.3,1.9,'virginica'
     UNION ALL SELECT 6.8,3.0,5.5,2.1,'virginica'
     UNION ALL SELECT 5.7,2.5,5.0,2.0,'virginica'
     UNION ALL SELECT 5.8,2.8,5.1,2.4,'virginica'
     UNION ALL SELECT 6.4,3.2,5.3,2.3,'virginica'
     UNION ALL SELECT 6.5,3.0,5.5,1.8,'virginica'
     UNION ALL SELECT 7.7,3.8,6.7,2.2,'virginica'
     UNION ALL SELECT 7.7,2.6,6.9,2.3,'virginica'
     UNION ALL SELECT 6.0,2.2,5.0,1.5,'virginica'
     UNION ALL SELECT 6.9,3.2,5.7,2.3,'virginica'
     UNION ALL SELECT 5.6,2.8,4.9,2.0,'virginica'
     UNION ALL SELECT 7.7,2.8,6.7,2.0,'virginica'
     UNION ALL SELECT 6.3,2.7,4.9,1.8,'virginica'
     UNION ALL SELECT 6.7,3.3,5.7,2.1,'virginica'
     UNION ALL SELECT 7.2,3.2,6.0,1.8,'virginica'
     UNION ALL SELECT 6.2,2.8,4.8,1.8,'virginica'
     UNION ALL SELECT 6.1,3.0,4.9,1.8,'virginica'
     UNION ALL SELECT 6.4,2.8,5.6,2.1,'virginica'
     UNION ALL SELECT 7.2,3.0,5.8,1.6,'virginica'
     UNION ALL SELECT 7.4,2.8,6.1,1.9,'virginica'
     UNION ALL SELECT 7.9,3.8,6.4,2.0,'virginica'
     UNION ALL SELECT 6.4,2.8,5.6,2.2,'virginica'
     UNION ALL SELECT 6.3,2.8,5.1,1.5,'virginica'
     UNION ALL SELECT 6.1,2.6,5.6,1.4,'virginica'
     UNION ALL SELECT 7.7,3.0,6.1,2.3,'virginica'
     UNION ALL SELECT 6.3,3.4,5.6,2.4,'virginica'
     UNION ALL SELECT 6.4,3.1,5.5,1.8,'virginica'
     UNION ALL SELECT 6.0,3.0,4.8,1.8,'virginica'
     UNION ALL SELECT 6.9,3.1,5.4,2.1,'virginica'
     UNION ALL SELECT 6.7,3.1,5.6,2.4,'virginica'
     UNION ALL SELECT 6.9,3.1,5.1,2.3,'virginica'
     UNION ALL SELECT 5.8,2.7,5.1,1.9,'virginica'
     UNION ALL SELECT 6.8,3.2,5.9,2.3,'virginica'
     UNION ALL SELECT 6.7,3.3,5.7,2.5,'virginica'
     UNION ALL SELECT 6.7,3.0,5.2,2.3,'virginica'
     UNION ALL SELECT 6.3,2.5,5.0,1.9,'virginica'
     UNION ALL SELECT 6.5,3.0,5.2,2.0,'virginica'
     UNION ALL SELECT 6.2,3.4,5.4,2.3,'virginica'
     UNION ALL SELECT 5.9,3.0,5.1,1.8,'virginica';


  CALL bqutil.procedure.t_test('iris', 'sepal_width', 'iris', 'petal_width', result);

  -- We round to 11 decimals here because there appears to be some inconsistency in the function, likely due to floating point errors and the order of aggregation
  ASSERT ROUND(result.t_value, 11) = 25.88834390257;
  ASSERT result.dof = 298;
  ASSERT result.p_value = 1.0;
END;

