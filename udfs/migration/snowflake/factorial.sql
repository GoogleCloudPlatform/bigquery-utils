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

-- factorial:
-- Computes the factorial of its input. The input argument must be an integer expression in the range of 0 to 27. 
-- Due to data type differences, the maximum input value in BigQuery is smaller than in Snowflake. 
-- Input:
-- integer_expr: INT64
-- Output: NUMERIC
CREATE OR REPLACE FUNCTION sf.factorial(integer_expr INT64) AS (
  (
    SELECT 
      ARRAY<NUMERIC>[ 
        1.0,
        1.0,
        2.0,
        6.0,
        24.0,
        120.0,
        720.0,
        5040.0,
        40320.0,
        362880.0,
        3628800.0,
        39916800.0,
        479001600.0,
        6227020800.0,
        87178291200.0,
        1307674368000.0,
        20922789888000.0,
        355687428096000.0,
        6402373705728000.0,
        121645100408832000.0,
        2432902008176640000.0,
        51090942171709440000.0,
        1124000727777607680000.0,
        25852016738884976640000.0,
        620448401733239439360000.0,
        15511210043330985984000000.0,
        403291461126605635584000000.0,
        10888869450418352160768000000.0][OFFSET(integer_expr)] 
  )
);
