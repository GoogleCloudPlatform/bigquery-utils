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

-- factorial:
-- Computes the factorial of its input. The input argument must be an integer expression in the range of 0 to 27. 
-- Due to data type differences, the maximum input value in BigQuery is smaller than in Snowflake. 
-- Input:
-- integer_expr: INT64
-- Output: NUMERIC
CREATE OR REPLACE FUNCTION sf.factorial(integer_expr INT64) 
AS ((
  SELECT ARRAY<NUMERIC>[ 
    1,
    1,
    2,
    6,
    24,
    120,
    720,
    5040,
    40320,
    362880,
    3628800,
    39916800,
    479001600,
    6227020800,
    87178291200,
    1307674368000,
    20922789888000,
    355687428096000,
    6402373705728000,
    121645100408832000,
    2432902008176640000,
    51090942171709440000.,
    1124000727777607680000.,
    25852016738884976640000.,
    620448401733239439360000.,
    15511210043330985984000000.,
    403291461126605635584000000.,
    10888869450418352160768000000.][OFFSET(integer_expr)] 
));