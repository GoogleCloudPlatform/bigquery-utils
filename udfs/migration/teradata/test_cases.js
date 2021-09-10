// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 "CAST(the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const { generate_udf_test } = unit_test_utils;

generate_udf_test("nullifzero", [
    {
        input_1: `CAST(-1 AS INT64)`,
        expected_output: `CAST(-1 AS INT64)`
    },
    {
        input_1: `CAST(1 AS INT64)`,
        expected_output: `CAST(1 AS INT64)`
    },
    {
        input_1: `CAST(0 AS INT64)`,
        expected_output: `NULL`
    }
]);
generate_udf_test("nullifzero", [
    {
        input_1: `CAST(0.0 AS FLOAT64)`,
        expected_output: `NULL`
    },
    {
        input_1: `CAST(1.1 AS FLOAT64)`,
        expected_output: `CAST(1.1 AS FLOAT64)`
    },
    {
        input_1: `NULL`,
        expected_output: `NULL`
    }
]);
generate_udf_test("nullifzero", [
    {
        input_1: `CAST(0 AS STRING)`,
        expected_output: `NULL`
    },
]);
generate_udf_test("nvl", [
    {
        input_1: `CAST(-1 AS INT64)`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(-1 AS INT64)`
    },
    {
        input_1: `CAST(0 AS INT64)`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(0 AS INT64)`
    },
    {
        input_1: `NULL`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(1 AS INT64)`
    },
    {
        input_1: `CAST(2 AS INT64)`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(2 AS INT64)`
    },
    {
        input_1: `CAST(3 AS INT64)`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(3 AS INT64)`
    },
    {
        input_1: `CAST(4 AS INT64)`,
        input_2: `CAST(1 AS INT64)`,
        expected_output: `CAST(4 AS INT64)`
    }
]);