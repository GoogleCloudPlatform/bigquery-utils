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

generate_udf_test("ascii", [
    {
        input_1: "CAST('a' AS STRING)",
        expected_output: "CAST(97 AS INT64)"
    },
    {
        input_1: "CAST('A' AS STRING)",
        expected_output: "CAST(65 AS INT64)"
    },
    {
        input_1: "CAST('0' AS STRING)",
        expected_output: "CAST(48 AS INT64)"
    },
    {
        input_1: "CAST('}' AS STRING)",
        expected_output: "CAST(125 AS INT64)"
    },
    {
        input_1: "CAST('test' AS STRING)",
        expected_output: "CAST(116 AS INT64)"
    },
    {
        input_1: "(NULL)",
        expected_output: "(NULL)"
    }
]);

generate_udf_test("chr", [
    {
        input_1: "CAST(99 AS INT64)",
        expected_output: "CAST('c' AS STRING)"
    },
    {
        input_1: "CAST(120 AS INT64)",
        expected_output: "CAST('x' AS STRING)"
    },
    {
        input_1: "CAST(68 AS INT64)",
        expected_output: "CAST('D' AS STRING)"
    },
    {
        input_1: "CAST(55 AS INT64)",
        expected_output: "CAST('7' AS STRING)"
    },
    {
        input_1: "CAST(40 AS INT64)",
        expected_output: "CAST('(' AS STRING)"
    },
    {
        input_1: "(NULL)",
        expected_output: "(NULL)"
    }
]);

generate_udf_test("last_day", [
    {
        input_1: "DATE('2019-10-18')",
        expected_output: "DATE('2019-10-31')"
    },
    {
        input_1: "DATE('2019-11-18')",
        expected_output: "DATE('2019-11-30')"
    },
    {
        input_1: "DATE('2019-12-18')",
        expected_output: "DATE('2019-12-31')"
    },
    {
        input_1: "DATE('2019-01-18')",
        expected_output: "DATE('2019-01-31')"
    },
    {
        input_1: "DATE('2019-02-18')",
        expected_output: "DATE('2019-02-28')"
    },
    {
        input_1: "DATE('2019-03-18')",
        expected_output: "DATE('2019-03-31')"
    },
    {
        input_1: "DATE('2019-04-18')",
        expected_output: "DATE('2019-04-30')"
    }
]);

generate_udf_test("nullifzero", [
    {
        input_1: "CAST(-1 AS INT64)",
        expected_output: "CAST(-1 AS INT64)"
    },
    {
        input_1: "CAST(1 AS INT64)",
        expected_output: "CAST(1 AS INT64)"
    },
    {
        input_1: "CAST(0 AS INT64)",
        expected_output: "(NULL)"
    }
]);
generate_udf_test("nullifzero", [
    {
        input_1: "CAST(0.0 AS FLOAT64)",
        expected_output: "(NULL)"
    },
    {
        input_1: "CAST(1.1 AS FLOAT64)",
        expected_output: "CAST(1.1 AS FLOAT64)"
    },
    {
        input_1: "(NULL)",
        expected_output: "(NULL)"
    }
]);
generate_udf_test("nullifzero", [
    {
        input_1: "CAST(0 AS STRING)",
        expected_output: "(NULL)"
    },
]);

generate_udf_test("nvl", [
    {
        input_1: "CAST(-1 AS INT64)",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(-1 AS INT64)"
    },
    {
        input_1: "CAST(0 AS INT64)",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(0 AS INT64)"
    },
    {
        input_1: "NULL",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(1 AS INT64)"
    },
    {
        input_1: "CAST(2 AS INT64)",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(2 AS INT64)"
    },
    {
        input_1: "CAST(3 AS INT64)",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(3 AS INT64)"
    },
    {
        input_1: "CAST(4 AS INT64)",
        input_2: "CAST(1 AS INT64)",
        expected_output: "CAST(4 AS INT64)"
    }
]);